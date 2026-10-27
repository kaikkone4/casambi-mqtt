#!/usr/bin/env python3
"""
Bounded, root-owned recovery supervisor for the Casambi MQTT bridge (#27).

Install this file root-owned (see docs/pi-recovery.md) and run it only from
its own systemd service. It is standalone (standard library only) so it never
imports code the unprivileged bridge user can modify.

What it does, and nothing else:

* reads the bridge's local health record as UNTRUSTED input;
* counts confirmed, repeated BLE startup failures of distinct bridge
  processes in a root-owned state file that survives bridge restarts;
* when they repeat and the budget allows: stop the bridge, wait until its
  process is gone, restart ``bluetooth.service``, start the bridge, and
  verify that a NEW bridge process reaches ``ready``;
* after the budget is spent, or when any step fails: latch
  ``recovery_failed``, log one critical line, and do nothing further until a
  verified healthy period or an operator ``reset``.

It never touches any unit other than the configured bridge unit and
``bluetooth.service``, never runs a shell, never reboots, and never reacts to
MQTT, configuration or Home Assistant consumer problems.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

BLUETOOTH_UNIT = "bluetooth.service"
DEFAULT_BRIDGE_UNIT = "casambi-mqtt.service"
DEFAULT_HEALTH_RECORD = "/run/casambi-mqtt/health.json"
DEFAULT_STATE_DIR = "/var/lib/casambi-mqtt-recovery"
SYSTEMCTL_CANDIDATES = ("/usr/bin/systemctl", "/bin/systemctl")
UNIT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9@._-]{0,200}\.service")
INSTANCE_ID = re.compile(r"[0-9a-f]{32}")
BOOT_ID = re.compile(r"[0-9a-f-]{36}")
STATE_VERSION = 1
RECORD_VERSION = 1
MAX_RECORD_BYTES = 4096
MAX_SEEN_INSTANCES = 32

# Startup failures that a Bluetooth stack reset can plausibly fix.
BLE_STARTUP_ERRORS = frozenset(
    {"ble_discovery_in_progress", "ble_discovery_failed", "ble_connect_failed"}
)
PHASES = frozenset(
    {"starting", "discovering", "connecting", "ready", "degraded", "offline"}
)
ERRORS = frozenset(
    {
        "none",
        "ble_disconnected",
        "ble_discovery_in_progress",
        "ble_discovery_failed",
        "ble_connect_failed",
        "network_not_found",
        "configuration",
        "mqtt_error",
        "snapshot_publish_failed",
        "state_publish_failed",
        "connection_lost",
        "shutdown",
        "unknown",
    }
)
LINKS = frozenset({"connected", "disconnected", "unknown"})

# Every validated setting maps to its default, minimum and maximum.
SETTING_RANGES: dict[str, tuple[int, int, int]] = {
    "RECOVERY_FAILURES_TO_RESET": (2, 2, 10),
    "RECOVERY_FAILURE_WINDOW_SECONDS": (600, 60, 86400),
    "RECOVERY_MAX_RESETS_SHORT": (1, 1, 5),
    "RECOVERY_SHORT_WINDOW_SECONDS": (600, 300, 86400),
    "RECOVERY_MAX_RESETS_LONG": (2, 1, 10),
    "RECOVERY_LONG_WINDOW_SECONDS": (1800, 600, 604800),
    "RECOVERY_STOP_TIMEOUT_SECONDS": (60, 5, 600),
    "RECOVERY_VERIFY_SECONDS": (180, 30, 1800),
    "RECOVERY_HEALTHY_RESET_SECONDS": (3600, 600, 604800),
}


def log(priority: int, message: str) -> None:
    """Write one journald line with an sd-daemon priority prefix."""
    sys.stdout.write(f"<{priority}>{message}\n")
    sys.stdout.flush()


class SettingsError(ValueError):
    pass


@dataclass(frozen=True)
class Settings:
    bridge_unit: str = DEFAULT_BRIDGE_UNIT
    health_record: Path = Path(DEFAULT_HEALTH_RECORD)
    state_dir: Path = Path(DEFAULT_STATE_DIR)
    failures_to_reset: int = 2
    failure_window: int = 600
    max_resets_short: int = 1
    short_window: int = 600
    max_resets_long: int = 2
    long_window: int = 1800
    stop_timeout: int = 60
    verify_seconds: int = 180
    healthy_reset_seconds: int = 3600
    poll_interval: float = 2.0

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Settings:
        bridge_unit = env.get("RECOVERY_BRIDGE_UNIT", DEFAULT_BRIDGE_UNIT)
        if UNIT_NAME.fullmatch(bridge_unit) is None or bridge_unit in {
            BLUETOOTH_UNIT,
            "dbus.service",
        }:
            message = "RECOVERY_BRIDGE_UNIT must be a plain, non-system .service name"
            raise SettingsError(message)
        paths = {}
        for key, default in (
            ("RECOVERY_HEALTH_RECORD", DEFAULT_HEALTH_RECORD),
            ("RECOVERY_STATE_DIR", DEFAULT_STATE_DIR),
        ):
            value = Path(env.get(key, default))
            if not value.is_absolute():
                message = f"{key} must be an absolute path"
                raise SettingsError(message)
            paths[key] = value
        values = {}
        for key, (default, minimum, maximum) in SETTING_RANGES.items():
            raw = env.get(key, str(default))
            try:
                value = int(raw)
            except ValueError:
                value = minimum - 1
            if not minimum <= value <= maximum:
                message = f"{key} must be an integer between {minimum} and {maximum}"
                raise SettingsError(message)
            values[key] = value
        if values["RECOVERY_MAX_RESETS_SHORT"] > values["RECOVERY_MAX_RESETS_LONG"]:
            message = "RECOVERY_MAX_RESETS_SHORT cannot exceed RECOVERY_MAX_RESETS_LONG"
            raise SettingsError(message)
        return cls(
            bridge_unit=bridge_unit,
            health_record=paths["RECOVERY_HEALTH_RECORD"],
            state_dir=paths["RECOVERY_STATE_DIR"],
            failures_to_reset=values["RECOVERY_FAILURES_TO_RESET"],
            failure_window=values["RECOVERY_FAILURE_WINDOW_SECONDS"],
            max_resets_short=values["RECOVERY_MAX_RESETS_SHORT"],
            short_window=values["RECOVERY_SHORT_WINDOW_SECONDS"],
            max_resets_long=values["RECOVERY_MAX_RESETS_LONG"],
            long_window=values["RECOVERY_LONG_WINDOW_SECONDS"],
            stop_timeout=values["RECOVERY_STOP_TIMEOUT_SECONDS"],
            verify_seconds=values["RECOVERY_VERIFY_SECONDS"],
            healthy_reset_seconds=values["RECOVERY_HEALTHY_RESET_SECONDS"],
        )


# ---------------------------------------------------------------------------
# Untrusted health record
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RecordView:
    instance_id: str
    pid: int
    phase: str
    error: str
    ever_ready: bool
    ready_since: float | None
    updated: float
    alive: bool


def _is_int(value: object, minimum: int = 0) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def _is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and value >= 0


def read_text(path: str) -> str | None:
    try:
        return Path(path).read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError):
        return None


def process_start_ticks(pid: int) -> int | None:
    stat = read_text(f"/proc/{pid}/stat")
    if stat is None or ")" not in stat:
        return None
    fields_after_comm = stat.rsplit(")", 1)[1].split()
    try:
        return int(fields_after_comm[19])
    except (IndexError, ValueError):
        return None


def parse_record(
    raw: bytes,
    *,
    boot_id: str | None,
    start_ticks_of: Callable[[int], int | None],
) -> RecordView | None:
    """
    Validate the bridge's record field by field; anything unexpected is None.

    A record from another boot is ignored entirely. A record whose PID is gone
    or now belongs to a different process (start time mismatch) describes a
    process that has exited: it is ``alive=False`` and can never be healthy.
    """
    if len(raw) > MAX_RECORD_BYTES:
        return None
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(data, dict) or data.get("record_version") != RECORD_VERSION:
        return None
    instance_id = data.get("instance_id")
    pid = data.get("pid")
    start_ticks = data.get("start_ticks")
    record_boot = data.get("boot_id")
    ready_since = data.get("ready_since_monotonic")
    if (
        not isinstance(instance_id, str)
        or INSTANCE_ID.fullmatch(instance_id) is None
        or not _is_int(pid, 1)
        or not _is_int(start_ticks)
        or not isinstance(record_boot, str)
        or BOOT_ID.fullmatch(record_boot) is None
        or data.get("phase") not in PHASES
        or data.get("error") not in ERRORS
        or data.get("ble") not in LINKS
        or data.get("mqtt") not in LINKS
        or not isinstance(data.get("ever_ready"), bool)
        or not (ready_since is None or _is_number(ready_since))
        or not _is_number(data.get("updated_monotonic"))
    ):
        return None
    if boot_id is None or record_boot != boot_id:
        return None
    return RecordView(
        instance_id=instance_id,
        pid=pid,
        phase=data["phase"],
        error=data["error"],
        ever_ready=data["ever_ready"],
        ready_since=ready_since,
        updated=data["updated_monotonic"],
        alive=start_ticks_of(pid) == start_ticks,
    )


def load_record(settings: Settings) -> RecordView | None:
    try:
        with settings.health_record.open("rb") as handle:
            raw = handle.read(MAX_RECORD_BYTES + 1)
    except OSError:
        return None
    return parse_record(
        raw,
        boot_id=read_text("/proc/sys/kernel/random/boot_id"),
        start_ticks_of=process_start_ticks,
    )


# ---------------------------------------------------------------------------
# Root-owned state
# ---------------------------------------------------------------------------


@dataclass
class State:
    ble_failures: list[float] = field(default_factory=list)
    seen_instances: list[str] = field(default_factory=list)
    resets: list[float] = field(default_factory=list)
    mqtt_failures: int = 0
    recovery_failed: bool = False
    reason: str = "none"
    last_result: str = "none"

    def to_json(self) -> str:
        return json.dumps({"version": STATE_VERSION, **asdict(self)}, indent=1)


def _number_list(value: object) -> list[float] | None:
    if not isinstance(value, list) or not all(_is_number(item) for item in value):
        return None
    return [float(item) for item in value]


def parse_state(raw: str) -> State:
    """Load state; a corrupt file fails closed as ``recovery_failed``."""
    try:
        data = json.loads(raw)
    except ValueError:
        data = None
    if not isinstance(data, dict) or data.get("version") != STATE_VERSION:
        return State(recovery_failed=True, reason="state_unreadable")
    failures = _number_list(data.get("ble_failures"))
    resets = _number_list(data.get("resets"))
    seen = data.get("seen_instances")
    if (
        failures is None
        or resets is None
        or not isinstance(seen, list)
        or not all(
            isinstance(item, str) and INSTANCE_ID.fullmatch(item) for item in seen
        )
        or not _is_int(data.get("mqtt_failures"))
        or not isinstance(data.get("recovery_failed"), bool)
        or not isinstance(data.get("reason"), str)
        or not isinstance(data.get("last_result"), str)
    ):
        return State(recovery_failed=True, reason="state_unreadable")
    return State(
        ble_failures=failures,
        seen_instances=seen[-MAX_SEEN_INSTANCES:],
        resets=resets,
        mqtt_failures=data["mqtt_failures"],
        recovery_failed=data["recovery_failed"],
        reason=data["reason"][:64],
        last_result=data["last_result"][:64],
    )


def load_state(settings: Settings) -> State:
    path = settings.state_dir / "state.json"
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return State()
    except (OSError, UnicodeDecodeError):
        return State(recovery_failed=True, reason="state_unreadable")
    return parse_state(raw)


def save_state(settings: Settings, state: State) -> None:
    settings.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = settings.state_dir / "state.json"
    temporary = path.with_name(".state.json.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(state.to_json())
        handle.flush()
        os.fsync(handle.fileno())
    temporary.chmod(0o600)
    temporary.replace(path)


@contextlib.contextmanager
def exclusive_lock(settings: Settings) -> Iterator[bool]:
    """Yield whether this run holds the single recovery lock."""
    settings.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (settings.state_dir / "lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


# ---------------------------------------------------------------------------
# Decision: pure functions over state and record
# ---------------------------------------------------------------------------

ACTION_NONE = "none"
ACTION_RECOVER = "recover"
ACTION_LATCH = "latch_failed"


@dataclass(frozen=True)
class Decision:
    action: str
    reason: str


def _recent(values: list[float], now: float, window: float) -> list[float]:
    # A timestamp "in the future" (wall clock stepped back) stays counted:
    # the budget may only ever be too strict, never too lenient.
    return [value for value in values if now - value < window]


def observe(
    state: State, record: RecordView | None, now: float, now_mono: float, s: Settings
) -> None:
    """Fold one record observation into the state."""
    state.ble_failures = _recent(state.ble_failures, now, s.failure_window)
    state.resets = _recent(state.resets, now, s.long_window)
    if record is None:
        return
    if record.alive:
        if (
            record.phase == "ready"
            and record.ready_since is not None
            and now_mono - record.ready_since >= s.healthy_reset_seconds
        ):
            # Only a long, verified healthy period clears the counters and
            # the latch -- never merely a new process starting.
            if state.ble_failures or state.recovery_failed:
                log(5, "Bridge verified healthy; recovery counters cleared")
            state.ble_failures = []
            state.recovery_failed = False
            state.reason = "none"
        return
    if record.instance_id in state.seen_instances:
        return
    state.seen_instances = [*state.seen_instances, record.instance_id][
        -MAX_SEEN_INSTANCES:
    ]
    if record.error in BLE_STARTUP_ERRORS or (
        record.error == "ble_disconnected" and not record.ever_ready
    ):
        state.ble_failures.append(now)
    elif record.error == "mqtt_error":
        state.mqtt_failures += 1


def decide(
    state: State, record: RecordView | None, now: float, s: Settings
) -> Decision:
    if state.recovery_failed:
        return Decision(ACTION_NONE, "recovery_failed")
    if len(state.ble_failures) < s.failures_to_reset:
        return Decision(ACTION_NONE, "no_repeated_ble_failure")
    if record is not None and record.alive and record.phase == "ready":
        return Decision(ACTION_NONE, "bridge_ready")
    if (
        len(_recent(state.resets, now, s.short_window)) >= s.max_resets_short
        or len(_recent(state.resets, now, s.long_window)) >= s.max_resets_long
    ):
        return Decision(ACTION_LATCH, "budget_exhausted")
    return Decision(ACTION_RECOVER, "repeated_ble_failure")


# ---------------------------------------------------------------------------
# Service manager
# ---------------------------------------------------------------------------


class ServiceManagerError(RuntimeError):
    pass


class Systemctl:
    """``systemctl`` with a fixed verb and unit allowlist; never a shell."""

    VERBS = frozenset({"stop", "start", "restart", "reset-failed"})
    SHOW = "show"

    def __init__(
        self,
        allowed_units: frozenset[str],
        *,
        run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
        executable: str | None = None,
    ) -> None:
        self.allowed_units = allowed_units
        self._run = run
        self.executable = executable or next(
            (path for path in SYSTEMCTL_CANDIDATES if Path(path).exists()),
            SYSTEMCTL_CANDIDATES[0],
        )

    def _check_unit(self, unit: str) -> None:
        if unit not in self.allowed_units:
            message = "unit is not allowlisted"
            raise ServiceManagerError(message)

    def action(self, verb: str, unit: str, timeout: float) -> None:
        if verb not in self.VERBS:
            message = "verb is not allowlisted"
            raise ServiceManagerError(message)
        self._check_unit(unit)
        try:
            result = self._run(
                [self.executable, verb, unit],
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise ServiceManagerError(verb) from error
        if result.returncode != 0:
            raise ServiceManagerError(verb)

    def show(self, unit: str) -> dict[str, str]:
        self._check_unit(unit)
        try:
            result = self._run(
                [
                    self.executable,
                    "show",
                    unit,
                    "--property=ActiveState,SubState,MainPID,Result",
                ],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise ServiceManagerError(self.SHOW) from error
        if result.returncode != 0:
            raise ServiceManagerError(self.SHOW)
        properties = {}
        for line in result.stdout.splitlines():
            key, _, value = line.partition("=")
            if key in {"ActiveState", "SubState", "MainPID", "Result"}:
                properties[key] = value.strip()
        return properties


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------


@dataclass
class Clock:
    wall: Callable[[], float] = time.time
    monotonic: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep


def _wait_until(
    predicate: Callable[[], bool], timeout: float, clock: Clock, interval: float
) -> bool:
    deadline = clock.monotonic() + timeout
    while True:
        try:
            if predicate():
                return True
        except ServiceManagerError:
            pass
        if clock.monotonic() >= deadline:
            return False
        clock.sleep(interval)


def recover(  # noqa: PLR0913
    manager: Systemctl,
    settings: Settings,
    state: State,
    previous: RecordView | None,
    clock: Clock,
    read_record: Callable[[], RecordView | None],
    persist: Callable[[State], None],
) -> bool:
    """
    Stop bridge -> restart Bluetooth -> start bridge -> verify a new ready.

    The reset is charged to the budget and persisted BEFORE the first action,
    so a crash halfway through can never hand out a free extra reset. Any
    failed step latches ``recovery_failed``; once the bridge was stopped, a
    best-effort start is always attempted so a failed recovery never leaves
    it stopped on purpose.
    """
    bridge = settings.bridge_unit
    state.resets.append(clock.wall())
    state.last_result = "in_progress"
    persist(state)
    log(4, "Repeated Casambi BLE startup failure: resetting Bluetooth once")

    def fail(reason: str) -> bool:
        state.recovery_failed = True
        state.reason = reason
        state.last_result = "recovery_failed"
        persist(state)
        log(2, f"Casambi bridge recovery failed: {reason}; no further resets")
        return False

    bridge_stopped = False
    try:
        manager.action("stop", bridge, settings.stop_timeout)
        bridge_stopped = True
        if not _wait_until(
            lambda: manager.show(bridge).get("MainPID") == "0",
            settings.stop_timeout,
            clock,
            settings.poll_interval,
        ):
            return fail("bridge_stop_timeout")
        manager.action("restart", BLUETOOTH_UNIT, settings.stop_timeout)
        if not _wait_until(
            lambda: manager.show(BLUETOOTH_UNIT).get("ActiveState") == "active",
            settings.stop_timeout,
            clock,
            settings.poll_interval,
        ):
            return fail("bluetooth_restart_timeout")
        if manager.show(bridge).get("Result") == "start-limit-hit":
            manager.action("reset-failed", bridge, settings.stop_timeout)
        manager.action("start", bridge, settings.stop_timeout)
        bridge_stopped = False
    except ServiceManagerError as error:
        return fail(f"service_manager_{error.args[0] if error.args else 'error'}")
    finally:
        if bridge_stopped:
            with contextlib.suppress(ServiceManagerError):
                manager.action("start", bridge, settings.stop_timeout)

    previous_instance = None if previous is None else previous.instance_id

    def recovered() -> bool:
        record = read_record()
        return (
            record is not None
            and record.alive
            and record.phase == "ready"
            and record.instance_id != previous_instance
        )

    if not _wait_until(
        recovered, settings.verify_seconds, clock, settings.poll_interval
    ):
        state.last_result = "verify_failed"
        if (
            len(_recent(state.resets, clock.wall(), settings.long_window))
            >= settings.max_resets_long
        ):
            return fail("verify_failed_budget_exhausted")
        persist(state)
        log(3, "Casambi bridge did not report ready after the Bluetooth reset")
        return False
    state.ble_failures = []
    state.last_result = "recovered"
    persist(state)
    log(5, "Casambi bridge recovered after a Bluetooth reset")
    return True


def run_once(
    settings: Settings,
    manager: Systemctl,
    clock: Clock | None = None,
    read_record: Callable[[], RecordView | None] | None = None,
) -> str:
    """One supervisor pass under the exclusive lock; return what happened."""
    clock = clock or Clock()
    reader = read_record or (lambda: load_record(settings))
    with exclusive_lock(settings) as acquired:
        if not acquired:
            log(6, "Another recovery run holds the lock; nothing to do")
            return "locked"
        state = load_state(settings)
        record = reader()
        observe(state, record, clock.wall(), clock.monotonic(), settings)
        decision = decide(state, record, clock.wall(), settings)
        if decision.action == ACTION_LATCH:
            state.recovery_failed = True
            state.reason = decision.reason
            state.last_result = "recovery_failed"
            save_state(settings, state)
            log(2, f"Casambi bridge recovery failed: {decision.reason}; no reset")
            return decision.reason
        save_state(settings, state)
        if decision.action != ACTION_RECOVER:
            return decision.reason
        recover(
            manager,
            settings,
            state,
            record,
            clock,
            reader,
            lambda current: save_state(settings, current),
        )
        return state.last_result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("command", choices=("run", "status", "reset"), nargs="?")
    args = parser.parse_args(argv)
    try:
        settings = Settings.from_env(os.environ)
    except SettingsError as error:
        log(3, f"Invalid recovery settings: {error}")
        return 78
    command = args.command or "run"
    if command == "status":
        sys.stdout.write(load_state(settings).to_json() + "\n")
        return 0
    if command == "reset":
        with exclusive_lock(settings) as acquired:
            if not acquired:
                log(3, "A recovery run is active; try again")
                return 75
            save_state(settings, State(last_result="operator_reset"))
        log(5, "Recovery state reset by operator")
        return 0
    manager = Systemctl(frozenset({settings.bridge_unit, BLUETOOTH_UNIT}))
    run_once(settings, manager)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
