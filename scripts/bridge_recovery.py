#!/usr/bin/env python3
"""
Bounded, root-owned recovery supervisor for the Casambi MQTT bridge (#27).

Install this file root-owned (see docs/pi-recovery.md) and run it only from
its own systemd service. It is standalone (standard library only) so it never
imports code the unprivileged bridge user can modify.

What it does, and nothing else:

* counts every bridge exit with the BLE startup exit status (76). systemd
  reports each exit to a root ``ExecStopPost=+`` hook (``record-exit``) in
  ``$EXIT_CODE``/``$EXIT_STATUS``; the hook appends it to a small root-owned
  exit log, and each supervisor run consumes that log into root-owned state
  that survives bridge restarts;
* reads the bridge's local health record only as UNTRUSTED, advisory input,
  and only when it belongs to the unit's current main process;
* when BLE failures repeat and the budget allows: stop the bridge, wait until
  it is fully stopped, restart ``bluetooth.service``, start the bridge, and
  verify that a NEW bridge process reaches ``ready``;
* when the bridge gave up for any other non-configuration reason: a
  separately budgeted plain restart of the bridge only;
* after the budget is spent, or when any step fails: latch
  ``recovery_failed``, log one critical line, and do nothing further until a
  long healthy period it observed itself, or an operator ``reset``.

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
import stat
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

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
EXIT_BLE_STARTUP_FAILED = 76
EXIT_CONFIGURATION = 78
# A live bridge whose record has not changed for this long while it is still
# discovering/connecting is stuck (it should have exited within ~3 minutes).
STUCK_SECONDS = 600
PLAIN_RESTART_WINDOW_SECONDS = 1800
MAX_PLAIN_RESTARTS_PER_WINDOW = 1
UNIT_PROPERTIES = ("ActiveState", "SubState", "MainPID", "Result", "ExecMainStatus")
EXIT_CODES = frozenset({"exited", "killed", "dumped"})
EXIT_STATUS = re.compile(r"[A-Z0-9+]{1,16}")
MAX_EXIT_LOG = 64

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
    mqtt_reconnects: int = 0


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
    process that has exited: it is ``alive=False``.
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
    mqtt_reconnects = data.get("mqtt_reconnects", 0)
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
        or not _is_int(mqtt_reconnects)
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
        mqtt_reconnects=mqtt_reconnects,
    )


def read_record_bytes(path: Path) -> bytes | None:
    """
    Read the record without following links or blocking.

    The record's directory belongs to the unprivileged bridge user, so the
    file may be swapped for a symlink (O_NOFOLLOW refuses it) or a FIFO
    (O_NONBLOCK keeps open() from hanging; fstat then rejects it).
    """
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return None
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            return None
        return os.read(descriptor, MAX_RECORD_BYTES + 1)
    except OSError:
        return None
    finally:
        os.close(descriptor)


def load_record(settings: Settings) -> RecordView | None:
    raw = read_record_bytes(settings.health_record)
    if raw is None:
        return None
    return parse_record(
        raw,
        boot_id=read_text("/proc/sys/kernel/random/boot_id"),
        start_ticks_of=process_start_ticks,
    )


# ---------------------------------------------------------------------------
# Trusted unit state from systemd
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class UnitView:
    active_state: str
    main_pid: int
    result: str
    exec_main_status: int | None

    @classmethod
    def from_properties(cls, properties: Mapping[str, str]) -> UnitView:
        def integer(key: str) -> int | None:
            try:
                return int(properties.get(key, ""))
            except ValueError:
                return None

        return cls(
            active_state=properties.get("ActiveState", "unknown"),
            main_pid=integer("MainPID") or 0,
            result=properties.get("Result", "unknown"),
            exec_main_status=integer("ExecMainStatus"),
        )


# ---------------------------------------------------------------------------
# Clock: budgets use boot-scoped CLOCK_BOOTTIME, never the wall clock alone
# ---------------------------------------------------------------------------


def _boottime() -> float:
    return time.clock_gettime(time.CLOCK_BOOTTIME)


@dataclass
class Clock:
    wall: Callable[[], float] = time.time
    monotonic: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    boottime: Callable[[], float] = _boottime
    boot_id: Callable[[], str | None] = lambda: read_text(
        "/proc/sys/kernel/random/boot_id"
    )

    def stamp(self) -> dict[str, Any]:
        return {"boot": self.boot_id(), "t": self.boottime(), "wall": self.wall()}


def _is_stamp(value: object) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == {"boot", "t", "wall"}
        and (value["boot"] is None or isinstance(value["boot"], str))
        and _is_number(value["t"])
        and _is_number(value["wall"])
    )


def is_recent(stamp: Mapping[str, Any], clock: Clock, window: float) -> bool:
    """
    Whether ``stamp`` lies within ``window`` seconds of now.

    Within one boot this uses CLOCK_BOOTTIME, which no NTP step can move. For
    a stamp from an earlier boot only the wall clock is left; it is used
    conservatively -- a stamp that looks to be in the future still counts.
    """
    boot = clock.boot_id()
    if boot is not None and stamp["boot"] == boot:
        return clock.boottime() - stamp["t"] < window
    return clock.wall() - stamp["wall"] < window


# ---------------------------------------------------------------------------
# Root-owned state
# ---------------------------------------------------------------------------


@dataclass
class State:
    ble_failures: list[dict[str, Any]] = field(default_factory=list)
    resets: list[dict[str, Any]] = field(default_factory=list)
    plain_restarts: list[dict[str, Any]] = field(default_factory=list)
    ready_instance: str = ""
    ready_since: dict[str, Any] | None = None
    stuck_counted: list[str] = field(default_factory=list)
    mqtt_reconnects: int = 0
    recovery_failed: bool = False
    reason: str = "none"
    last_result: str = "none"

    def to_json(self) -> str:
        return json.dumps({"version": STATE_VERSION, **asdict(self)}, indent=1)


def parse_state(raw: str) -> State:
    """Load state; a corrupt file fails closed as ``recovery_failed``."""
    unreadable = State(recovery_failed=True, reason="state_unreadable")
    try:
        data = json.loads(raw)
    except ValueError:
        return unreadable
    if not isinstance(data, dict) or data.get("version") != STATE_VERSION:
        return unreadable
    stamps = {}
    for key in ("ble_failures", "resets", "plain_restarts"):
        value = data.get(key)
        if not isinstance(value, list) or not all(_is_stamp(item) for item in value):
            return unreadable
        stamps[key] = value
    ready_since = data.get("ready_since")
    stuck = data.get("stuck_counted")
    if (
        not isinstance(data.get("ready_instance"), str)
        or not (ready_since is None or _is_stamp(ready_since))
        or not isinstance(stuck, list)
        or not all(
            isinstance(item, str) and INSTANCE_ID.fullmatch(item) for item in stuck
        )
        or not _is_int(data.get("mqtt_reconnects"))
        or not isinstance(data.get("recovery_failed"), bool)
        or not isinstance(data.get("reason"), str)
        or not isinstance(data.get("last_result"), str)
    ):
        return unreadable
    return State(
        **stamps,
        ready_instance=data["ready_instance"][:32],
        ready_since=ready_since,
        stuck_counted=stuck[-MAX_SEEN_INSTANCES:],
        mqtt_reconnects=data["mqtt_reconnects"],
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
# Decision: pure functions over state, unit and record
# ---------------------------------------------------------------------------

ACTION_NONE = "none"
ACTION_RECOVER = "recover"
ACTION_RESTART = "restart_bridge"
ACTION_LATCH = "latch_failed"


@dataclass(frozen=True)
class Decision:
    action: str
    reason: str


def trusted_record(record: RecordView | None, unit: UnitView) -> RecordView | None:
    """Return the record only if it is the unit's current, live main process."""
    if record is None or not record.alive or record.pid != unit.main_pid:
        return None
    return record


def _recent(stamps: list[dict[str, Any]], clock: Clock, window: float) -> list:
    return [stamp for stamp in stamps if is_recent(stamp, clock, window)]


# ---------------------------------------------------------------------------
# Exit log, written by the root ExecStopPost=+ hook for every bridge exit
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _exit_log_lock(settings: Settings) -> Iterator[None]:
    """Hold the separate, brief exit-log lock (never the recovery lock)."""
    settings.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (settings.state_dir / "exits.lock").open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _read_exit_log(path: Path) -> list[dict[str, Any]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if not isinstance(data, list):
        return []
    return [
        item
        for item in data
        if isinstance(item, dict)
        and _is_stamp(item.get("stamp"))
        and item.get("code") in EXIT_CODES
        and isinstance(item.get("status"), str)
        and EXIT_STATUS.fullmatch(item["status"])
    ]


def _write_exit_log(path: Path, entries: list[dict[str, Any]]) -> None:
    temporary = path.with_name(".exits.json.tmp")
    temporary.write_text(json.dumps(entries[-MAX_EXIT_LOG:]), encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(path)


def record_exit(settings: Settings, env: Mapping[str, str], clock: Clock) -> bool:
    """
    Append one bridge exit reported by systemd; return whether it was valid.

    Runs as ``ExecStopPost=+`` of the bridge unit, i.e. as root, for every
    stop including each automatic restart. ``EXIT_CODE``/``EXIT_STATUS`` are
    set by systemd from the real process exit, not by the bridge. Kept
    deliberately small and quick: it only appends under its own short lock.
    """
    code = env.get("EXIT_CODE", "")
    status = env.get("EXIT_STATUS", "")
    if code not in EXIT_CODES or EXIT_STATUS.fullmatch(status) is None:
        return False
    path = settings.state_dir / "exits.json"
    with _exit_log_lock(settings):
        entries = _read_exit_log(path)
        entries.append({"stamp": clock.stamp(), "code": code, "status": status})
        _write_exit_log(path, entries)
    return True


def consume_exits(settings: Settings) -> list[dict[str, Any]]:
    path = settings.state_dir / "exits.json"
    with _exit_log_lock(settings):
        entries = _read_exit_log(path)
        if entries:
            _write_exit_log(path, [])
    return entries


def observe(  # noqa: PLR0913
    state: State,
    unit: UnitView,
    record: RecordView | None,
    clock: Clock,
    s: Settings,
    exits: list[dict[str, Any]] | None = None,
) -> None:
    """Fold one observation of systemd, its exit log and the record."""
    for item in exits or []:
        if item["code"] == "exited" and item["status"] == str(EXIT_BLE_STARTUP_FAILED):
            state.ble_failures.append(item["stamp"])
    state.ble_failures = _recent(state.ble_failures, clock, s.failure_window)
    state.resets = _recent(state.resets, clock, s.long_window)
    state.plain_restarts = _recent(
        state.plain_restarts, clock, PLAIN_RESTART_WINDOW_SECONDS
    )

    record = trusted_record(record, unit)
    if record is None:
        state.ready_instance = ""
        state.ready_since = None
        return
    state.mqtt_reconnects = record.mqtt_reconnects
    if record.phase == "ready":
        if state.ready_instance != record.instance_id or state.ready_since is None:
            # The healthy period starts when THIS supervisor first sees it,
            # never at a time the (untrusted) record claims.
            state.ready_instance = record.instance_id
            state.ready_since = clock.stamp()
        elif not is_recent(state.ready_since, clock, s.healthy_reset_seconds):
            if state.ble_failures or state.recovery_failed:
                log(5, "Bridge observed healthy for a long period; counters cleared")
            state.ble_failures = []
            state.recovery_failed = False
            state.reason = "none"
        return
    state.ready_instance = ""
    state.ready_since = None
    if (
        record.phase in {"discovering", "connecting"}
        and clock.monotonic() - record.updated > STUCK_SECONDS
        and record.instance_id not in state.stuck_counted
    ):
        state.stuck_counted = [*state.stuck_counted, record.instance_id][
            -MAX_SEEN_INSTANCES:
        ]
        # Stuck for 10 minutes means many missed retry cycles: that alone is
        # a confirmed, repeated BLE fault, so it reaches the threshold.
        state.ble_failures.extend(clock.stamp() for _ in range(s.failures_to_reset))


def decide(  # noqa: PLR0911
    state: State,
    unit: UnitView,
    record: RecordView | None,
    clock: Clock,
    s: Settings,
) -> Decision:
    if state.recovery_failed:
        return Decision(ACTION_NONE, "recovery_failed")
    trusted = trusted_record(record, unit)
    if trusted is not None and trusted.phase == "ready":
        return Decision(ACTION_NONE, "bridge_ready")
    if len(state.ble_failures) >= s.failures_to_reset:
        if (
            len(_recent(state.resets, clock, s.short_window)) >= s.max_resets_short
            or len(_recent(state.resets, clock, s.long_window)) >= s.max_resets_long
        ):
            return Decision(ACTION_LATCH, "budget_exhausted")
        return Decision(ACTION_RECOVER, "repeated_ble_failure")
    if unit.active_state == "failed" and unit.exec_main_status != EXIT_CONFIGURATION:
        if len(state.plain_restarts) >= MAX_PLAIN_RESTARTS_PER_WINDOW:
            return Decision(ACTION_NONE, "bridge_failed_restart_budget_used")
        return Decision(ACTION_RESTART, "bridge_failed")
    return Decision(ACTION_NONE, "no_repeated_ble_failure")


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
                    f"--property={','.join(UNIT_PROPERTIES)}",
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
            if key in UNIT_PROPERTIES:
                properties[key] = value.strip()
        return properties


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------


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


def _fully_stopped(manager: Systemctl, unit: str) -> bool:
    properties = manager.show(unit)
    return properties.get("MainPID") == "0" and properties.get("ActiveState") in {
        "inactive",
        "failed",
    }


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
    state.resets.append(clock.stamp())
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
            lambda: _fully_stopped(manager, bridge),
            settings.stop_timeout,
            clock,
            settings.poll_interval,
        ):
            return fail("bridge_stop_timeout")
        # The exit caused by this stop was requested here, not a failure:
        # drop whatever the exit hook just recorded for it.
        consume_exits(settings)
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
        unit = UnitView.from_properties(manager.show(bridge))
        record = trusted_record(read_record(), unit)
        return (
            record is not None
            and record.phase == "ready"
            and record.instance_id != previous_instance
        )

    if not _wait_until(
        recovered, settings.verify_seconds, clock, settings.poll_interval
    ):
        state.last_result = "verify_failed"
        if len(_recent(state.resets, clock, settings.long_window)) >= (
            settings.max_resets_long
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


def restart_bridge(
    manager: Systemctl, settings: Settings, state: State, clock: Clock
) -> None:
    """Budgeted plain restart of a bridge systemd gave up on; no Bluetooth."""
    state.plain_restarts.append(clock.stamp())
    try:
        manager.action("reset-failed", settings.bridge_unit, settings.stop_timeout)
        manager.action("start", settings.bridge_unit, settings.stop_timeout)
    except ServiceManagerError:
        state.last_result = "bridge_restart_failed"
        log(3, "Casambi bridge restart after systemd gave up failed")
        return
    state.last_result = "bridge_restarted"
    log(5, "Casambi bridge restarted after systemd gave up (no Bluetooth reset)")


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
        try:
            unit = UnitView.from_properties(manager.show(settings.bridge_unit))
        except ServiceManagerError:
            log(3, "Unable to read the bridge unit state; nothing done")
            return "systemd_unavailable"
        state = load_state(settings)
        record = reader()
        observe(state, unit, record, clock, settings, consume_exits(settings))
        decision = decide(state, unit, record, clock, settings)
        if decision.action == ACTION_LATCH:
            state.recovery_failed = True
            state.reason = decision.reason
            state.last_result = "recovery_failed"
            save_state(settings, state)
            log(2, f"Casambi bridge recovery failed: {decision.reason}; no reset")
            return decision.reason
        if decision.action == ACTION_RESTART:
            restart_bridge(manager, settings, state, clock)
            save_state(settings, state)
            return state.last_result
        save_state(settings, state)
        if decision.action != ACTION_RECOVER:
            return decision.reason
        recover(
            manager,
            settings,
            state,
            trusted_record(record, unit),
            clock,
            reader,
            lambda current: save_state(settings, current),
        )
        return state.last_result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument(
        "command", choices=("run", "status", "reset", "record-exit"), nargs="?"
    )
    args = parser.parse_args(argv)
    try:
        settings = Settings.from_env(os.environ)
    except SettingsError as error:
        log(3, f"Invalid recovery settings: {error}")
        return 78
    command = args.command or "run"
    if command == "record-exit":
        # Never fail the bridge's stop: a bad value is just not recorded.
        with contextlib.suppress(OSError):
            record_exit(settings, os.environ, Clock())
        return 0
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
