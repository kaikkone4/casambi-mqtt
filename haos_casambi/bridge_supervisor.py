"""
HAOS-only child controller; HTTP liveness is NOT bridge/RF health.

The state ledger is charged and fsynced before spawning. Nothing, including a
brief ready phase, renews its budget except an explicit greater reset generation.
This module never imports Home Assistant or changes the shared bridge runtime.
"""

# Standalone executable: delayed imports keep disabled mode inert.
# ruff: noqa: INP001, PLC0415, T201
from __future__ import annotations

import asyncio
import contextlib
import fcntl
import importlib.util
import json
import logging
import math
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import ModuleType

MAX_REPLACEMENTS = 3
MAX_STATE_BYTES = 16384
MAX_RECORD_BYTES = 4096
MAX_RESET_GENERATION = 1_000_000
INCIDENT_ID_LENGTH = 32
CONTROLLER_STALL_SECONDS = 30
EXIT_CONFIGURATION = 78
REASONS = frozenset(
    {
        "none",
        "configuration",
        "retry_exhausted",
        "child_failure",
        "mqtt_auth",
        "interrupted_spawn",
        "orphan_child",
        "controller_death",
        "controller_failure",
        "manual_reset",
        "storage_failure",
        "health_stale",
        "spawn_failed",
    }
)


def log_event(event: str, reason: str, attempts: int) -> None:
    """Emit only fixed controller vocabulary and a bounded replacement count."""
    if (
        event not in {"terminal_latched", "launch_reserved", "retry_scheduled"}
        or reason not in REASONS
        or type(attempts) is not int
        or not 0 <= attempts <= MAX_REPLACEMENTS
    ):
        raise ValueError
    print(
        f"event={event} reason={reason} attempts={attempts}",
        file=sys.stderr,
        flush=True,
    )


def namespace_id() -> str:
    """Identify the boot/PID namespace containing our owned children."""
    boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    return f"{boot}:{Path('/proc/self/ns/pid').stat().st_ino}"


def sync_directory(directory: Path) -> None:
    """Durably commit directory entries, including initial recovery creation."""
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def valid_identity(identity: object) -> bool:
    """Validate the small persisted ownership tuple."""
    return (
        isinstance(identity, dict)
        and set(identity) == {"pid", "start_ticks", "boot_id"}
        and type(identity["pid"]) is int
        and identity["pid"] > 0
        and type(identity["start_ticks"]) is int
        and identity["start_ticks"] > 0
        and isinstance(identity["boot_id"], str)
        and bool(identity["boot_id"])
    )


class RecoveryStore:
    """Exclusive, durable recovery ledger. Keep the lock for controller lifetime."""

    def __init__(self, data_dir: Path) -> None:
        self.failed = False
        self.directory = data_dir / "recovery"
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.directory.chmod(0o700)
        sync_directory(data_dir)
        self.path = self.directory / "state.json"
        self.lock = (self.directory / "controller.lock").open("a", encoding="ascii")
        self.lock_path = self.directory / "controller.lock"
        self.lock_path.chmod(0o600)
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if self.path.exists():
                if self.path.stat().st_size > MAX_STATE_BYTES:
                    raise ValueError  # noqa: TRY301 — lock release covers all load failures.
                self.state = json.loads(self.path.read_text())
                self.validate()
                self.path.chmod(0o600)
            else:
                self.state = {
                    "schema_version": 1,
                    "namespace": namespace_id(),
                    "incident_id": uuid.uuid4().hex,
                    "attempts": 0,
                    "launched": False,
                    "active": None,
                    "inflight": False,
                    "controller_active": False,
                    "latched": False,
                    "reason": "none",
                    "reset_generation": 0,
                    "notification": {"pending": None, "acked": None},
                }
                self.save()
        except BaseException:
            self.lock.close()
            raise

    def __enter__(self) -> Self:
        """Hold the advisory lock across all controller activity."""
        return self

    def __exit__(self, *_: object) -> None:
        """Release the lock without modifying the durable budget."""
        self.lock.close()

    def validate(self) -> None:
        """Reject incomplete/unknown ledgers instead of silently renewing budgets."""
        state = self.state
        required = {
            "schema_version",
            "namespace",
            "incident_id",
            "attempts",
            "launched",
            "active",
            "inflight",
            "controller_active",
            "latched",
            "reason",
            "reset_generation",
            "notification",
        }
        if not isinstance(state, dict) or set(state) != required:
            raise ValueError
        if (
            type(state["schema_version"]) is not int
            or state["schema_version"] != 1
            or type(state["attempts"]) is not int
            or not 0 <= state["attempts"] <= MAX_REPLACEMENTS
            or type(state["reset_generation"]) is not int
            or not 0 <= state["reset_generation"] <= MAX_RESET_GENERATION
            or any(
                type(state[key]) is not bool
                for key in ("launched", "inflight", "controller_active", "latched")
            )
            or not isinstance(state["incident_id"], str)
            or len(state["incident_id"]) != INCIDENT_ID_LENGTH
            or any(c not in "0123456789abcdef" for c in state["incident_id"])
            or state["reason"] not in REASONS
            or not isinstance(state["namespace"], str)
            or not state["namespace"]
        ):
            raise ValueError
        active = state["active"]
        if active is not None and not valid_identity(active):
            raise ValueError
        notification = state["notification"]
        if not isinstance(notification, dict) or set(notification) != {
            "pending",
            "acked",
        }:
            raise ValueError
        for payload in notification.values():
            if payload is not None and (
                not isinstance(payload, dict)
                or set(payload)
                != {"protocol_version", "incident_id", "state", "reason", "attempts"}
                or payload["protocol_version"] != 1
                or payload["incident_id"] != state["incident_id"]
                or payload["state"] not in {"failed", "recovering", "armed"}
                or payload["reason"] not in REASONS
                or type(payload["attempts"]) is not int
                or not 0 <= payload["attempts"] <= MAX_REPLACEMENTS
            ):
                raise ValueError

    def save(self) -> None:
        """Never authorize another spawn after any ambiguous storage failure."""
        try:
            self._save()
        except OSError:
            self.failed = True
            raise

    def _save(self) -> None:
        """Commit atomically, including directory metadata; failures forbid spawn."""
        temporary = self.directory / "state.tmp"
        with open(
            temporary, "w", encoding="ascii", opener=self._private_open
        ) as output:
            json.dump(self.state, output, separators=(",", ":"))
            output.flush()
            os.fsync(output.fileno())
        temporary.chmod(0o600)
        temporary.replace(self.path)
        sync_directory(self.directory)

    @staticmethod
    def _private_open(path: str, flags: int) -> int:
        return os.open(path, flags, 0o600)

    def notify(self, state: str, reason: str) -> None:
        """Persist an outbox entry together with its corresponding state change."""
        pending = self.state["notification"]["pending"]
        # The reset dismissal must reach HA even if retries occur offline.
        # Keep its original attempts=0 payload until PUBACK; only a terminal
        # failure may supersede it. The ledger budget advances independently.
        if (
            state == "recovering"
            and pending is not None
            and pending["state"] == "armed"
        ):
            return
        self.state["notification"]["pending"] = {
            "protocol_version": 1,
            "incident_id": self.state["incident_id"],
            "state": state,
            "reason": reason,
            "attempts": self.state["attempts"],
        }

    def latch(self, reason: str) -> None:
        """Terminal until an operator explicitly increases reset generation."""
        self.state.update(latched=True, reason=reason)
        self.notify("failed", reason)
        self.save()
        log_event("terminal_latched", reason, self.state["attempts"])

    def charge(self) -> bool:
        """Durably reserve original launch or one of three replacements."""
        if self.failed or self.state["latched"]:
            return False
        if self.state["inflight"] or self.state["active"] is not None:
            self.latch(
                "interrupted_spawn" if self.state["inflight"] else "orphan_child"
            )
            return False
        if self.state["launched"]:
            if self.state["attempts"] >= MAX_REPLACEMENTS:
                self.latch("retry_exhausted")
                return False
            self.state["attempts"] += 1
            self.notify("recovering", "child_failure")
        self.state.update(launched=True, inflight=True, controller_active=True)
        self.save()
        log_event("launch_reserved", self.state["reason"], self.state["attempts"])
        return True

    def rearm(self, generation: int) -> bool:
        """Acknowledge a greater operator generation exactly once."""
        if type(generation) is not int or not 0 <= generation <= MAX_RESET_GENERATION:
            raise ValueError
        if self.failed or generation <= self.state["reset_generation"]:
            return False
        # A reset cannot authorize overlap with an unresolved prior child.
        if self.state["active"] is not None or self.state["inflight"]:
            return False
        self.state.update(
            incident_id=uuid.uuid4().hex,
            attempts=0,
            launched=False,
            latched=False,
            reason="none",
            reset_generation=generation,
            controller_active=False,
            notification={"pending": None, "acked": None},
        )
        self.notify("armed", "manual_reset")
        self.save()
        return True

    def reconcile(self) -> None:
        """Never trust or signal a child inherited from another controller."""
        current_namespace = namespace_id()
        if self.state["namespace"] != current_namespace:
            # HAOS native container replacement destroys the previous container's
            # process namespace. Do not trust reused PID/start ticks in a new one.
            # Keep any latch and the charged budget; only ownership is obsolete.
            was_inflight = self.state["inflight"]
            self.state.update(namespace=current_namespace, active=None, inflight=False)
            if was_inflight:
                self.latch("interrupted_spawn")
            else:
                self.save()
        elif self.state["inflight"]:
            # Popen could have forked before the wrapper died. Without a durable
            # child identity we cannot prove absence in this same namespace,
            # even for manual rearm. Native container replacement resolves it.
            self.latch("interrupted_spawn")
        elif self.state["active"] is not None:
            prior = self.state["active"]
            # PID reuse is not ownership; do not signal or overlap anything
            # still occupying that PID on the same boot.
            current = process_identity(prior["pid"])
            if current is not None and current["boot_id"] == prior["boot_id"]:
                self.latch("orphan_child")
            else:
                self.state["active"] = None
                self.state["reason"] = "controller_death"
                self.save()

    def mark_child(self, identity: dict) -> None:
        """Record owned child identity after Popen, without refunding the intent."""
        self.state.update(active=identity, inflight=False)
        self.save()

    def child_ended(self) -> None:
        """Forget a reaped child, preserving every charged attempt."""
        self.state.update(active=None, inflight=False)
        self.save()


# Full six-attempt startup envelope: discovery + connect + cancellation,
# cleanup and bounded reporting per attempt, jittered backoffs, scheduling margin.
# This deliberately overestimates, rather than killing a valid 90s connect at 30s.
STARTUP_GRACE = 6 * (30 + 90 + 10 + 10 + 10) + sum((1, 2, 4, 8, 16)) * 1.2 + 60
HEALTH_PATH = Path("/dev/shm/casambi-health.json")  # noqa: S108 — fixed private container RAM tmpfs, not shared host /tmp.


def process_identity(pid: int) -> dict | None:
    """Read actual kernel PID/start ticks/boot ID, never a cached health claim."""
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return {
            "pid": pid,
            "start_ticks": int(fields[19]),
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        }
    except (OSError, ValueError, IndexError):
        return None


def health_fresh(
    record: object, identity: dict | None, now: float, heartbeat: int
) -> bool:
    """Local loop freshness only: not publication, BLE RF, or end-to-end proof."""
    if (
        not isinstance(record, dict)
        or not isinstance(identity, dict)
        or not valid_identity(identity)
    ):
        return False
    updated = record.get("updated_monotonic")
    if (
        record.get("record_version") != 1
        or any(record.get(key) != value for key, value in identity.items())
        or process_identity(identity["pid"]) != identity
        or not isinstance(updated, int | float)
        or isinstance(updated, bool)
        or not math.isfinite(updated)
        or not 0 <= updated <= now
        or record.get("phase")
        not in {"starting", "discovering", "connecting", "ready", "degraded", "offline"}
    ):
        return False
    timeout = (
        max(120, 3 * heartbeat + 20)
        if record["phase"] in {"ready", "degraded"}
        else STARTUP_GRACE
    )
    return now - updated <= timeout


class BridgeChild:
    """One fixed Python bridge, with identity-checked bounded shutdown/reaping."""

    def __init__(
        self,
        data_dir: Path,
        server_path: Path,
        values: dict[str, str],
        *,
        stop_timeout: float = 5,
    ) -> None:
        self.data_dir = data_dir
        self.server_path = server_path
        self.values = values
        self.stop_timeout = stop_timeout
        self.process = None
        self.identity = None

    def start(self) -> None:
        """Never accept command arguments from recovered settings."""
        if self.process is not None:
            raise RuntimeError
        environment = os.environ.copy()
        # Validated secrets remain literal; inherited bridge settings cannot
        # bypass validation. Keep unrelated process environment (PATH, DBus).
        for key in (
            "MQTT_BROKER",
            "MQTT_PORT",
            "MQTT_USERNAME",
            "MQTT_PASSWORD",
            "CASAMBI_NETWORK_ADDRESS",
            "CASAMBI_NETWORK_PASSWORD",
            "CASAMBI_NETWORK_NAME",
            "LOG_LEVEL",
            "HEALTH_HEARTBEAT_SECONDS",
            "HEALTH_RECORD_PATH",
        ):
            environment.pop(key, None)
        environment.update(self.values)
        environment.update(
            PYTHON_DOTENV_DISABLED="1",
            HEALTH_RECORD_PATH=str(HEALTH_PATH),
        )
        self.process = subprocess.Popen(  # noqa: S603 — fixed interpreter/script, no shell.
            [sys.executable, str(self.server_path)],
            cwd=self.data_dir,
            env=environment,
            stdin=subprocess.DEVNULL,
            # The unchanged bridge may log transport exceptions; do not leak
            # those through the HAOS controller's sanitized log boundary.
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.identity = process_identity(self.process.pid)
        if self.identity is None:
            # A quickly exited child is still ours and must be reaped.
            self.process.wait(timeout=self.stop_timeout)

    def stop(self) -> None:
        """Signal only our unreaped Popen child with its original kernel identity."""
        if self.process is None or self.process.poll() is not None:
            return
        if self.identity is None or process_identity(self.process.pid) != self.identity:
            raise RuntimeError
        self.process.terminate()
        try:
            self.process.wait(timeout=self.stop_timeout)
        except subprocess.TimeoutExpired:
            if process_identity(self.process.pid) != self.identity:
                raise RuntimeError from None
            self.process.kill()
            self.process.wait(timeout=self.stop_timeout)


def is_mqtt_auth(error: BaseException, *, connecting: bool) -> bool:
    """Classify machine reason codes, never exception text or local publish rc4."""
    import aiomqtt

    if not isinstance(error, aiomqtt.MqttCodeError):
        return False
    rc = error.rc
    numeric = getattr(rc, "value", rc)
    if type(numeric) is not int:
        return False
    return numeric in {0x86, 0x87} or (
        connecting and type(rc) is int and numeric in {4, 5}
    )


def mqtt_client(values: dict[str, str]) -> Any:
    """Construct the single controller MQTT transport; no subscriptions/commands."""
    import aiomqtt

    return aiomqtt.Client(
        hostname=values["MQTT_BROKER"],
        port=int(values["MQTT_PORT"]),
        username=values["MQTT_USERNAME"],
        password=values["MQTT_PASSWORD"],
        timeout=10,
        # Isolate third-party logging without mutating any global logger.
        logger=logging.Logger(  # noqa: LOG001
            "haos-mqtt-silent", level=logging.CRITICAL + 1
        ),
    )


class Notifier:
    """One shared auth detector and durable retained QoS1 recovery outbox."""

    def __init__(
        self,
        store: RecoveryStore,
        values: dict[str, str],
        *,
        client_factory: Callable[[dict[str, str]], Any] = mqtt_client,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.store = store
        self.values = values
        self.factory = client_factory
        self.clock = clock
        self.client = None
        self.transport = None
        self.context_open = False
        self.monitor = None
        self.next_at = 0.0
        self.backoff = 1.0

    @staticmethod
    async def _monitor(client: Any) -> None:
        # Public aiomqtt message iterator also reports disconnects. We never
        # subscribe, and unsolicited messages cannot invoke commands.
        async for _ in client.messages:
            pass

    async def tick(self) -> None:
        """Retry transport failures independently of the bridge replacement budget."""
        import aiomqtt

        if self.store.failed or self.clock() < self.next_at:
            return
        connecting = self.client is None
        replay_failed = connecting and self.store.state["latched"]
        try:
            if self.client is None:
                if self.transport is None:
                    self.transport = self.factory(self.values)
                self.context_open = True
                # aiomqtt supplies its own bounded CONNACK timeout. Do not cancel
                # the executor's DNS/connect and start a second overlapping one.
                await self.transport.__aenter__()
                self.client = self.transport
                self.monitor = asyncio.create_task(self._monitor(self.client))
            if self.monitor is not None and self.monitor.done():
                self.monitor.result()
                raise aiomqtt.MqttError  # noqa: TRY301 — unify reconnect cleanup.
            connecting = False
            outbox = self.store.state["notification"]
            if replay_failed:
                # Retained failure is replayed once per new connection, with the
                # same incident ID (consumer dedupe), never a spurious armed state.
                self.store.notify("failed", self.store.state["reason"])
                self.store.save()
            payload = outbox["pending"]
            if payload is not None:
                if replay_failed or payload != outbox["acked"]:
                    topic = f"casambi/{self.values['CASAMBI_NETWORK_NAME']}/recovery"
                    await asyncio.wait_for(
                        self.client.publish(
                            topic,
                            json.dumps(payload, separators=(",", ":")),
                            qos=1,
                            retain=True,
                        ),
                        timeout=12,
                    )
                # Awaited QoS1 publish is PUBACK, not merely queued output.
                # If state changed while awaiting, never ack the newer payload.
                if outbox["pending"] == payload:
                    outbox.update(pending=None, acked=payload)
                    self.store.save()
            self.backoff = 1.0
        except (aiomqtt.MqttError, OSError, TimeoutError) as error:
            if isinstance(error, OSError) and self.store.failed:
                raise
            if is_mqtt_auth(error, connecting=connecting) and (
                not self.store.state["latched"]
                or self.store.state["reason"] != "mqtt_auth"
            ):
                self.store.latch("mqtt_auth")
            await self.close()
            self.next_at = self.clock() + self.backoff
            self.backoff = min(self.backoff * 2, 60)

    async def close(self) -> None:
        """Close before reconnecting; no second live controller connection."""
        if self.monitor is not None:
            self.monitor.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.monitor
            self.monitor = None
        self.client = None
        if self.context_open:
            self.context_open = False
            with contextlib.suppress(Exception):
                await asyncio.wait_for(
                    self.transport.__aexit__(None, None, None), timeout=12
                )

    async def run(self) -> None:
        """Run independently so broker I/O never supplies the control heartbeat."""
        try:
            while True:
                await self.tick()
                await asyncio.sleep(1)
        finally:
            await self.close()


def load_helper() -> ModuleType:
    """Load the adjacent launcher without importing any HA package."""
    spec = importlib.util.spec_from_file_location(
        "haos_bridge_launcher",
        Path(__file__).with_name("run.py"),
    )
    if spec is None or spec.loader is None:
        raise ValueError
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    return helper


class Controller:
    """Keep the outer container alive, even when bridge recovery is latched."""

    def __init__(
        self,
        data_dir: Path = Path("/data"),
        server_path: Path = Path("/app/server.py"),
        *,
        clock: Callable[[], float] = time.monotonic,
        client_factory: Callable[[dict[str, str]], Any] = mqtt_client,
    ) -> None:
        self.data_dir = data_dir
        self.server_path = server_path
        self.clock = clock
        self.factory = client_factory
        self.store = None
        self.child = None
        self.notifier = None
        self.notifier_task = None
        self.http = None
        self.http_thread = None
        self.last_tick = clock()
        self.next_at = clock()
        self.started_at = 0.0
        self.mode = "disabled"
        self.values = {}
        self.stopping = asyncio.Event()

    async def initialize(self) -> None:
        """Validate once; disabled and invalid modes make no bridge/MQTT calls."""
        try:
            self.store = RecoveryStore(self.data_dir)
            self.store.reconcile()
        except (OSError, ValueError, TypeError):
            print("Recovery state unavailable; bridge stopped", file=sys.stderr)
            return
        try:
            helper = load_helper()
            options = helper._read_options(self.data_dir / "options.json")  # noqa: SLF001 — reuse the adjacent launcher validator, not HA imports.
            generation = options.get("recovery_reset_generation", 0)
            self.store.rearm(generation)
            mode = options.get("mode", "disabled")
            if mode == "disabled":
                return
            if mode == "preflight":
                try:
                    await asyncio.wait_for(helper.preflight(), timeout=25)
                except Exception:  # noqa: BLE001 — fixed message, no D-Bus identifiers.
                    print("Bluetooth preflight failed", file=sys.stderr)
                return
            self.values = helper.bridge_environment(
                self.data_dir,
                self.server_path,
                options.get("env", ""),
            )
            self.notifier = Notifier(
                self.store,
                self.values,
                client_factory=self.factory,
                clock=self.clock,
            )
            self.mode = "bridge"
        except Exception:  # noqa: BLE001 — validation errors may include secret literals.
            print("Invalid bridge configuration", file=sys.stderr)
            with contextlib.suppress(OSError):
                self.store.latch("configuration")

    def liveness_status(self) -> int:
        """Only controller-loop progress; terminal bridge failure remains HTTP200."""
        return (
            HTTPStatus.OK
            if 0 <= self.clock() - self.last_tick < CONTROLLER_STALL_SECONDS
            else HTTPStatus.SERVICE_UNAVAILABLE
        )

    def start_http(self, *, host: str = "0.0.0.0", port: int = 8099) -> None:  # noqa: S104 — internal container endpoint, no mapped port.
        """Serve fixed secret-free liveness from a thread, never updating the tick."""
        controller = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                status = (
                    controller.liveness_status()
                    if self.path == "/supervisor-health"
                    else HTTPStatus.NOT_FOUND
                )
                body = (
                    b"controller alive\n"
                    if status == HTTPStatus.OK
                    else b"unavailable\n"
                )
                self.send_response(status)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_: object) -> None:
                pass

        self.http = ThreadingHTTPServer((host, port), Handler)
        self.http.daemon_threads = True
        self.http_thread = threading.Thread(target=self.http.serve_forever, daemon=True)
        self.http_thread.start()

    async def _stop_child(self) -> None:
        if self.child is not None:
            await asyncio.to_thread(self.child.stop)
            self.child = None
            if self.store is not None and not self.store.failed:
                self.store.child_ended()

    def _failure(self, reason: str) -> None:
        self.store.child_ended()
        if reason == "configuration":
            self.store.latch(reason)
        elif self.store.state["attempts"] >= MAX_REPLACEMENTS:
            self.store.latch("retry_exhausted")
        else:
            self.store.state["reason"] = reason
            self.store.notify("recovering", reason)
            self.store.save()
            self.next_at = self.clock() + (5, 15, 30)[self.store.state["attempts"]]
            log_event("retry_scheduled", reason, self.store.state["attempts"])

    def _read_health(self) -> object:
        try:
            if HEALTH_PATH.stat().st_size > MAX_RECORD_BYTES:
                return None
            return json.loads(HEALTH_PATH.read_text())
        except (OSError, ValueError):
            return None

    # Keep policy explicit in one place, with fail-closed early returns.
    async def tick(self) -> None:  # noqa: PLR0911, PLR0912
        """Make one bounded policy step. Only this main loop supplies liveness."""
        self.last_tick = self.clock()
        if self.mode != "bridge" or self.store is None or self.stopping.is_set():
            return
        try:
            if self.notifier_task is not None and self.notifier_task.done():
                # An unexpected notifier/storage failure is not safe to ignore.
                self.notifier_task.result()
                self.store.latch("storage_failure")
            if self.store.failed or self.store.state["latched"]:
                await self._stop_child()
                return
            if self.child is not None:
                code = self.child.process.poll()
                if code is not None:
                    self.child = None  # poll reaped the actual Popen child.
                    self._failure(
                        "configuration"
                        if code == EXIT_CONFIGURATION
                        else "child_failure"
                    )
                    return
                record = self._read_health()
                heartbeat = int(self.values.get("HEALTH_HEARTBEAT_SECONDS", "30"))
                # A broker outage can stall initial connection/reconnect reporting.
                # Defer advisory health-only recovery until our single transport
                # can connect again. Child exits and definite auth still take effect.
                if health_fresh(
                    record, self.child.identity, self.clock(), heartbeat
                ) or (self.notifier is not None and self.notifier.client is None):
                    return
                timeout = STARTUP_GRACE
                if (
                    isinstance(record, dict)
                    and self.child.identity is not None
                    and all(
                        record.get(key) == value
                        for key, value in self.child.identity.items()
                    )
                    and record.get("phase") in {"ready", "degraded"}
                ):
                    timeout = max(120, 3 * heartbeat + 20)
                if self.clock() - self.started_at > timeout:
                    await self._stop_child()
                    self._failure("health_stale")
                return
            if self.clock() < self.next_at or not self.store.charge():
                return
            self.child = BridgeChild(self.data_dir, self.server_path, self.values)
            self.started_at = self.clock()
            try:
                self.child.start()
            except OSError:
                self.child = None
                self._failure("spawn_failed")
                return
            if self.child.identity is not None:
                self.store.mark_child(self.child.identity)
        except Exception:  # noqa: BLE001 — terminal boundary must not leak credentials.
            # Any ambiguous write/ownership error forbids further launch. Leave
            # its durable active/intent marker intact for the next controller.
            self.mode = "failed"
            if not self.store.failed:
                with contextlib.suppress(OSError):
                    self.store.latch("controller_failure")
            with contextlib.suppress(OSError, RuntimeError):
                await self._stop_child()
            print("Recovery controller failed closed", file=sys.stderr)

    async def close(self) -> None:
        """Stop cleanly without rearming or restarting a bridge child."""
        self.stopping.set()
        if self.notifier_task is not None:
            self.notifier_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.notifier_task
        elif self.notifier is not None:
            await self.notifier.close()
        with contextlib.suppress(OSError, RuntimeError):
            await self._stop_child()
        if self.store is not None:
            if not self.store.failed:
                self.store.state["controller_active"] = False
                with contextlib.suppress(OSError):
                    self.store.save()
            self.store.lock.close()
        if self.http is not None:
            await asyncio.to_thread(self.http.shutdown)
            self.http.server_close()
            self.http_thread.join(timeout=2)

    async def run(self, *, host: str = "0.0.0.0", port: int = 8099) -> None:  # noqa: S104 — container-internal liveness.
        """Remain running in inert/latched modes; native watchdog must not loop."""
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, self.stopping.set)
        try:
            self.start_http(host=host, port=port)
            await self.initialize()
            if self.notifier is not None:
                self.notifier_task = asyncio.create_task(self.notifier.run())
            while not self.stopping.is_set():
                await self.tick()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.stopping.wait(), timeout=1)
        finally:
            await self.close()
            for signum in (signal.SIGTERM, signal.SIGINT):
                loop.remove_signal_handler(signum)


def main() -> None:
    """Run the fixed HAOS CLI without arbitrary commands, paths or flags."""
    os.umask(0o077)
    asyncio.run(Controller().run())


if __name__ == "__main__":
    main()
