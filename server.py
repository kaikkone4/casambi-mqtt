import asyncio
import contextlib
import json
import logging
import os
import re
import secrets
import signal
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import aiomqtt
import aiomqtt.client
import CasambiBt
from CasambiBt import Casambi, discover
from dotenv import load_dotenv

from custom_components.casambi_mqtt import health_protocol
from custom_components.casambi_mqtt.const import is_valid_network_name
from custom_components.casambi_mqtt.entities.commands import (
    PublishEntities,
    SetLevel,
    SetScene,
    TurnOn,
)
from custom_components.casambi_mqtt.entities.entities import (
    Scene,
    Unit,
    UnitControl,
    UnitControlType,
    UnitState,
    UnitType,
)
from switch_decoder import install_switch_event_decoder

if TYPE_CHECKING:
    from bleak import BLEDevice

TOPIC_PREFIX = "casambi"
MAX_BRIGHTNESS = 255
SWITCH_PROBE_SECONDS = 90
MAX_SWITCH_EVENT_VALUE = 255
SWITCH_EVENT_DEDUP_SECONDS = 0.25
SUPPORTED_SWITCH_EVENTS = frozenset({"PRESS", "RELEASE", "HOLD", "RELEASE_AFTER_HOLD"})
EXIT_BLE_DISCONNECTED = 75
DRAIN_TIMEOUT = 2.0
SWITCH_EVENT_QUEUE_MAX = 64
HEARTBEAT_SECONDS = 5
LOOP_LAG_WARN_SECONDS = 1.0
DIAGNOSTICS_INTERVAL_SECONDS = 300
PUBLISH_RETRY_SECONDS = 1.0
REDACTED_MESSAGE_MAX_LENGTH = 160
DEFAULT_HEALTH_HEARTBEAT_SECONDS = 30
MIN_HEALTH_HEARTBEAT_SECONDS = 5
MAX_HEALTH_HEARTBEAT_SECONDS = 300
HEALTH_PUBLISH_TIMEOUT = 10.0
FINAL_STATUS_TIMEOUT = 2.0
HEALTH_REPLY_MIN_INTERVAL = 1.0
SNAPSHOT_REQUEST_MIN_INTERVAL = 30.0

load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
NETWORK_ADDRESS = os.getenv("CASAMBI_NETWORK_ADDRESS")
NETWORK_PASSWORD = os.getenv("CASAMBI_NETWORK_PASSWORD")
NETWORK_NAME = os.getenv("CASAMBI_NETWORK_NAME", "default")
HEALTH_HEARTBEAT_SECONDS = os.getenv(
    "HEALTH_HEARTBEAT_SECONDS", str(DEFAULT_HEALTH_HEARTBEAT_SECONDS)
)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(LOG_LEVEL)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
)
LOGGER.addHandler(handler)


class RedactingFilter(logging.Filter):
    """
    Aggressively scrub CasambiBt log records before they reach any handler.

    CasambiBt's WARNING/ERROR sites interpolate raw BLE packet bytes and HTTP
    response bodies into f-strings (e.g. ``_client.py`` ``b2a(data)``, or
    ``_network.py`` ``res.text``), and several attach a full traceback via
    ``exc_info=True``. Because these are f-strings rather than %-style
    templates, there is no argument to allowlist and no fixed message shape
    to trust on a future upstream release, so the safe posture is to keep
    only a small fixed character set and drop everything else -- this is
    also why hex-looking runs and colon/hyphen-separated hex groups (MAC
    addresses, UUID segments) are stripped as a separate pass before the
    character allowlist: those are built entirely from letters and digits
    that are individually harmless, so the allowlist alone would let a MAC
    address or a raw byte run written in hex survive untouched.
    """

    _BYTES_LITERAL = re.compile(r"b(['\"])(?:(?!\1).)*\1")
    _MAC_LIKE = re.compile(r"(?:[0-9A-Fa-f]{2}[:-]){2,}[0-9A-Fa-f]{2}")
    _HEX_RUN = re.compile(r"[0-9A-Fa-f]{6,}")
    _DISALLOWED = re.compile(r"[^A-Za-z ,.:;!?'()/-]")
    _WHITESPACE = re.compile(r"\s+")

    def filter(self, record: logging.LogRecord) -> bool:
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        message = record.getMessage()
        # CasambiBt/_network.py logs an HTTP error body on a continuation
        # line after the status code (f"Update failed: {code}\n{res.text}").
        # That body is arbitrary cloud-API JSON/text -- ordinary letters
        # survive the allowlist below, so a first-line-only cut is the one
        # thing that reliably keeps it out regardless of what it contains.
        message = message.split("\n", 1)[0]
        message = self._BYTES_LITERAL.sub("", message)
        message = self._MAC_LIKE.sub("", message)
        message = self._HEX_RUN.sub("", message)
        message = self._DISALLOWED.sub("", message)
        message = self._WHITESPACE.sub(" ", message).strip()
        record.msg = message[:REDACTED_MESSAGE_MAX_LENGTH]
        record.args = ()
        return True


def configure_logging() -> None:
    """
    Route third-party loggers through the bridge's handler without a root handler.

    aiomqtt.Client, constructed without an explicit ``logger=`` kwarg (as the
    bridge does), defaults to ``aiomqtt.client.MQTT_LOGGER`` --
    ``logging.getLogger("mqtt")``. There is no logger literally named
    "aiomqtt" anywhere in the dependency chain. aiomqtt's own
    ``Client.__init__`` also calls ``self._client.enable_logger(logger)``
    with that same object, so paho-mqtt shares it: paho's WARNING/ERROR
    sites carry no topics or payloads (audited: the only one is an
    unrecognised-command byte), so it is safe to leave unfiltered -- but at
    DEBUG paho logs the full topic and byte count for every publish, which
    is exactly why this logger's level is pinned to WARNING rather than left
    at its default.

    CasambiBt logs from many per-module loggers (``CasambiBt._client``,
    ``CasambiBt._casambi``, ...), all children of ``CasambiBt`` that
    propagate up to it. A filter attached to the ``CasambiBt`` logger itself
    is only consulted for records that originate there, not for records
    propagating up from a child logger -- Python's logging module only
    checks a *handler's* filters while walking the hierarchy, not every
    ancestor logger's filters. So the redaction filter has to live on a
    handler, and it must be a handler dedicated to CasambiBt: the handler
    shared with the mqtt logger and ``__main__`` must stay unfiltered for
    those, so the filter cannot live on that shared handler either. Hence
    CasambiBt gets its own ``StreamHandler`` (same format as the bridge's
    own).

    Idempotent so ``cli()`` can call it unconditionally; no root handler and
    no ``logging.basicConfig()`` -- everything outside these loggers keeps
    Python's default behaviour untouched. The idempotency guard is a
    dedicated flag rather than "does CasambiBt already have a handler",
    since anything else attaching a handler to CasambiBt first would
    otherwise trip that check and silently skip configuring the mqtt logger
    too.
    """
    if configure_logging.configured:
        return
    configure_logging.configured = True

    mqtt_logger = aiomqtt.client.MQTT_LOGGER
    mqtt_logger.setLevel(logging.WARNING)
    mqtt_logger.propagate = False
    mqtt_logger.addHandler(handler)

    casambi_logger = logging.getLogger("CasambiBt")
    casambi_handler = logging.StreamHandler()
    casambi_handler.setFormatter(handler.formatter)
    casambi_handler.addFilter(RedactingFilter())
    casambi_logger.setLevel(logging.WARNING)
    casambi_logger.propagate = False
    casambi_logger.addHandler(casambi_handler)


configure_logging.configured = False


@dataclass
class BridgeDiagnostics:
    """
    Bridge-owned health counters: integers only, never unit/topic/network data.

    Updated by the publishers, the callbacks and the heartbeat. Exists so an
    operator can see the bridge is keeping up (or isn't) without any of the
    data it carries ever needing to appear in a log line.
    """

    unit_callbacks: int = 0
    commands_received: int = 0
    unit_queue_depth: int = 0
    unit_queue_peak: int = 0
    unit_publish_attempted: int = 0
    unit_publish_confirmed: int = 0
    unit_publish_failed: int = 0
    unit_publish_coalesced: int = 0
    switch_queue_depth: int = 0
    switch_publish_attempted: int = 0
    switch_dropped: int = 0
    ble_disconnects: int = 0
    loop_lag_ms_max: int = 0
    health_status_published: int = 0
    health_status_failed: int = 0
    health_requests_rate_limited: int = 0

    def as_log_fields(self) -> str:
        return " ".join(
            f"{field.name}={getattr(self, field.name)}" for field in fields(self)
        )


def health_heartbeat_seconds() -> int:
    """Return the validated status heartbeat interval, or raise on bad config."""
    try:
        value = int(HEALTH_HEARTBEAT_SECONDS)
    except (TypeError, ValueError):
        value = -1
    if not MIN_HEALTH_HEARTBEAT_SECONDS <= value <= MAX_HEALTH_HEARTBEAT_SECONDS:
        message = (
            "HEALTH_HEARTBEAT_SECONDS must be an integer between "
            f"{MIN_HEALTH_HEARTBEAT_SECONDS} and {MAX_HEALTH_HEARTBEAT_SECONDS}"
        )
        raise RuntimeError(message)
    return value


@dataclass(frozen=True)
class SnapshotResult:
    """Truthful outcome of one snapshot: only broker-confirmed publishes count."""

    units: int
    scenes: int
    failed: int

    @property
    def ok(self) -> bool:
        return self.failed == 0


def _utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class BridgeHealth:
    """
    Process-lifetime health state behind the v1 status protocol.

    One instance per bridge process, shared by every MQTT session, so the
    ``instance_id`` and ``sequence`` survive an MQTT reconnect but never a
    process restart: a new process always starts at ``phase=starting`` with a
    fresh random identity and cannot inherit a previous instance's ``ready``.
    Durations use the monotonic clock; the UTC ``time`` field is diagnostic
    only and is never used for freshness by any consumer.

    What a status or reply proves: this process is alive, its MQTT session
    works, and it believes its BLE link is connected. It does NOT prove a
    physical radio round trip -- unit data comes from casambi-bt's in-memory
    ``casa.units`` cache, and casambi-bt 0.3.2 exposes no non-actuating
    ping. See docs/health-protocol.md.
    """

    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], str] = _utc_now,
        instance_id: str | None = None,
    ) -> None:
        self.instance_id = instance_id or secrets.token_hex(16)
        self._monotonic = monotonic
        self._wall_clock = wall_clock
        self._started = monotonic()
        self.sequence = 0
        self.phase = health_protocol.PHASE_STARTING
        self.ble = health_protocol.LINK_UNKNOWN
        self.mqtt = health_protocol.LINK_UNKNOWN
        self.error = health_protocol.ERROR_NONE
        self.state_publish_failing: Callable[[], bool] = lambda: False
        self._snapshot = health_protocol.SnapshotSummary()
        self._snapshot_completed_at: float | None = None
        self._last_reply_at: float | None = None
        self._last_snapshot_request_at: float | None = None
        self._status_failure_streak = 0

    def __repr__(self) -> str:
        """Fixed-shape repr: integers and fixed enums only."""
        return f"BridgeHealth(phase={self.phase}, sequence={self.sequence})"

    def set_phase(self, phase: str, error: str = health_protocol.ERROR_NONE) -> None:
        self.phase = phase
        self.error = error

    def session_started(self) -> None:
        """Start a new MQTT session at ``starting`` until its snapshot lands."""
        self.ble = health_protocol.LINK_CONNECTED
        self.mqtt = health_protocol.LINK_CONNECTED
        self.set_phase(health_protocol.PHASE_STARTING)

    def record_snapshot(self, result: SnapshotResult) -> int:
        """Record a completed snapshot and return its generation number."""
        generation = self._snapshot.generation + 1
        self._snapshot = health_protocol.SnapshotSummary(
            generation=generation,
            units=result.units,
            scenes=result.scenes,
            failed=result.failed,
        )
        self._snapshot_completed_at = self._monotonic()
        if result.ok:
            self.set_phase(health_protocol.PHASE_READY)
        else:
            self.set_phase(
                health_protocol.PHASE_DEGRADED,
                health_protocol.ERROR_SNAPSHOT_PUBLISH_FAILED,
            )
        return generation

    def _effective_phase(self) -> tuple[str, str]:
        if self.phase == health_protocol.PHASE_READY and self.state_publish_failing():
            return (
                health_protocol.PHASE_DEGRADED,
                health_protocol.ERROR_STATE_PUBLISH_FAILED,
            )
        return self.phase, self.error

    def build_status(self) -> health_protocol.BridgeStatus:
        """Return the next status message; every call consumes one sequence."""
        self.sequence += 1
        now = self._monotonic()
        age = (
            None
            if self._snapshot_completed_at is None
            else int(now - self._snapshot_completed_at)
        )
        snapshot = health_protocol.SnapshotSummary(
            generation=self._snapshot.generation,
            units=self._snapshot.units,
            scenes=self._snapshot.scenes,
            failed=self._snapshot.failed,
            age_s=age,
        )
        phase, error = self._effective_phase()
        return health_protocol.BridgeStatus(
            instance_id=self.instance_id,
            sequence=self.sequence,
            phase=phase,
            ble=self.ble,
            mqtt=self.mqtt,
            uptime_s=int(now - self._started),
            snapshot=snapshot,
            error=error,
            time=self._wall_clock(),
        )

    def will_payload(self) -> str:
        """
        Last-will status, fixed at MQTT client creation.

        It cannot carry a live sequence, so it uses ``sequence: null``; the
        protocol only allows that for ``phase=offline``, which no consumer
        ever treats as healthy.
        """
        return health_protocol.BridgeStatus(
            instance_id=self.instance_id,
            sequence=None,
            phase=health_protocol.PHASE_OFFLINE,
            ble=health_protocol.LINK_UNKNOWN,
            mqtt=health_protocol.LINK_DISCONNECTED,
            uptime_s=None,
            snapshot=health_protocol.SnapshotSummary(),
            error=health_protocol.ERROR_CONNECTION_LOST,
            time=None,
        ).to_json()

    async def publish_status(
        self,
        client: aiomqtt.Client,
        diagnostics: BridgeDiagnostics | None = None,
        *,
        publish_timeout: float = HEALTH_PUBLISH_TIMEOUT,
    ) -> bool:
        """Publish one retained status, bounded so it can never block a caller."""
        try:
            await asyncio.wait_for(
                client.publish(
                    health_protocol.status_topic(TOPIC_PREFIX, NETWORK_NAME),
                    payload=self.build_status().to_json(),
                    qos=1,
                    retain=True,
                ),
                timeout=publish_timeout,
            )
        except (aiomqtt.MqttError, TimeoutError):
            self._status_failure_streak += 1
            if diagnostics is not None:
                diagnostics.health_status_failed += 1
            if self._status_failure_streak == 1:
                LOGGER.warning("Bridge status publish failed")
            return False
        self._status_failure_streak = 0
        if diagnostics is not None:
            diagnostics.health_status_published += 1
        return True

    def reply_allowed(self) -> bool:
        """Coalesce health requests: at most one reply per interval."""
        now = self._monotonic()
        if (
            self._last_reply_at is not None
            and now - self._last_reply_at < HEALTH_REPLY_MIN_INTERVAL
        ):
            return False
        self._last_reply_at = now
        return True

    def snapshot_request_allowed(self) -> bool:
        """Bound correlated snapshot probes so they cannot become a flood."""
        now = self._monotonic()
        if (
            self._last_snapshot_request_at is not None
            and now - self._last_snapshot_request_at < SNAPSHOT_REQUEST_MIN_INTERVAL
        ):
            return False
        self._last_snapshot_request_at = now
        return True

    async def publish_reply(
        self,
        client: aiomqtt.Client,
        request_id: str,
        result: str,
        snapshot: SnapshotResult | None = None,
        generation: int | None = None,
    ) -> None:
        status = self.build_status()
        if snapshot is None:
            reply = health_protocol.HealthReply(
                health_protocol.REPLY_STATUS,
                request_id,
                self.instance_id,
                result,
                status,
            )
        else:
            reply = health_protocol.HealthReply(
                health_protocol.REPLY_SNAPSHOT,
                request_id,
                self.instance_id,
                result,
                status,
                units=snapshot.units,
                scenes=snapshot.scenes,
                failed=snapshot.failed,
                generation=generation or 0,
            )
        await asyncio.wait_for(
            client.publish(
                health_protocol.reply_topic(TOPIC_PREFIX, NETWORK_NAME),
                payload=reply.to_json(),
                qos=1,
                retain=False,
            ),
            timeout=HEALTH_PUBLISH_TIMEOUT,
        )


def sanitize_switch_event(event: Any) -> dict[str, int | str] | None:
    """Return the public semantic fields from a supported switch event."""
    try:
        event_type = event.event.name
        button = event.button
        unit_id = event.unit_id
        if (
            event_type not in SUPPORTED_SWITCH_EVENTS
            or type(button) is not int
            or type(unit_id) is not int
            or not 0 <= button <= MAX_SWITCH_EVENT_VALUE
            or not 0 <= unit_id <= MAX_SWITCH_EVENT_VALUE
        ):
            return None
    except Exception:  # noqa: BLE001  # Never leak dependency/event details.
        return None
    else:
        return {"unit_id": unit_id, "button": button, "event": event_type}


def emit_switch_event(event: Any) -> None:
    """Write only the supported semantic fields from a Casambi switch event."""
    try:
        record = sanitize_switch_event(event)
        if record is None:
            return
        sys.stdout.write(json.dumps(record, separators=(",", ":")) + "\n")
        sys.stdout.flush()
    except Exception:  # noqa: BLE001  # Never leak dependency/event details.
        return


def switch_event_dedup_key(
    event: Any, record: dict[str, int | str]
) -> tuple[object, ...]:
    """
    Return the private in-process identity used to collapse duplicates.

    The decoder knows, from the frame origin, which events are retransmissions
    of one physical action and which are separate actions that happen to share
    their public fields. Prefer that identity: two genuine presses of the same
    button 100 ms apart look identical in the public payload, and deduplicating
    on the payload alone would silently drop the second one.

    Callbacks that did not come from the decoder have no such identity, so they
    keep the original conservative behaviour of collapsing an identical burst.
    """
    identity = getattr(event, "dedup_identity", None)
    if identity is not None:
        return ("decoded", identity)
    return ("callback", record["unit_id"], record["button"], record["event"])


class SwitchEventPublisher:
    """
    Publish sanitized switch events through one worker draining a bounded FIFO.

    Overload policy: physical presses are not coalesced (unlike unit state,
    see UnitStatePublisher) -- every distinct press must reach MQTT. The
    64-slot queue exists only as a defensive bound on worst-case memory: at
    human press rates, with one worker continuously draining it, filling it
    is not expected to happen in practice. If it ever does, the newest event
    is dropped (not queued) and at most one WARNING is emitted for the whole
    session, carrying only a count -- never which event was dropped.
    """

    def __init__(
        self,
        client: aiomqtt.Client,
        *,
        monotonic: Any = time.monotonic,
        diagnostics: BridgeDiagnostics | None = None,
    ) -> None:
        self.client = client
        self.monotonic = monotonic
        self.diagnostics = diagnostics
        self.last_seen: dict[tuple[object, ...], float] = {}
        self.dropped = 0
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=SWITCH_EVENT_QUEUE_MAX)
        self._worker_task: asyncio.Task[None] | None = None
        self._dropped_warned = False

    def __repr__(self) -> str:
        """Fixed-shape repr: class name plus integer counters only, no events."""
        return (
            f"SwitchEventPublisher(queued={self._queue.qsize()}, "
            f"dropped={self.dropped})"
        )

    def _purge_expired(self, now: float) -> None:
        """
        Drop entries that can no longer suppress anything.

        Decoded events are keyed by a per-action identity, so without this the
        table would gain one permanent entry per physical press for as long as
        the bridge runs. An entry older than the window already fails the check
        below, so removing it changes no decision.
        """
        cutoff = now - SWITCH_EVENT_DEDUP_SECONDS
        for key, seen_at in list(self.last_seen.items()):
            if seen_at <= cutoff:
                del self.last_seen[key]

    async def publish(self, event: Any) -> None:
        record = sanitize_switch_event(event)
        if record is None:
            return

        now = self.monotonic()
        self._purge_expired(now)
        key = switch_event_dedup_key(event, record)
        previous = self.last_seen.get(key)
        if previous is not None and now - previous < SWITCH_EVENT_DEDUP_SECONDS:
            return
        self.last_seen[key] = now

        await log_exceptions(
            self.client.publish(
                f"{TOPIC_PREFIX}/{NETWORK_NAME}/switch_events",
                payload=json.dumps(record, separators=(",", ":")),
                qos=1,
                retain=False,
            )
        )

    def __call__(self, event: Any) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self.dropped += 1
            if self.diagnostics is not None:
                self.diagnostics.switch_dropped += 1
            if not self._dropped_warned:
                self._dropped_warned = True
                LOGGER.warning(
                    "Dropping Casambi switch events due to a full queue: %d",
                    self.dropped,
                )
        if self.diagnostics is not None:
            self.diagnostics.switch_queue_depth = self._queue.qsize()

    async def _worker(self) -> None:
        while True:
            event = await self._queue.get()
            if self.diagnostics is not None:
                self.diagnostics.switch_queue_depth = self._queue.qsize()
                self.diagnostics.switch_publish_attempted += 1
            try:
                await self.publish(event)
            except Exception:  # noqa: BLE001  # Never let one publish kill the worker.
                LOGGER.warning("Switch event publish failed")
            finally:
                self._queue.task_done()

    async def start(self) -> None:
        self._worker_task = asyncio.create_task(self._worker())

    async def aclose(self, *, drain_timeout: float = DRAIN_TIMEOUT) -> None:
        task = self._worker_task
        if task is None:
            return
        self._worker_task = None
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._queue.join(), timeout=drain_timeout)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def configured_network_device(devices: list[Any]) -> Any | None:
    """Select the bridge-configured network from discovery results."""
    configured = None
    for device in devices:
        if device.address == NETWORK_ADDRESS:
            configured = device
    return configured


def create_casambi_connection() -> Casambi:
    """Construct Casambi with the bridge's default cache-path semantics."""
    # casambi-bt 0.3.2 misparses switch-event frames and reports every physical
    # control as button 4. Correct that one parser here, at the single point
    # both the bridge and the probe build a connection, before any packet can
    # arrive. Every other casambi-bt path is left untouched.
    install_switch_event_decoder()
    return Casambi()


async def run_switch_event_probe(*, sleep: Any = asyncio.sleep) -> None:
    """Connect with bridge settings and listen read-only for a bounded window."""
    device = configured_network_device(await discover())
    if device is None:
        raise RuntimeError

    casa = create_casambi_connection()
    registered = False
    try:
        await casa.connect(device, NETWORK_PASSWORD)
        casa.registerSwitchEventHandler(emit_switch_event)
        registered = True
        await sleep(SWITCH_PROBE_SECONDS)
    finally:
        try:
            if registered:
                casa.unregisterSwitchEventHandler(emit_switch_event)
        finally:
            await casa.disconnect()


async def log_exceptions(awaitable: Awaitable[Any]) -> Any:
    try:
        return await awaitable
    except aiomqtt.MqttError as e:
        LOGGER.warning("Unhandled exception: %s", e)


def to_unit_control_type(t: CasambiBt.UnitControlType) -> UnitControlType:
    return UnitControlType(t.name, t.value)


def to_unit_control(c: CasambiBt.UnitControl) -> UnitControl:
    return UnitControl(
        c.default, c.length, c.offset, c.readonly, to_unit_control_type(c.type)
    )


def to_unit_type(t: CasambiBt.UnitType) -> UnitType:
    return UnitType(
        t.id,
        t.manufacturer,
        t.mode,
        t.model,
        t.stateLength,
        [to_unit_control(c) for c in t.controls],
    )


def to_unit_state(s: CasambiBt.UnitState | None) -> UnitState:
    """
    Map the optional Casambi state without crashing the bridge.

    Some non-dimmable / partially discovered units report no state. They are
    still published for discovery, but a missing dimmer must not take down the
    entire MQTT bridge.
    """
    if s is None or not hasattr(s, "dimmer"):
        return UnitState(None)
    return UnitState(s.dimmer)


def to_entity(unit: CasambiBt.Unit) -> Unit:
    return Unit(
        unit.address,
        unit.deviceId,
        unit.is_on,
        unit.name,
        unit.online,
        to_unit_state(unit.state),
        unit.uuid,
        to_unit_type(unit.unitType),
    )


def to_scene(scene: CasambiBt.Scene) -> Scene:
    return Scene(scene.sceneId, scene.name)


def unit_event_topic(unit: CasambiBt.Unit) -> str | None:
    """Return a collision-free state topic without changing control address."""
    if unit.address:
        return f"{TOPIC_PREFIX}/{NETWORK_NAME}/events/{unit.address}"

    unit_uuid = getattr(unit, "uuid", None)
    if not isinstance(unit_uuid, str) or not unit_uuid:
        LOGGER.error("Skipping addressless Casambi unit without a usable UUID")
        return None
    return f"{TOPIC_PREFIX}/{NETWORK_NAME}/events/uuid/{quote(unit_uuid, safe='')}"


async def publish_unit(
    unit: CasambiBt.Unit, client: aiomqtt.Client, *, suppress_errors: bool = True
) -> bool:
    """
    Publish one retained unit state using the same topic for all paths.

    ``suppress_errors`` preserves publish_entities' existing behaviour
    byte-for-byte (swallow aiomqtt.MqttError via log_exceptions, still
    return True). UnitStatePublisher's worker needs the opposite: it calls
    this with suppress_errors=False so a broker-level failure actually
    reaches it and can be counted and retried instead of silently counted
    as a success.
    """
    topic = unit_event_topic(unit)
    if topic is None:
        return False
    entity = to_entity(unit)
    publish_call = client.publish(topic, payload=entity.to_json(), qos=1, retain=True)
    if suppress_errors:
        await log_exceptions(publish_call)
    else:
        await publish_call
    return True


class UnitStatePublisher:
    """
    Coalesce unit-state updates into one bounded worker.

    Overload policy: per-unit coalescing. A unit that updates faster than the
    broker acknowledges collapses to its newest state, keyed by that unit's
    publish topic -- newest write wins. No distinct unit is ever dropped, so
    memory is bounded by the number of units in the network, not by the event
    rate. This loses nothing observable: retained state means only the newest
    value is ever the correct one to have published anyway.

    Fairness: drained FIFO (oldest-submitted topic first). ``submit``
    coalescing assigns to an existing dict key in place, which does not
    change that key's iteration position -- so a unit re-submitted while
    still pending updates its value without jumping the queue ahead of a
    cold, already-pending, distinct unit. The one deliberate exception is
    the retry path below: a topic that just failed to publish is requeued
    at the BACK, not the front, so one broker hiccup cannot keep
    reappearing ahead of everything else either.

    Retry policy: a publish failure (aiomqtt.MqttError or otherwise) is
    counted truthfully as failed, never as confirmed, and the unit's state
    is requeued rather than lost -- but only if no newer value for that
    same topic arrived while the publish was in flight (checked via
    ``topic not in self._pending``); newest-wins must still hold, so a
    fresher value already waiting must never be overwritten by the stale
    one that just failed. After a requeue the worker stops draining for
    this cycle and sleeps PUBLISH_RETRY_SECONDS before resuming, so a dead
    broker produces at most one publish attempt per second instead of a
    spin loop. This is self-healing beyond that: a broken MQTT session also
    fails the command task, which tears the whole session down and
    republishes the full baseline on reconnect.


    Log volume: a contiguous run of failures (a "streak") logs exactly ONE
    warning, when it starts -- not one per retry. With PUBLISH_RETRY_SECONDS
    == 1.0, a half-open broker would otherwise produce one identical
    warning per second for as long as the session stays up-but-failing,
    which is dense enough to trip journald rate limiting and can silently
    drop OTHER records queued around it -- including the single BLE
    disconnect warning this whole class of hardening exists to keep
    visible. The first successful publish that ends a streak logs exactly
    one integer-only recovery summary. ``_failure_streak`` is plain
    per-instance state with no explicit reset on teardown: a session
    tear-down discards this UnitStatePublisher and a fresh one is
    constructed for the next session, so the streak count starts at zero
    again by construction, not by any special-cased cleanup code.
    """

    def __init__(
        self,
        client: aiomqtt.Client,
        diagnostics: BridgeDiagnostics | None = None,
        *,
        sleep: Any = asyncio.sleep,
    ) -> None:
        self.client = client
        self.diagnostics = diagnostics
        self.failed = 0
        self._sleep = sleep
        self._failure_streak = 0
        self._pending: dict[str, CasambiBt.Unit] = {}
        self._wakeup = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._worker_task: asyncio.Task[None] | None = None

    def __repr__(self) -> str:
        """Fixed-shape repr: class name plus integer counters only, no units."""
        return f"UnitStatePublisher(pending={len(self._pending)}, failed={self.failed})"

    def is_failing(self) -> bool:
        """Whether the newest publish attempt failed (a streak is open)."""
        return self._failure_streak > 0

    def submit(self, unit: CasambiBt.Unit) -> None:
        topic = unit_event_topic(unit)
        if topic is None:
            return
        if topic in self._pending and self.diagnostics is not None:
            self.diagnostics.unit_publish_coalesced += 1
        self._pending[topic] = unit
        self._idle.clear()
        if self.diagnostics is not None:
            self.diagnostics.unit_queue_depth = len(self._pending)
            self.diagnostics.unit_queue_peak = max(
                self.diagnostics.unit_queue_peak, len(self._pending)
            )
        self._wakeup.set()

    async def _worker(self) -> None:
        while True:
            await self._wakeup.wait()
            self._wakeup.clear()
            while self._pending:
                # FIFO: oldest-submitted topic first (see class docstring
                # for why -- LIFO here would let a hot, repeatedly
                # re-submitted unit starve a cold, already-pending one).
                topic = next(iter(self._pending))
                unit = self._pending.pop(topic)
                if self.diagnostics is not None:
                    self.diagnostics.unit_queue_depth = len(self._pending)
                    self.diagnostics.unit_publish_attempted += 1
                try:
                    await publish_unit(unit, self.client, suppress_errors=False)
                except Exception:  # noqa: BLE001  # Never let one publish kill the worker.
                    self.failed += 1
                    self._failure_streak += 1
                    if self.diagnostics is not None:
                        self.diagnostics.unit_publish_failed += 1
                    # One warning per streak, logged only when it starts --
                    # see the class docstring for why repeating this every
                    # retry is dangerous, not just noisy.
                    if self._failure_streak == 1:
                        LOGGER.warning(
                            "Unit state publish failed; retrying after a backoff"
                        )
                    # Requeue at the back -- the one deliberate exception to
                    # FIFO -- but only if nothing newer for this topic
                    # arrived while the failed publish was in flight;
                    # newest-wins must never be overwritten by a stale
                    # retry.
                    if topic not in self._pending:
                        self._pending[topic] = unit
                    if self.diagnostics is not None:
                        self.diagnostics.unit_queue_depth = len(self._pending)
                    await self._sleep(PUBLISH_RETRY_SECONDS)
                    # Stop draining this cycle rather than immediately
                    # trying the next pending topic: if the broker is dead,
                    # every remaining item would fail too, and without this
                    # break each one would serialize its own one-second
                    # backoff (N pending units -> N seconds). One failure
                    # is enough signal to back off once and re-evaluate.
                    # There is now known pending work (the requeue above,
                    # or whatever newer value pre-empted it) but nothing
                    # new necessarily called submit() to set this wakeup --
                    # without it the outer loop would hang waiting for an
                    # external update that may never come.
                    self._wakeup.set()
                    break
                else:
                    if self._failure_streak > 0:
                        LOGGER.info(
                            "Unit state publishing recovered after %d failed attempts",
                            self._failure_streak,
                        )
                        self._failure_streak = 0
                    if self.diagnostics is not None:
                        self.diagnostics.unit_publish_confirmed += 1
            if not self._pending:
                self._idle.set()

    async def start(self) -> None:
        self._worker_task = asyncio.create_task(self._worker())

    async def aclose(self, *, drain_timeout: float = DRAIN_TIMEOUT) -> None:
        task = self._worker_task
        if task is None:
            return
        self._worker_task = None
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._idle.wait(), timeout=drain_timeout)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def command_unit(casa: Casambi, address: str, unit_uuid: str | None) -> CasambiBt.Unit:
    """Resolve an unambiguous command target without synthetic addresses."""
    if unit_uuid is None:
        if not address:
            message = "Addressless Casambi commands require a unit UUID"
            raise ValueError(message)
        units = [unit for unit in casa.units if unit.address == address]
    else:
        units = [unit for unit in casa.units if unit.uuid == unit_uuid]
    if len(units) != 1:
        message = "Command target does not resolve to exactly one Casambi unit"
        raise ValueError(message)
    unit = units[0]
    if unit_uuid is not None and unit.address != address:
        message = "Command UUID does not match its Casambi address"
        raise ValueError(message)
    return unit


def validate_light_command(address: object, unit_uuid: object) -> None:
    if not isinstance(address, str):
        message = "Casambi command address must be a string"
        raise TypeError(message)
    if unit_uuid is not None and (not isinstance(unit_uuid, str) or not unit_uuid):
        message = "Casambi command UUID must be a non-empty string or null"
        raise TypeError(message)


def validate_integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        message = f"Casambi {name} must be an integer"
        raise TypeError(message)
    return value


def validate_brightness(value: object) -> int:
    value = validate_integer(value, "brightness value")
    if not 0 <= value <= MAX_BRIGHTNESS:
        message = "Casambi brightness value must be between 0 and 255"
        raise ValueError(message)
    return value


def addressless_unit_uuid_is_unique(casa: Casambi, unit: CasambiBt.Unit) -> bool:
    """Return whether an addressless unit has one usable UUID in this network."""
    if unit.address or not isinstance(getattr(unit, "uuid", None), str):
        return False
    return (
        sum(
            not candidate.address and candidate.uuid == unit.uuid
            for candidate in casa.units
        )
        == 1
    )


def validate_command_keys(command: dict[str, object], action: str) -> None:
    allowed_keys = {
        SetLevel.ACTION: {"action", "address", "value", "unit_uuid"},
        TurnOn.ACTION: {"action", "address", "unit_uuid"},
        PublishEntities.ACTION: {"action", "request_id"},
        SetScene.ACTION: {"action", "scene_id"},
        GET_STATUS_ACTION: {"action", "request_id"},
    }
    required_keys = {
        SetLevel.ACTION: {"action", "address", "value"},
        TurnOn.ACTION: {"action", "address"},
        PublishEntities.ACTION: {"action"},
        SetScene.ACTION: {"action", "scene_id"},
        GET_STATUS_ACTION: {"action", "request_id"},
    }
    if action not in allowed_keys:
        return
    if set(command) - allowed_keys[action] or not required_keys[action] <= set(command):
        message = "Casambi command has an invalid schema"
        raise ValueError(message)
    if "request_id" in command and not health_protocol.is_valid_request_id(
        command["request_id"]
    ):
        message = "Casambi health request ID is invalid"
        raise ValueError(message)


async def _publish_snapshot_message(
    client: aiomqtt.Client, topic: str, payload: str | bytes
) -> bool:
    """Publish one retained snapshot message; False only on a broker failure."""
    try:
        await client.publish(topic, payload=payload, qos=1, retain=True)
    except aiomqtt.MqttError as e:
        LOGGER.warning("Unhandled exception: %s", e)
        return False
    return True


async def publish_entities(casa: Casambi, client: aiomqtt.Client) -> tuple[int, int]:
    """
    Publish the current Casambi snapshot, one QoS message at a time.

    A reconnect/startup sync can contain dozens of units. Awaiting each publish
    deliberately avoids overflowing aiomqtt's pending-publish queue, which can
    otherwise leave Home Assistant with stale retained state.
    """
    result = await publish_snapshot(casa, client)
    return result.units, result.scenes


async def publish_snapshot(casa: Casambi, client: aiomqtt.Client) -> SnapshotResult:
    """
    Publish the snapshot and count only broker-confirmed messages as published.

    A failed publish is counted in ``failed`` and never as a unit or scene, so
    a partial failure can never be reported as a successful snapshot. The
    legacy ``events/`` tombstone is not a unit: it is never counted as one,
    but its failure still makes the snapshot unsuccessful.
    """
    units_published = 0
    scenes_published = 0
    failed = 0
    addressless_uuid_counts: dict[str, int] = {}
    for unit in casa.units:
        if not unit.address and isinstance(getattr(unit, "uuid", None), str):
            addressless_uuid_counts[unit.uuid] = (
                addressless_uuid_counts.get(unit.uuid, 0) + 1
            )

    # Clear the retained legacy collision before publishing UUID-keyed units.
    if not await _publish_snapshot_message(
        client, f"{TOPIC_PREFIX}/{NETWORK_NAME}/events/", b""
    ):
        failed += 1

    for unit in casa.units:
        if not unit.address:
            unit_uuid = getattr(unit, "uuid", None)
            if (
                not isinstance(unit_uuid, str)
                or addressless_uuid_counts.get(unit_uuid) != 1
            ):
                LOGGER.error("Skipping addressless Casambi unit with non-unique UUID")
                continue
        try:
            published = await publish_unit(unit, client, suppress_errors=False)
        except aiomqtt.MqttError as e:
            LOGGER.warning("Unhandled exception: %s", e)
            failed += 1
            continue
        if published:
            units_published += 1

    for scene in casa.scenes:
        scene_entity = to_scene(scene)
        if await _publish_snapshot_message(
            client,
            f"{TOPIC_PREFIX}/{NETWORK_NAME}/scenes/{scene_entity.scene_id}",
            scene_entity.to_json(),
        ):
            scenes_published += 1
        else:
            failed += 1

    LOGGER.info(
        "Published Casambi state snapshot: %d units, %d scenes",
        units_published,
        scenes_published,
    )
    if failed:
        LOGGER.warning("Casambi state snapshot had %d failed publishes", failed)
    return SnapshotResult(units_published, scenes_published, failed)


GET_STATUS_ACTION = health_protocol.GET_STATUS_ACTION
_KNOWN_COMMAND_ACTIONS = frozenset(
    {
        SetLevel.ACTION,
        TurnOn.ACTION,
        PublishEntities.ACTION,
        SetScene.ACTION,
        GET_STATUS_ACTION,
    }
)


def _safe_action_name(action: str) -> str:
    """
    Return the action name only if it is one of the known constants.

    ``action`` otherwise comes straight from the MQTT command payload, so it
    is attacker/user-controlled text -- never log it verbatim.
    """
    return action if action in _KNOWN_COMMAND_ACTIONS else "<unrecognized>"


async def _handle_snapshot_request(
    casa: Casambi,
    client: aiomqtt.Client,
    request_id: str | None,
    health: BridgeHealth | None,
    diagnostics: BridgeDiagnostics | None,
) -> None:
    """
    Run the legacy snapshot, or the correlated v1 snapshot probe.

    Without a ``request_id`` this is byte-for-byte the legacy
    ``PUBLISH_ENTITIES`` behaviour. With one, the reply is published only
    after every snapshot publish has been confirmed by the broker (aiomqtt
    awaits the QoS 1 PUBACK), and it reports ``failed`` unless all of them
    succeeded -- an acknowledgement is never sent for a partial snapshot.
    """
    if request_id is None or health is None:
        await publish_entities(casa, client)
        return
    if not health.snapshot_request_allowed():
        if diagnostics is not None:
            diagnostics.health_requests_rate_limited += 1
        if health.reply_allowed():
            await health.publish_reply(
                client, request_id, health_protocol.RESULT_RATE_LIMITED
            )
        return
    result = await publish_snapshot(casa, client)
    generation = health.record_snapshot(result)
    await health.publish_reply(
        client,
        request_id,
        health_protocol.RESULT_OK if result.ok else health_protocol.RESULT_FAILED,
        snapshot=result,
        generation=generation,
    )


async def _handle_status_request(
    client: aiomqtt.Client,
    request_id: str,
    health: BridgeHealth | None,
    diagnostics: BridgeDiagnostics | None,
) -> None:
    """Answer GET_STATUS from bridge state only: never touches a Casambi unit."""
    if health is None:
        LOGGER.warning("Ignoring health request: bridge health is not enabled")
        return
    if not health.reply_allowed():
        if diagnostics is not None:
            diagnostics.health_requests_rate_limited += 1
        return
    await health.publish_reply(client, request_id, health_protocol.RESULT_OK)


async def process_command(
    message: aiomqtt.Message,
    casa: Casambi,
    client: aiomqtt.Client,
    health: BridgeHealth | None = None,
    diagnostics: BridgeDiagnostics | None = None,
) -> None:
    expected_topic = f"{TOPIC_PREFIX}/{NETWORK_NAME}/commands"
    if hasattr(message, "topic") and str(message.topic) != expected_topic:
        LOGGER.warning("Ignoring command received on an unexpected MQTT topic")
        return
    try:
        payload = message.payload.decode()
        command = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        LOGGER.warning("Ignoring malformed MQTT command: %s", type(error).__name__)
        return

    if not isinstance(command, dict):
        LOGGER.warning("Ignoring MQTT command with a non-object JSON payload")
        return

    action = command.get("action")
    if not isinstance(action, str):
        LOGGER.warning("Ignoring MQTT command without an action")
        return

    try:
        validate_command_keys(command, action)
        match action:
            case SetLevel.ACTION:
                cmd = SetLevel.from_json(payload)
                validate_light_command(cmd.address, cmd.unit_uuid)
                value = validate_brightness(cmd.value)
                unit = command_unit(casa, cmd.address, cmd.unit_uuid)
                await casa.setLevel(unit, value)
            case TurnOn.ACTION:
                cmd = TurnOn.from_json(payload)
                validate_light_command(cmd.address, cmd.unit_uuid)
                unit = command_unit(casa, cmd.address, cmd.unit_uuid)
                await casa.turnOn(unit)
            case PublishEntities.ACTION:
                await _handle_snapshot_request(
                    casa, client, command.get("request_id"), health, diagnostics
                )
            case health_protocol.GET_STATUS_ACTION:
                await _handle_status_request(
                    client, command["request_id"], health, diagnostics
                )
            case SetScene.ACTION:
                cmd = SetScene.from_json(payload)
                scene_id = validate_integer(cmd.scene_id, "scene ID")
                scene = next(s for s in casa.scenes if s.sceneId == scene_id)
                await casa.switchToScene(scene)
            case _:
                # action is never one of the known constants here (the
                # cases above already matched them), so this is always
                # fixed text -- no interpolation of attacker-controlled input.
                LOGGER.warning("Ignoring unknown Casambi command action")
    except (
        AttributeError,
        KeyError,
        StopIteration,
        TypeError,
        ValueError,
        TimeoutError,
    ) as error:
        LOGGER.warning(
            "Unable to process Casambi command %s: %s",
            _safe_action_name(action),
            type(error).__name__,
        )


class CasambiDisconnected(RuntimeError):  # noqa: N818
    """
    Bridge-owned sentinel raised when the BLE link drops mid-session.

    Its message is a fixed safe string with no interpolation -- it must never
    be able to carry a network name, unit name, address or any other private
    detail, however it gets constructed or wrapped. Named without an -Error
    suffix to match the required public API (server.CasambiDisconnected).
    """

    def __init__(self) -> None:
        super().__init__("Casambi BLE connection lost; bridge will restart")


async def run_heartbeat(
    diagnostics: BridgeDiagnostics,
    *,
    sleep: Any = asyncio.sleep,
    monotonic: Any = time.monotonic,
) -> None:
    """
    Emit periodic health signals that never carry bridge-specific data.

    Loop lag is measured as actual elapsed time minus the requested sleep
    interval: under normal load this is ~0; a busy event loop (e.g. many
    queued publishes) makes it grow, which is the one external symptom worth
    a warning on its own. The diagnostics line is INFO because it is expected
    routine output, not an anomaly.
    """
    last_tick = monotonic()
    last_report = last_tick
    while True:
        await sleep(HEARTBEAT_SECONDS)
        now = monotonic()
        lag_seconds = now - last_tick - HEARTBEAT_SECONDS
        last_tick = now
        if lag_seconds > LOOP_LAG_WARN_SECONDS:
            lag_ms = int(lag_seconds * 1000)
            diagnostics.loop_lag_ms_max = max(diagnostics.loop_lag_ms_max, lag_ms)
            LOGGER.warning("Event loop lag detected: %d ms", lag_ms)
        if now - last_report >= DIAGNOSTICS_INTERVAL_SECONDS:
            last_report = now
            LOGGER.info("Bridge diagnostics: %s", diagnostics.as_log_fields())


async def run_status_heartbeat(
    health: BridgeHealth,
    client: aiomqtt.Client,
    diagnostics: BridgeDiagnostics | None = None,
    *,
    interval: float = DEFAULT_HEALTH_HEARTBEAT_SECONDS,
    sleep: Any = asyncio.sleep,
) -> None:
    """
    Publish the retained v1 status once per interval.

    Runs as its own task so it never waits for a snapshot or a command to
    finish; each publish is individually bounded by publish_status(). The
    caller publishes the first status itself, so this sleeps first.
    """
    while True:
        await sleep(interval)
        await health.publish_status(client, diagnostics)


async def _race_session_against_disconnect(
    session_task: asyncio.Task[None], disconnect_task: asyncio.Task[None]
) -> None:
    """
    Await whichever of the two finishes first and act accordingly.

    Precedence: the disconnect wins whenever it is part of the completed
    set, even if the session task ALSO completed in the same tick -- that is
    exactly what a BLE-induced MQTT teardown looks like (the broker
    connection dies at the same moment the BLE link does), and a BLE loss
    must never be masked by an MQTT-level symptom of that same loss. Any
    outcome the session task produced in that case (a clean return, an
    aiomqtt.MqttError, anything else) is discarded in favour of
    CasambiDisconnected.

    Session-task-wins cleanly (disconnect not set): propagate its
    result/exception exactly as if it had been awaited directly (an
    aiomqtt.MqttError still reaches main()'s reconnect loop unchanged).

    Either way the loser is cancelled and awaited so no task is leaked. If
    this coroutine itself is cancelled (normal shutdown), cancel both
    children, await them, and let CancelledError propagate untouched.
    """
    try:
        done, _pending = await asyncio.wait(
            {session_task, disconnect_task}, return_when=asyncio.FIRST_COMPLETED
        )
    except asyncio.CancelledError:
        session_task.cancel()
        disconnect_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await session_task
        with contextlib.suppress(asyncio.CancelledError):
            await disconnect_task
        raise

    if disconnect_task in done:
        session_task.cancel()
        with contextlib.suppress(BaseException):
            await session_task
        LOGGER.warning("Casambi BLE connection lost; bridge will restart")
        raise CasambiDisconnected

    disconnect_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await disconnect_task
    session_task.result()


@dataclass
class _ConnectedBridgeHandles:
    """
    Everything run_connected_bridge's finally needs to unwind cleanly.

    Does NOT include the BLE disconnect callback: that is registered once,
    for the whole process lifetime, by main() -- run_connected_bridge only
    receives the already-registered event to race against.
    """

    unit_publisher: UnitStatePublisher
    switch_publisher: SwitchEventPublisher
    unit_callback: Any
    heartbeat_task: asyncio.Task[None] | None = None
    status_task: asyncio.Task[None] | None = None
    unit_registered: bool = False
    switch_registered: bool = False


async def _cleanup_connected_bridge(
    casa: Casambi, handles: _ConnectedBridgeHandles
) -> None:
    """Reverse registration/startup order: newest-registered unwinds first."""
    for task in (handles.heartbeat_task, handles.status_task):
        if task is None:
            continue
        task.cancel()
        # Suppress any exception, not just CancelledError: a heartbeat
        # failure must never mask the original failure propagating out of
        # this cleanup (which can be a CasambiDisconnected).
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await task
    try:
        if handles.switch_registered:
            casa.unregisterSwitchEventHandler(handles.switch_publisher)
    finally:
        try:
            if handles.unit_registered:
                casa.unregisterUnitChangedHandler(handles.unit_callback)
        finally:
            try:
                await handles.switch_publisher.aclose()
            finally:
                await handles.unit_publisher.aclose()


async def _publish_final_status(
    health: BridgeHealth,
    client: aiomqtt.Client,
    diagnostics: BridgeDiagnostics,
    final_status: tuple[str, str, str],
) -> None:
    """Best-effort, bounded last status before a session ends on purpose."""
    phase, error, ble = final_status
    health.set_phase(phase, error)
    health.ble = ble
    with contextlib.suppress(Exception):
        await health.publish_status(
            client, diagnostics, publish_timeout=FINAL_STATUS_TIMEOUT
        )


async def run_connected_bridge(  # noqa: PLR0915
    casa: Casambi,
    client: aiomqtt.Client,
    disconnected: asyncio.Event,
    health: BridgeHealth | None = None,
    *,
    heartbeat_seconds: float = DEFAULT_HEALTH_HEARTBEAT_SECONDS,
) -> None:
    """
    Run one connected MQTT session with paired callback cleanup.

    ``disconnected`` is owned and registered by main(), not here: this
    function only races the ENTIRE session body -- subscribe, the baseline
    snapshot, and the command loop -- against it, so a BLE drop at any point
    during the session (including mid-snapshot) aborts that work and
    surfaces as CasambiDisconnected, rather than hanging or completing as if
    nothing happened.

    With ``health`` (always passed by main()), the session also runs the v1
    status heartbeat from before the baseline snapshot, answers GET_STATUS
    and correlated snapshot probes, and publishes a bounded final status on
    BLE loss or shutdown. Without it the session is the pre-v1 bridge.
    """
    diagnostics = BridgeDiagnostics()
    unit_publisher = UnitStatePublisher(client, diagnostics=diagnostics)
    switch_publisher = SwitchEventPublisher(client, diagnostics=diagnostics)

    def unit_callback(unit: CasambiBt.Unit) -> None:
        diagnostics.unit_callbacks += 1
        if not unit.address and not addressless_unit_uuid_is_unique(casa, unit):
            LOGGER.error("Skipping live addressless unit update with non-unique UUID")
            return
        unit_publisher.submit(unit)

    async def consume_commands() -> None:
        async for message in client.messages:
            # Bridge-owned: integers only. The decoded command topic and
            # payload carry the network name, unit address/UUID, scene id
            # and brightness value -- never log them, at any level.
            diagnostics.commands_received += 1
            await process_command(message, casa, client, health, diagnostics)

    handles = _ConnectedBridgeHandles(
        unit_publisher=unit_publisher,
        switch_publisher=switch_publisher,
        unit_callback=unit_callback,
    )

    async def session_body() -> None:
        await client.subscribe(f"{TOPIC_PREFIX}/{NETWORK_NAME}/commands")

        if health is not None:
            health.session_started()
            health.state_publish_failing = unit_publisher.is_failing
            await health.publish_status(client, diagnostics)
            handles.status_task = asyncio.create_task(
                run_status_heartbeat(
                    health, client, diagnostics, interval=heartbeat_seconds
                )
            )

        # Publish the baseline before registering live updates. Casambi can emit
        # an initial UnitChanged burst during registration; handling it in
        # parallel would recreate the MQTT queue exhaustion this snapshot avoids.
        if health is None:
            await publish_entities(casa, client)
        else:
            health.record_snapshot(await publish_snapshot(casa, client))
            # Phase change: report it now rather than at the next heartbeat.
            await health.publish_status(client, diagnostics)
        await unit_publisher.start()
        await switch_publisher.start()

        casa.registerUnitChangedHandler(unit_callback)
        handles.unit_registered = True
        casa.registerSwitchEventHandler(switch_publisher)
        handles.switch_registered = True

        handles.heartbeat_task = asyncio.create_task(run_heartbeat(diagnostics))

        LOGGER.info("Subscribed to commands and registered Casambi event handlers")

        await consume_commands()

    final_status: tuple[str, str, str] | None = None
    try:
        session_task = asyncio.create_task(session_body())
        disconnect_task = asyncio.create_task(disconnected.wait())
        try:
            await _race_session_against_disconnect(session_task, disconnect_task)
        except CasambiDisconnected:
            diagnostics.ble_disconnects += 1
            final_status = (
                health_protocol.PHASE_DEGRADED,
                health_protocol.ERROR_BLE_DISCONNECTED,
                health_protocol.LINK_DISCONNECTED,
            )
            raise
        except asyncio.CancelledError:
            final_status = (
                health_protocol.PHASE_OFFLINE,
                health_protocol.ERROR_SHUTDOWN,
                health_protocol.LINK_CONNECTED,
            )
            raise
    finally:
        await _cleanup_connected_bridge(casa, handles)
        # After cleanup: the heartbeat task is gone, so nothing can publish an
        # older ``ready`` over this final status.
        if health is not None and final_status is not None:
            await _publish_final_status(health, client, diagnostics, final_status)


async def _run_bridge_until_stopped() -> None:
    """
    Run main() with SIGTERM mapped to a clean, cancelling shutdown.

    Python's default SIGTERM action kills the process without unwinding, so
    systemd's stop would skip the BLE disconnect and the final ``offline``
    status. With this handler a stop cancels main(), runs its normal cleanup
    and exits 0 -- whichever phase (discovery, connect, session) it was in.
    """
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    stop_requested = False

    def on_sigterm() -> None:
        nonlocal stop_requested
        stop_requested = True
        if task is not None:
            task.cancel()

    installed = False
    with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
        loop.add_signal_handler(signal.SIGTERM, on_sigterm)
        installed = True
    try:
        await main()
    except asyncio.CancelledError:
        if not stop_requested:
            raise
        LOGGER.info("Stopped by SIGTERM")
    finally:
        if installed:
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.remove_signal_handler(signal.SIGTERM)


async def main(  # noqa: PLR0915
    *,
    sleep: Any = asyncio.sleep,
    health: BridgeHealth | None = None,
) -> None:
    if not is_valid_network_name(NETWORK_NAME):
        message = "CASAMBI_NETWORK_NAME must be one literal, non-empty MQTT topic level"
        raise RuntimeError(message)
    heartbeat_seconds = health_heartbeat_seconds()
    health = health or BridgeHealth()
    health.set_phase(health_protocol.PHASE_DISCOVERING)
    devices = await discover()
    device: BLEDevice | None = configured_network_device(devices)

    if device is None:
        LOGGER.info(
            "No casambi network specified, "
            "store the address of your network in CASAMBI_NETWORK_ADDRESS:"
        )
        for i, d in enumerate(devices):
            LOGGER.info("[%d]\t%s", i, d.address)
        sys.exit(0)

    casa = create_casambi_connection()
    disconnected = asyncio.Event()
    disconnect_registered = False
    health.set_phase(health_protocol.PHASE_CONNECTING)
    try:
        await casa.connect(device, NETWORK_PASSWORD)
        LOGGER.info("Connected to Casambi network")

        loop = asyncio.get_running_loop()

        def on_ble_disconnect() -> None:
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(disconnected.set)

        # One registration for the whole process lifetime, owned here --
        # NOT inside run_connected_bridge -- so a BLE drop in the window
        # between connecting and the first MQTT session (or during any
        # later reconnect gap) is never silently missed.
        casa.registerDisconnectCallback(on_ble_disconnect)
        disconnect_registered = True
        # Yield once so a disconnect scheduled via call_soon_threadsafe in
        # the narrow window right after registration is observed by the
        # check below instead of being acted on one loop iteration late.
        await sleep(0)
        if disconnected.is_set():
            raise CasambiDisconnected

        client = aiomqtt.Client(
            MQTT_BROKER,
            port=MQTT_PORT,
            username=MQTT_USERNAME,
            password=MQTT_PASSWORD,
            will=aiomqtt.Will(
                health_protocol.status_topic(TOPIC_PREFIX, NETWORK_NAME),
                payload=health.will_payload(),
                qos=1,
                retain=True,
            ),
        )
        interval = 5

        while True:
            if disconnected.is_set():
                raise CasambiDisconnected
            try:
                async with client:
                    await run_connected_bridge(
                        casa,
                        client,
                        disconnected,
                        health,
                        heartbeat_seconds=heartbeat_seconds,
                    )
            except aiomqtt.MqttError as e:
                # A BLE loss must never be retried as an MQTT reconnect: an
                # MqttError arriving here can be the MQTT-level symptom of
                # the very same BLE drop (broker TCP session torn down at
                # the same moment), so the disconnect always takes
                # precedence over treating this as an ordinary connectivity
                # hiccup.
                if disconnected.is_set():
                    raise CasambiDisconnected from None
                health.mqtt = health_protocol.LINK_DISCONNECTED
                health.set_phase(
                    health_protocol.PHASE_DEGRADED, health_protocol.ERROR_MQTT
                )
                LOGGER.warning(
                    "Connection lost (%s); Reconnecting in %d seconds ...", e, interval
                )
                await sleep(interval)
            except asyncio.CancelledError:
                LOGGER.info("Main task cancelled, waiting for cleanup")
                break

    finally:
        if disconnect_registered:
            casa.unregisterDisconnectCallback(on_ble_disconnect)
        LOGGER.info("Shutting down..")
        await casa.disconnect()


def cli(argv: list[str] | None = None) -> int:
    """Run the unchanged bridge, or the explicit sanitized switch probe."""
    args = sys.argv[1:] if argv is None else argv
    if args != ["switch-event-probe"]:
        configure_logging()
        try:
            asyncio.run(_run_bridge_until_stopped())
        except CasambiDisconnected:
            return EXIT_BLE_DISCONNECTED
        return 0

    logging.disable(logging.CRITICAL)
    try:
        asyncio.run(run_switch_event_probe())
    except KeyboardInterrupt:
        return 130
    except Exception:  # noqa: BLE001  # Errors can contain private network details.
        sys.stderr.write("casambi switch probe: failed\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
