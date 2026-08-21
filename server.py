import asyncio
import contextlib
import json
import logging
import os
import re
import sys
import time
from collections.abc import Awaitable
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import aiomqtt
import aiomqtt.client
import CasambiBt
from CasambiBt import Casambi, discover
from dotenv import load_dotenv

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
REDACTED_MESSAGE_MAX_LENGTH = 160

load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
NETWORK_ADDRESS = os.getenv("CASAMBI_NETWORK_ADDRESS")
NETWORK_PASSWORD = os.getenv("CASAMBI_NETWORK_PASSWORD")
NETWORK_NAME = os.getenv("CASAMBI_NETWORK_NAME", "default")
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

    def as_log_fields(self) -> str:
        return " ".join(
            f"{field.name}={getattr(self, field.name)}" for field in fields(self)
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


async def publish_unit(unit: CasambiBt.Unit, client: aiomqtt.Client) -> bool:
    """Publish one retained unit state using the same topic for all paths."""
    topic = unit_event_topic(unit)
    if topic is None:
        return False
    entity = to_entity(unit)
    await log_exceptions(
        client.publish(topic, payload=entity.to_json(), qos=1, retain=True)
    )
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
    """

    def __init__(
        self, client: aiomqtt.Client, diagnostics: BridgeDiagnostics | None = None
    ) -> None:
        self.client = client
        self.diagnostics = diagnostics
        self.failed = 0
        self._pending: dict[str, CasambiBt.Unit] = {}
        self._wakeup = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._worker_task: asyncio.Task[None] | None = None

    def __repr__(self) -> str:
        """Fixed-shape repr: class name plus integer counters only, no units."""
        return f"UnitStatePublisher(pending={len(self._pending)}, failed={self.failed})"

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
                _topic, unit = self._pending.popitem()
                if self.diagnostics is not None:
                    self.diagnostics.unit_queue_depth = len(self._pending)
                    self.diagnostics.unit_publish_attempted += 1
                try:
                    await publish_unit(unit, self.client)
                except Exception:  # noqa: BLE001  # Never let one publish kill the worker.
                    self.failed += 1
                    if self.diagnostics is not None:
                        self.diagnostics.unit_publish_failed += 1
                    LOGGER.warning(
                        "Unit state publish failed; will retry on next update"
                    )
                else:
                    if self.diagnostics is not None:
                        self.diagnostics.unit_publish_confirmed += 1
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
        PublishEntities.ACTION: {"action"},
        SetScene.ACTION: {"action", "scene_id"},
    }
    required_keys = {
        SetLevel.ACTION: {"action", "address", "value"},
        TurnOn.ACTION: {"action", "address"},
        PublishEntities.ACTION: {"action"},
        SetScene.ACTION: {"action", "scene_id"},
    }
    if action not in allowed_keys:
        return
    if set(command) - allowed_keys[action] or not required_keys[action] <= set(command):
        message = "Casambi command has an invalid schema"
        raise ValueError(message)


async def publish_entities(casa: Casambi, client: aiomqtt.Client) -> tuple[int, int]:
    """
    Publish the current Casambi snapshot, one QoS message at a time.

    A reconnect/startup sync can contain dozens of units. Awaiting each publish
    deliberately avoids overflowing aiomqtt's pending-publish queue, which can
    otherwise leave Home Assistant with stale retained state.
    """
    units_published = 0
    scenes_published = 0
    addressless_uuid_counts: dict[str, int] = {}
    for unit in casa.units:
        if not unit.address and isinstance(getattr(unit, "uuid", None), str):
            addressless_uuid_counts[unit.uuid] = (
                addressless_uuid_counts.get(unit.uuid, 0) + 1
            )

    # Clear the retained legacy collision before publishing UUID-keyed units.
    await log_exceptions(
        client.publish(
            f"{TOPIC_PREFIX}/{NETWORK_NAME}/events/",
            payload=b"",
            qos=1,
            retain=True,
        )
    )

    for unit in casa.units:
        if not unit.address:
            unit_uuid = getattr(unit, "uuid", None)
            if (
                not isinstance(unit_uuid, str)
                or addressless_uuid_counts.get(unit_uuid) != 1
            ):
                LOGGER.error("Skipping addressless Casambi unit with non-unique UUID")
                continue
        if await publish_unit(unit, client):
            units_published += 1

    for scene in casa.scenes:
        scene_entity = to_scene(scene)
        await log_exceptions(
            client.publish(
                f"{TOPIC_PREFIX}/{NETWORK_NAME}/scenes/{scene_entity.scene_id}",
                payload=scene_entity.to_json(),
                qos=1,
                retain=True,
            )
        )
        scenes_published += 1

    LOGGER.info(
        "Published Casambi state snapshot: %d units, %d scenes",
        units_published,
        scenes_published,
    )
    return units_published, scenes_published


async def process_command(
    message: aiomqtt.Message, casa: Casambi, client: aiomqtt.Client
) -> None:
    expected_topic = f"{TOPIC_PREFIX}/{NETWORK_NAME}/commands"
    if hasattr(message, "topic") and str(message.topic) != expected_topic:
        LOGGER.warning("Ignoring command received on an unexpected MQTT topic")
        return
    try:
        payload = message.payload.decode()
        command = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        LOGGER.warning("Ignoring malformed MQTT command: %s", error)
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
                await publish_entities(casa, client)
            case SetScene.ACTION:
                cmd = SetScene.from_json(payload)
                scene_id = validate_integer(cmd.scene_id, "scene ID")
                scene = next(s for s in casa.scenes if s.sceneId == scene_id)
                await casa.switchToScene(scene)
            case _:
                LOGGER.warning("Ignoring unknown Casambi command action: %s", action)
    except (AttributeError, KeyError, StopIteration, TypeError, ValueError) as error:
        LOGGER.warning("Unable to process Casambi command %s: %s", action, error)


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


async def _race_commands_against_disconnect(
    command_task: asyncio.Task[None], disconnect_task: asyncio.Task[None]
) -> None:
    """
    Await whichever of the two finishes first and act accordingly.

    Command-task-wins: propagate its result/exception exactly as if it had
    been awaited directly (an aiomqtt.MqttError still reaches main()'s
    reconnect loop unchanged). Disconnect-wins: log the one fixed warning and
    raise CasambiDisconnected. Either way the loser is cancelled and awaited
    so no task is leaked. If this coroutine itself is cancelled (normal
    shutdown), cancel both children, await them, and let CancelledError
    propagate untouched.
    """
    try:
        done, _pending = await asyncio.wait(
            {command_task, disconnect_task}, return_when=asyncio.FIRST_COMPLETED
        )
    except asyncio.CancelledError:
        command_task.cancel()
        disconnect_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await command_task
        with contextlib.suppress(asyncio.CancelledError):
            await disconnect_task
        raise

    if command_task in done:
        disconnect_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await disconnect_task
        command_task.result()
        return

    command_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await command_task
    LOGGER.warning("Casambi BLE connection lost; bridge will restart")
    raise CasambiDisconnected


@dataclass
class _ConnectedBridgeHandles:
    """Everything run_connected_bridge's finally needs to unwind cleanly."""

    unit_publisher: UnitStatePublisher
    switch_publisher: SwitchEventPublisher
    unit_callback: Any
    on_ble_disconnect: Any
    heartbeat_task: asyncio.Task[None] | None = None
    unit_registered: bool = False
    switch_registered: bool = False
    disconnect_registered: bool = False


async def _cleanup_connected_bridge(
    casa: Casambi, handles: _ConnectedBridgeHandles
) -> None:
    """Reverse registration/startup order: newest-registered unwinds first."""
    if handles.heartbeat_task is not None:
        handles.heartbeat_task.cancel()
        # Suppress any exception, not just CancelledError: a heartbeat
        # failure must never mask the original failure propagating out of
        # this cleanup (which can be a CasambiDisconnected).
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await handles.heartbeat_task
    try:
        if handles.disconnect_registered:
            casa.unregisterDisconnectCallback(handles.on_ble_disconnect)
    finally:
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


async def run_connected_bridge(casa: Casambi, client: aiomqtt.Client) -> None:
    """Run one connected MQTT session with paired callback cleanup."""
    diagnostics = BridgeDiagnostics()
    loop = asyncio.get_running_loop()
    disconnected = asyncio.Event()
    unit_publisher = UnitStatePublisher(client, diagnostics=diagnostics)
    switch_publisher = SwitchEventPublisher(client, diagnostics=diagnostics)

    def on_ble_disconnect() -> None:
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(disconnected.set)

    def unit_callback(unit: CasambiBt.Unit) -> None:
        diagnostics.unit_callbacks += 1
        if not unit.address and not addressless_unit_uuid_is_unique(casa, unit):
            LOGGER.error("Skipping live addressless unit update with non-unique UUID")
            return
        unit_publisher.submit(unit)

    async def consume_commands() -> None:
        async for message in client.messages:
            LOGGER.debug(
                "Received command: %s on topic: '%s'",
                message.payload.decode(),
                message.topic,
            )
            await process_command(message, casa, client)

    handles = _ConnectedBridgeHandles(
        unit_publisher=unit_publisher,
        switch_publisher=switch_publisher,
        unit_callback=unit_callback,
        on_ble_disconnect=on_ble_disconnect,
    )
    try:
        await client.subscribe(f"{TOPIC_PREFIX}/{NETWORK_NAME}/commands")

        # Publish the baseline before registering live updates. Casambi can emit
        # an initial UnitChanged burst during registration; handling it in
        # parallel would recreate the MQTT queue exhaustion this snapshot avoids.
        await publish_entities(casa, client)
        await unit_publisher.start()
        await switch_publisher.start()

        casa.registerUnitChangedHandler(unit_callback)
        handles.unit_registered = True
        casa.registerSwitchEventHandler(switch_publisher)
        handles.switch_registered = True
        casa.registerDisconnectCallback(on_ble_disconnect)
        handles.disconnect_registered = True

        handles.heartbeat_task = asyncio.create_task(run_heartbeat(diagnostics))

        LOGGER.info("Subscribed to commands and registered Casambi event handlers")

        command_task = asyncio.create_task(consume_commands())
        disconnect_task = asyncio.create_task(disconnected.wait())
        try:
            await _race_commands_against_disconnect(command_task, disconnect_task)
        except CasambiDisconnected:
            diagnostics.ble_disconnects += 1
            raise
    finally:
        await _cleanup_connected_bridge(casa, handles)


async def main() -> None:
    if not is_valid_network_name(NETWORK_NAME):
        message = "CASAMBI_NETWORK_NAME must be one literal, non-empty MQTT topic level"
        raise RuntimeError(message)

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
    try:
        await casa.connect(device, NETWORK_PASSWORD)
        LOGGER.info("Connected to Casambi network")
        client = aiomqtt.Client(
            MQTT_BROKER, port=MQTT_PORT, username=MQTT_USERNAME, password=MQTT_PASSWORD
        )
        interval = 5

        while True:
            try:
                async with client:
                    await run_connected_bridge(casa, client)
            except aiomqtt.MqttError as e:
                LOGGER.warning(
                    "Connection lost (%s); Reconnecting in %d seconds ...", e, interval
                )
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                LOGGER.info("Main task cancelled, waiting for cleanup")
                break

    finally:
        LOGGER.info("Shutting down..")
        await casa.disconnect()


def cli(argv: list[str] | None = None) -> int:
    """Run the unchanged bridge, or the explicit sanitized switch probe."""
    args = sys.argv[1:] if argv is None else argv
    if args != ["switch-event-probe"]:
        configure_logging()
        try:
            asyncio.run(main())
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
