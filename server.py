import asyncio
import json
import logging
import os
import sys
import time
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import aiomqtt
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

background_tasks = set()


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
    """Publish sanitized switch events while collapsing callback bursts."""

    def __init__(
        self, client: aiomqtt.Client, *, monotonic: Any = time.monotonic
    ) -> None:
        self.client = client
        self.monotonic = monotonic
        self.last_seen: dict[tuple[object, ...], float] = {}

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
        task = asyncio.create_task(self.publish(event))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)


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


async def run_connected_bridge(casa: Casambi, client: aiomqtt.Client) -> None:
    """Run one connected MQTT session with paired callback cleanup."""

    def unit_callback(unit: CasambiBt.Unit) -> None:
        if not unit.address and not addressless_unit_uuid_is_unique(casa, unit):
            LOGGER.error("Skipping live addressless unit update with non-unique UUID")
            return
        task = asyncio.create_task(publish_unit(unit, client))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)

    switch_callback = SwitchEventPublisher(client)
    unit_registered = False
    switch_registered = False
    try:
        await client.subscribe(f"{TOPIC_PREFIX}/{NETWORK_NAME}/commands")

        # Publish the baseline before registering live updates. Casambi can emit
        # an initial UnitChanged burst during registration; handling it in
        # parallel would recreate the MQTT queue exhaustion this snapshot avoids.
        await publish_entities(casa, client)
        casa.registerUnitChangedHandler(unit_callback)
        unit_registered = True
        casa.registerSwitchEventHandler(switch_callback)
        switch_registered = True

        LOGGER.info("Subscribed to commands and registered Casambi event handlers")
        async for message in client.messages:
            LOGGER.debug(
                "Received command: %s on topic: '%s'",
                message.payload.decode(),
                message.topic,
            )
            await process_command(message, casa, client)
    finally:
        try:
            if switch_registered:
                casa.unregisterSwitchEventHandler(switch_callback)
        finally:
            if unit_registered:
                casa.unregisterUnitChangedHandler(unit_callback)


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
        asyncio.run(main())
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
