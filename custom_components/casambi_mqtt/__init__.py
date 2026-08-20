from functools import partial
from urllib.parse import quote

from homeassistant.components.mqtt import ReceiveMessage, async_subscribe
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.typing import ConfigType

from .const import (
    DOMAIN,
    LOGGER,
    MQTT_TOPIC_PREFIX,
    configured_network_name,
    entry_scoped_unique_id,
)
from .entities.entities import Scene, Unit
from .light import CasambiMqttLight
from .runtime_data import CasambiMqttRuntimeData
from .scene import CasambiMqttScene
from .switch_events import decode_switch_event

PLATFORMS: list[Platform] = [Platform.LIGHT, Platform.BUTTON, Platform.SCENE]
LEGACY_ADDRESSLESS_LIGHT_UNIQUE_ID = "casambi_mqtt_light_"
CONFIG_ENTRY_VERSION = 2


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """YAML setup (unused)."""
    return True


def _remove_legacy_addressless_light(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Remove only the former unowned empty-address light for this entry."""
    registry = er.async_get(hass)
    entity_id = registry.async_get_entity_id(
        Platform.LIGHT, DOMAIN, LEGACY_ADDRESSLESS_LIGHT_UNIQUE_ID
    )
    if entity_id is None:
        return
    registry_entry = registry.async_get(entity_id)
    if registry_entry is not None and registry_entry.config_entry_id == entry.entry_id:
        registry.async_remove(entity_id)


def _migrated_unique_id(entry_id: str, unique_id: str) -> str | None:
    if unique_id == LEGACY_ADDRESSLESS_LIGHT_UNIQUE_ID:
        return None
    if unique_id.startswith(
        ("casambi_mqtt_light_", "casambi_mqtt_scene_", "casambi_mqtt_reload_entities")
    ):
        return entry_scoped_unique_id(entry_id, unique_id)
    return ""


async def async_migrate_entry(  # noqa: PLR0911
    hass: HomeAssistant, entry: ConfigEntry
) -> bool:
    """Scope v1 entity identities by entry without changing entity IDs."""
    if entry.version == CONFIG_ENTRY_VERSION:
        return True
    if entry.version != 1:
        LOGGER.error("Unsupported Casambi MQTT config-entry version %s", entry.version)
        return False

    registry = er.async_get(hass)
    network_name = configured_network_name(entry.options, entry.data)
    colliding_entry = next(
        (
            candidate
            for candidate in hass.config_entries.async_entries(DOMAIN)
            if candidate.entry_id != entry.entry_id
            and configured_network_name(candidate.options, candidate.data)
            == network_name
        ),
        None,
    )
    if colliding_entry is not None:
        LOGGER.error("Cannot migrate Casambi MQTT entry: network name is in use")
        return False
    updates: list[tuple[str, str]] = []
    removals: list[str] = []
    scoped_prefix = f"{entry.entry_id}_"
    for registry_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if registry_entry.platform != DOMAIN:
            continue
        if registry_entry.unique_id.startswith(scoped_prefix):
            continue
        target_unique_id = _migrated_unique_id(entry.entry_id, registry_entry.unique_id)
        if target_unique_id is None:
            removals.append(registry_entry.entity_id)
            continue
        if not target_unique_id:
            LOGGER.error(
                "Unexpected Casambi MQTT unique ID %s", registry_entry.unique_id
            )
            return False
        existing_entity_id = registry.async_get_entity_id(
            registry_entry.domain, DOMAIN, target_unique_id
        )
        if existing_entity_id not in (None, registry_entry.entity_id):
            LOGGER.error(
                "Cannot migrate Casambi MQTT entity due to unique-ID collision"
            )
            return False
        updates.append((registry_entry.entity_id, target_unique_id))

    try:
        for entity_id, target_unique_id in updates:
            registry.async_update_entity(entity_id, new_unique_id=target_unique_id)
        for entity_id in removals:
            registry.async_remove(entity_id)
    except ValueError:
        LOGGER.exception("Unable to migrate Casambi MQTT entity registry")
        return False
    hass.config_entries.async_update_entry(
        entry, version=CONFIG_ENTRY_VERSION, unique_id=network_name
    )
    return True


async def _async_process_switch_event(
    hass: HomeAssistant,
    entry: ConfigEntry,
    switch_event_topic: str,
    msg: ReceiveMessage,
) -> None:
    """Consume one sanitized event without exposing its MQTT envelope."""
    if msg.topic != switch_event_topic or getattr(msg, "retain", False):
        return
    switch_event = decode_switch_event(msg.payload)
    if switch_event is None:
        return
    unit_id, button, event_type = switch_event
    runtime_data: CasambiMqttRuntimeData = entry.runtime_data
    event_key = (unit_id, button, event_type)
    if runtime_data.is_duplicate_switch_event(event_key):
        return

    switch_unit = runtime_data.switch_units.get(unit_id)
    if switch_unit is None:
        device = dr.async_get(hass).async_get_or_create(
            config_entry_id=entry.entry_id,
            identifiers={(DOMAIN, f"{entry.entry_id}:switch:{unit_id}")},
            manufacturer="Casambi",
            model="Switch",
            name=f"Casambi switch {unit_id}",
        )
        switch_unit = runtime_data.add_switch_unit(unit_id, device.id)
    switch_unit.buttons.add(button)
    runtime_data.fire_switch_event(event_key)


async def async_setup_entry(  # noqa: PLR0915
    hass: HomeAssistant, entry: ConfigEntry
) -> bool:
    network_name = configured_network_name(entry.options, entry.data)
    entry.runtime_data = CasambiMqttRuntimeData(network_name=network_name)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    _remove_legacy_addressless_light(hass, entry)

    event_prefix = f"{MQTT_TOPIC_PREFIX}/{network_name}/events/"
    scene_prefix = f"{MQTT_TOPIC_PREFIX}/{network_name}/scenes/"
    switch_event_topic = f"{MQTT_TOPIC_PREFIX}/{network_name}/switch_events"

    async def event_processor(  # noqa: PLR0911, PLR0912
        msg: ReceiveMessage,
    ) -> None:
        if not msg.payload or not msg.topic.startswith(event_prefix):
            return
        topic_suffix = msg.topic[len(event_prefix) :]
        # `.../events/` was the old shared retained topic for every unit
        # without an address. Never recreate its untrustworthy entity.
        if not topic_suffix:
            return
        unit: Unit | None = None
        unit_kind: str | None = None
        try:
            unit = Unit.from_json(msg.payload)
            if unit is None:
                valid_unit = False
            else:
                unit_kind = unit.type()
                valid_unit = (
                    isinstance(unit.address, str)
                    and isinstance(unit.uuid, str)
                    and unit.state is not None
                    and isinstance(unit.state.dimmer, int | None)
                    and not isinstance(unit.state.dimmer, bool)
                )
        except (AttributeError, KeyError, TypeError, ValueError):
            valid_unit = False
        if not valid_unit or unit is None:
            LOGGER.warning("Invalid Casambi unit payload on topic %s", msg.topic)
            return
        if unit_kind != Unit.TYPE_LIGHT:
            return

        if unit.address:
            if topic_suffix != unit.address:
                LOGGER.warning(
                    "Ignoring Casambi unit payload with mismatched address topic"
                )
                return
        else:
            expected_suffix = f"uuid/{quote(unit.uuid, safe='')}"
            if not unit.uuid or topic_suffix != expected_suffix:
                LOGGER.warning(
                    "Ignoring addressless Casambi unit without matching UUID topic"
                )
                return

        runtime_data: CasambiMqttRuntimeData = entry.runtime_data
        async_add_entities = runtime_data.light_add_entities
        if async_add_entities is None:
            LOGGER.warning("Light platform not ready yet. Message ignored")
            return
        if msg.topic not in runtime_data.lights:
            light_entity = CasambiMqttLight(
                hass, msg.topic, network_name, entry.entry_id, unit
            )
            runtime_data.lights[msg.topic] = light_entity
            async_add_entities([light_entity])
        else:
            runtime_data.lights[msg.topic].update_entity(unit)

    async def scene_processor(msg: ReceiveMessage) -> None:
        if not msg.payload or not msg.topic.startswith(scene_prefix):
            return
        topic_suffix = msg.topic[len(scene_prefix) :]
        if not topic_suffix:
            return
        scene: Scene | None = None
        try:
            scene = Scene.from_json(msg.payload)
            valid_scene = (
                scene is not None
                and isinstance(scene.scene_id, int)
                and not isinstance(scene.scene_id, bool)
            )
        except (AttributeError, KeyError, TypeError, ValueError):
            valid_scene = False
        if not valid_scene or scene is None:
            LOGGER.warning("Invalid Casambi scene payload on topic %s", msg.topic)
            return
        if topic_suffix != str(scene.scene_id):
            LOGGER.warning("Ignoring Casambi scene payload with mismatched topic")
            return

        runtime_data: CasambiMqttRuntimeData = entry.runtime_data
        async_add_entities = runtime_data.scene_add_entities
        if async_add_entities is None:
            LOGGER.warning("Scene platform not ready yet. Message ignored")
            return
        if msg.topic not in runtime_data.scenes:
            scene_entity = CasambiMqttScene(hass, network_name, entry.entry_id, scene)
            runtime_data.scenes[msg.topic] = scene_entity
            async_add_entities([scene_entity])
        else:
            runtime_data.scenes[msg.topic].update_entity(scene)

    unsubscribers = []
    try:
        unsubscribers.append(
            await async_subscribe(hass, f"{event_prefix}#", event_processor, 1)
        )
        unsubscribers.append(
            await async_subscribe(hass, f"{scene_prefix}#", scene_processor, 1)
        )
        unsubscribers.append(
            await async_subscribe(
                hass,
                switch_event_topic,
                partial(_async_process_switch_event, hass, entry, switch_event_topic),
                1,
            )
        )
    except Exception:
        for unsubscribe in unsubscribers:
            unsubscribe()
        raise
    for unsubscribe in unsubscribers:
        entry.async_on_unload(unsubscribe)

    LOGGER.debug("Casambi MQTT subscriptions set up for %s", network_name)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload platforms and their entry-scoped MQTT subscriptions."""
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
