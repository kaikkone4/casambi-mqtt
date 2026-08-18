from typing import Any

from homeassistant.components import mqtt
from homeassistant.components.light import ATTR_BRIGHTNESS, ColorMode, LightEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from custom_components.casambi_mqtt.entities.commands import SetLevel, TurnOn

from .const import MQTT_TOPIC_PREFIX, entry_scoped_unique_id
from .entities.entities import Unit


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    entry.runtime_data.light_add_entities = async_add_entities


class CasambiMqttLight(LightEntity):
    _attr_bt_address: str
    _mqtt_network_name: str

    def __init__(
        self,
        hass: HomeAssistant,
        topic: str,
        network_name: str,
        entry_id: str,
        unit: Unit,
    ) -> None:
        self.hass = hass
        self._mqtt_network_name = network_name
        self._attr_name = unit.name
        legacy_unique_id = (
            f"casambi_mqtt_light_{unit.address}"
            if unit.address
            else f"casambi_mqtt_light_uuid_{unit.uuid}"
        )
        self._attr_unique_id = entry_scoped_unique_id(entry_id, legacy_unique_id)
        dimmer = unit.state.dimmer or 0
        self._attr_is_on = dimmer > 0
        self._attr_brightness = dimmer
        self._attr_color_mode = ColorMode.BRIGHTNESS
        self._attr_supported_color_modes = {ColorMode.BRIGHTNESS}
        self._topic = topic
        self._attr_bt_address = unit.address
        self._unit_uuid = unit.uuid

    def update_entity(self, unit: Unit) -> None:
        dimmer = unit.state.dimmer or 0
        self._unit_uuid = unit.uuid
        self._attr_is_on = dimmer > 0
        self._attr_brightness = dimmer
        self.async_write_ha_state()

    async def async_turn_on(self, **kwargs: Any) -> None:
        if ATTR_BRIGHTNESS in kwargs:
            command = SetLevel(
                self._attr_bt_address, kwargs[ATTR_BRIGHTNESS], self._unit_uuid
            )
        else:
            command = TurnOn(self._attr_bt_address, self._unit_uuid)
        await mqtt.async_publish(
            self.hass,
            f"{MQTT_TOPIC_PREFIX}/{self._mqtt_network_name}/commands",
            command.to_json(),
        )

    async def async_turn_off(self, **kwargs: Any) -> None:
        command = SetLevel(self._attr_bt_address, 0, self._unit_uuid)
        await mqtt.async_publish(
            self.hass,
            f"{MQTT_TOPIC_PREFIX}/{self._mqtt_network_name}/commands",
            command.to_json(),
        )
