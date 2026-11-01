"""Diagnostic sensor showing one entry's bridge/consumer health state."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.const import EntityCategory

from .const import entry_scoped_unique_id
from .recovery import ConsumerState, HealthCoordinator

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    coordinator = entry.runtime_data.health_coordinator
    if coordinator is not None:
        async_add_entities([CasambiMqttHealthSensor(entry.entry_id, coordinator)])


class CasambiMqttHealthSensor(SensorEntity):
    """State, reason and last outcomes only: no network, topic or unit data."""

    _attr_device_class = SensorDeviceClass.ENUM
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_options = [state.value for state in ConsumerState]  # noqa: RUF012
    _attr_translation_key = "health"
    _attr_should_poll = False
    _attr_icon = "mdi:heart-pulse"

    def __init__(self, entry_id: str, coordinator: HealthCoordinator) -> None:
        self._coordinator = coordinator
        self._attr_name = "Casambi health"
        self._attr_unique_id = entry_scoped_unique_id(entry_id, "casambi_mqtt_health")

    async def async_added_to_hass(self) -> None:
        self._coordinator.listeners.append(self.async_write_ha_state)
        self.async_on_remove(
            lambda: self._coordinator.listeners.remove(self.async_write_ha_state)
        )

    @property
    def native_value(self) -> str:
        return self._coordinator.state.value

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        diagnostics = self._coordinator.diagnostics()
        return {
            "reason": diagnostics["reason"],
            "last_consumer_ok": diagnostics["last_consumer_ok"],
            "last_recovery": diagnostics["last_recovery"],
            "last_recovery_result": diagnostics["last_recovery_result"],
            "reloads_last_30_min": diagnostics["reloads_last_30_min"],
            "recovery_enabled": diagnostics["recovery_enabled"],
        }
