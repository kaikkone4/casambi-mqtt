"""Read-only button events for discovered Casambi switches."""

# A Casambi switch is input only. An event entity is the only entity kind that
# represents it faithfully: it has no service calls and nothing to control, it
# just reports which button phase last arrived. Home Assistant's automation
# editor resolves a device's triggers through the entities it owns, so without
# these the switch offers the editor nothing to trigger on.

from __future__ import annotations

from typing import TYPE_CHECKING

from homeassistant.components.event import EventDeviceClass, EventEntity
from homeassistant.core import callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_connect

from .const import DOMAIN, entry_scoped_unique_id, switch_button_signal
from .switch_events import DEFAULT_SWITCH_BUTTONS, SUPPORTED_SWITCH_EVENTS

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback

    from .runtime_data import CasambiMqttRuntimeData

EVENT_TYPES = [event.lower() for event in SUPPORTED_SWITCH_EVENTS]


def switch_buttons(runtime_data: CasambiMqttRuntimeData, unit_id: int) -> list[int]:
    """Return the standard buttons plus any extra button already observed."""
    switch_unit = runtime_data.switch_units.get(unit_id)
    observed = switch_unit.buttons if switch_unit is not None else set()
    return sorted({*DEFAULT_SWITCH_BUTTONS, *observed})


@callback
def async_add_switch_buttons(entry: ConfigEntry, unit_id: int) -> None:
    """Create the button entities a switch is still missing."""
    runtime_data: CasambiMqttRuntimeData = entry.runtime_data
    async_add_entities = runtime_data.event_add_entities
    if async_add_entities is None:
        return
    missing = [
        button
        for button in switch_buttons(runtime_data, unit_id)
        if (unit_id, button) not in runtime_data.switch_button_events
    ]
    if not missing:
        return
    runtime_data.switch_button_events.update((unit_id, button) for button in missing)
    async_add_entities(
        CasambiMqttSwitchButton(entry.entry_id, unit_id, button) for button in missing
    )


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Restore the button entities of every switch already known."""
    runtime_data: CasambiMqttRuntimeData = entry.runtime_data
    runtime_data.event_add_entities = async_add_entities
    for unit_id in sorted(runtime_data.switch_units):
        async_add_switch_buttons(entry, unit_id)


class CasambiMqttSwitchButton(EventEntity):
    """One physical button of a Casambi switch."""

    _attr_should_poll = False
    _attr_has_entity_name = True
    _attr_device_class = EventDeviceClass.BUTTON
    _attr_event_types = EVENT_TYPES
    _attr_translation_key = "button"

    def __init__(self, entry_id: str, unit_id: int, button: int) -> None:
        self._entry_id = entry_id
        self._unit_id = unit_id
        self._button = button
        self._attr_unique_id = entry_scoped_unique_id(
            entry_id, f"casambi_mqtt_switch_{unit_id}_button_{button}"
        )
        self._attr_translation_placeholders = {"button": str(button)}
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, f"{entry_id}:switch:{unit_id}")}
        )

    async def async_added_to_hass(self) -> None:
        """Listen to this button's private signal only."""
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                switch_button_signal(self._entry_id, self._unit_id, self._button),
                self._async_handle_switch_event,
            )
        )

    @callback
    def _async_handle_switch_event(self, event_type: str) -> None:
        """Report a supported phase, ignoring anything else."""
        phase = event_type.lower()
        if phase not in EVENT_TYPES:
            return
        self._trigger_event(phase)
        self.async_write_ha_state()
