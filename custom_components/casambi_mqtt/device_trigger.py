"""Device automation triggers for discovered Casambi switches."""

from __future__ import annotations

from typing import TYPE_CHECKING

import voluptuous as vol
from homeassistant.components.device_automation import (
    DEVICE_TRIGGER_BASE_SCHEMA,
    InvalidDeviceAutomationConfig,
)
from homeassistant.const import CONF_DEVICE_ID, CONF_DOMAIN, CONF_PLATFORM, CONF_TYPE
from homeassistant.core import HassJob, callback
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import device_registry as dr

from .const import DOMAIN
from .runtime_data import CasambiMqttRuntimeData
from .switch_events import MAX_SWITCH_VALUE, SUPPORTED_SWITCH_EVENTS

if TYPE_CHECKING:
    from typing import Any

    from homeassistant.core import CALLBACK_TYPE, HomeAssistant
    from homeassistant.helpers.trigger import TriggerActionType, TriggerInfo
    from homeassistant.helpers.typing import ConfigType

CONF_SUBTYPE = "subtype"
EVENT_TYPES = tuple(event.lower() for event in SUPPORTED_SWITCH_EVENTS)

TRIGGER_SCHEMA = DEVICE_TRIGGER_BASE_SCHEMA.extend(
    {
        vol.Required(CONF_TYPE): vol.In(EVENT_TYPES),
        vol.Required(CONF_SUBTYPE): vol.All(
            cv.positive_int, vol.Range(max=MAX_SWITCH_VALUE)
        ),
    }
)


def _runtime_for_device(
    hass: HomeAssistant, device_id: str
) -> tuple[CasambiMqttRuntimeData, int] | None:
    """Resolve a switch through its device-registry config entry and identifier."""
    device = dr.async_get(hass).async_get(device_id)
    if device is None:
        return None

    for entry_id in device.config_entries:
        entry = hass.config_entries.async_get_entry(entry_id)
        if entry is None or entry.domain != DOMAIN:
            continue
        runtime_data = getattr(entry, "runtime_data", None)
        if not isinstance(runtime_data, CasambiMqttRuntimeData):
            continue

        identifier_prefix = f"{entry.entry_id}:switch:"
        identifiers = [
            value
            for domain, value in device.identifiers
            if domain == DOMAIN and value.startswith(identifier_prefix)
        ]
        if len(identifiers) != 1:
            continue
        try:
            unit_id = int(identifiers[0][len(identifier_prefix) :])
        except ValueError:
            continue
        if not 0 <= unit_id <= MAX_SWITCH_VALUE:
            continue

        switch_unit = runtime_data.switch_units.get(unit_id)
        if switch_unit is not None and switch_unit.device_id != device_id:
            continue
        if switch_unit is None:
            runtime_data.add_switch_unit(unit_id, device_id)
        return runtime_data, unit_id
    return None


def _invalid_device(device_id: str) -> InvalidDeviceAutomationConfig:
    """Build a privacy-safe invalid-trigger error."""
    return InvalidDeviceAutomationConfig(
        f"Casambi MQTT switch trigger is not valid for device_id '{device_id}'"
    )


async def async_get_triggers(
    hass: HomeAssistant, device_id: str
) -> list[dict[str, Any]]:
    """Return supported triggers for each observed button."""
    found = _runtime_for_device(hass, device_id)
    if found is None:
        return []
    runtime_data, unit_id = found
    return [
        {
            CONF_PLATFORM: "device",
            CONF_DOMAIN: DOMAIN,
            CONF_DEVICE_ID: device_id,
            CONF_TYPE: event_type,
            CONF_SUBTYPE: button,
        }
        for button in sorted(runtime_data.switch_units[unit_id].buttons)
        for event_type in EVENT_TYPES
    ]


async def async_validate_trigger_config(
    hass: HomeAssistant, config: ConfigType
) -> ConfigType:
    """Validate a Casambi switch device trigger."""
    config = TRIGGER_SCHEMA(config)
    if _runtime_for_device(hass, config[CONF_DEVICE_ID]) is None:
        raise _invalid_device(config[CONF_DEVICE_ID])
    return config


async def async_attach_trigger(
    hass: HomeAssistant,
    config: ConfigType,
    action: TriggerActionType,
    trigger_info: TriggerInfo,
) -> CALLBACK_TYPE:
    """Attach a private callback with standard HA device-trigger variables."""
    config = TRIGGER_SCHEMA(config)
    found = _runtime_for_device(hass, config[CONF_DEVICE_ID])
    if found is None:
        raise _invalid_device(config[CONF_DEVICE_ID])
    runtime_data, unit_id = found
    key = (unit_id, config[CONF_SUBTYPE], config[CONF_TYPE].upper())
    job = HassJob(action)
    variables = {
        "trigger": {
            **trigger_info.get("trigger_data", {}),
            **config,
            "description": (
                f"Casambi switch button {config[CONF_SUBTYPE]} {config[CONF_TYPE]}"
            ),
        }
    }

    @callback
    def run_action() -> None:
        hass.async_run_hass_job(job, variables)

    listeners = runtime_data.switch_listeners.setdefault(key, [])
    listeners.append(run_action)

    @callback
    def remove_listener() -> None:
        listeners.remove(run_action)
        if not listeners:
            runtime_data.switch_listeners.pop(key, None)

    return remove_listener
