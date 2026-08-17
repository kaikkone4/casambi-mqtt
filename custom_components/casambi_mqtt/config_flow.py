from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.data_entry_flow import FlowResult

import voluptuous as vol
from homeassistant.core import callback

from homeassistant import config_entries

from .const import (
    CONF_NETWORK_NAME,
    DEFAULT_NETWORK_NAME,
    DOMAIN,
    configured_network_name,
    is_valid_network_name,
)


def configured_network_names(
    hass: HomeAssistant, exclude_entry_id: str | None = None
) -> set[str]:
    """Return every configured literal Casambi MQTT namespace."""
    return {
        configured_network_name(candidate.options, candidate.data)
        for candidate in hass.config_entries.async_entries(DOMAIN)
        if candidate.entry_id != exclude_entry_id
    }


class CasambiMqttConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Casambi MQTT config flow."""

    VERSION = 2

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            network_name = user_input[CONF_NETWORK_NAME]
            if not is_valid_network_name(network_name):
                errors[CONF_NETWORK_NAME] = "invalid_network_name"
            elif network_name in configured_network_names(self.hass):
                errors[CONF_NETWORK_NAME] = "network_name_in_use"
            else:
                await self.async_set_unique_id(network_name)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=network_name,
                    data={CONF_NETWORK_NAME: network_name},
                )

        data_schema = vol.Schema(
            {
                vol.Required(CONF_NETWORK_NAME, default=DEFAULT_NETWORK_NAME): str,
            }
        )

        return self.async_show_form(
            step_id="user", data_schema=data_schema, errors=errors
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Return the options flow handler for updating configuration."""
        return CasambiMqttOptionsFlow(entry)


class CasambiMqttOptionsFlow(config_entries.OptionsFlow):
    """Handle options for Casambi MQTT integration."""

    def __init__(self, entry: config_entries.ConfigEntry) -> None:
        self.entry = entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        errors: dict[str, str] = {}

        if user_input is not None:
            network_name = user_input[CONF_NETWORK_NAME]
            other_network_names = configured_network_names(
                self.hass, self.entry.entry_id
            )
            if not is_valid_network_name(network_name):
                errors[CONF_NETWORK_NAME] = "invalid_network_name"
            elif network_name in other_network_names:
                errors[CONF_NETWORK_NAME] = "network_name_in_use"
            else:
                self.hass.config_entries.async_update_entry(
                    self.entry, unique_id=network_name
                )
                return self.async_create_entry(
                    title=network_name,
                    data={CONF_NETWORK_NAME: network_name},
                )

        current_name: str = configured_network_name(self.entry.options, self.entry.data)

        data_schema = vol.Schema(
            {
                vol.Required(CONF_NETWORK_NAME, default=current_name): str,
            }
        )

        return self.async_show_form(
            step_id="init",
            data_schema=data_schema,
            errors=errors,
            description_placeholders={
                "restart_note": "Restart Home Assistant after changing this value"
            },
        )
