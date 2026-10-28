"""Privacy-limited config-entry diagnostics: health counters and states only."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry
) -> dict[str, Any]:
    """Return no network name, topic, unit, request ID or payload."""
    health = getattr(getattr(entry, "runtime_data", None), "health", None)
    return {"health": None if health is None else health.diagnostics()}
