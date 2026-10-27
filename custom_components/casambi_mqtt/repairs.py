"""Operator acknowledgement for a stopped consumer recovery."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import voluptuous as vol
from homeassistant.components.repairs import RepairsFlow

from .recovery import get_recovery_manager

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.data_entry_flow import FlowResult


class RecoveryAcknowledgeFlow(RepairsFlow):
    """Confirming re-enables automatic reloads for this one entry."""

    def __init__(self, entry_id: str | None) -> None:
        self._entry_id = entry_id

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        return await self.async_step_confirm()

    async def async_step_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        if user_input is not None:
            manager = (
                None
                if self._entry_id is None
                else get_recovery_manager(self.hass, self._entry_id)
            )
            if manager is not None:
                await manager.async_acknowledge()
            return self.async_create_entry(data={})
        return self.async_show_form(step_id="confirm", data_schema=vol.Schema({}))


async def async_create_fix_flow(
    hass: HomeAssistant, issue_id: str, data: dict[str, Any] | None
) -> RepairsFlow:
    entry_id = (data or {}).get("entry_id")
    return RecoveryAcknowledgeFlow(entry_id if isinstance(entry_id, str) else None)
