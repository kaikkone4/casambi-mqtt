from __future__ import annotations

from dataclasses import dataclass, field
from time import monotonic
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from homeassistant.helpers.entity_platform import AddEntitiesCallback
    from homeassistant.helpers.storage import Store

    from .light import CasambiMqttLight
    from .scene import CasambiMqttScene

SWITCH_EVENT_DEDUP_SECONDS = 0.25


@dataclass
class CasambiMqttRuntimeData:
    """Runtime state isolated to one config entry."""

    network_name: str
    light_add_entities: AddEntitiesCallback | None = None
    scene_add_entities: AddEntitiesCallback | None = None
    lights: dict[str, CasambiMqttLight] = field(default_factory=dict)
    scenes: dict[str, CasambiMqttScene] = field(default_factory=dict)
    switch_units: dict[int, SwitchUnit] = field(default_factory=dict)
    last_switch_events: dict[tuple[int, int, str], float] = field(default_factory=dict)
    switch_store: Store | None = None

    def is_duplicate_switch_event(self, event_key: tuple[int, int, str]) -> bool:
        """Collapse an identical QoS retry burst, but not distinct event phases."""
        now = monotonic()
        previous = self.last_switch_events.get(event_key)
        self.last_switch_events[event_key] = now
        return previous is not None and now - previous < SWITCH_EVENT_DEDUP_SECONDS

    def add_switch_unit(self, unit_id: int, device_id: str) -> SwitchUnit:
        """Track a newly discovered switch for device automations."""
        switch_unit = SwitchUnit(device_id=device_id)
        self.switch_units[unit_id] = switch_unit
        return switch_unit

    def stored_switch_buttons(self) -> dict[str, list[int]]:
        """Return the minimal durable switch discovery state."""
        return {
            str(unit_id): sorted(switch_unit.buttons)
            for unit_id, switch_unit in self.switch_units.items()
            if switch_unit.buttons
        }


@dataclass
class SwitchUnit:
    """A Casambi switch discovered from sanitized events."""

    device_id: str
    buttons: set[int] = field(default_factory=set)
