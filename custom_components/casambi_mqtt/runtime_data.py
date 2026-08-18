from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from homeassistant.helpers.entity_platform import AddEntitiesCallback

    from .light import CasambiMqttLight
    from .scene import CasambiMqttScene


@dataclass
class CasambiMqttRuntimeData:
    """Runtime state isolated to one config entry."""

    network_name: str
    light_add_entities: AddEntitiesCallback | None = None
    scene_add_entities: AddEntitiesCallback | None = None
    lights: dict[str, CasambiMqttLight] = field(default_factory=dict)
    scenes: dict[str, CasambiMqttScene] = field(default_factory=dict)
