from collections.abc import Mapping
from logging import Logger, getLogger
from typing import Any

LOGGER: Logger = getLogger(__package__)

DOMAIN = "casambi_mqtt"
LIGHT_ADD_ENTITIES = "light_add_entities"
SCENE_ADD_ENTITIES = "scene_add_entities"

MQTT_TOPIC_PREFIX = "casambi"
CONF_NETWORK_NAME = "mqtt_network_name"
DEFAULT_NETWORK_NAME = "default"


def configured_network_name(options: Mapping[str, Any], data: Mapping[str, Any]) -> str:
    """Return the normalized MQTT namespace for a config entry."""
    return (
        options.get(CONF_NETWORK_NAME)
        or data.get(CONF_NETWORK_NAME)
        or DEFAULT_NETWORK_NAME
    )


def switch_event_signal(
    entry_id: str, unit_id: int, button: int, event_type: str
) -> str:
    """Name the in-process signal for one switch event phase."""
    # Dispatcher signals never reach the event bus, so device triggers stay
    # private while surviving a config-entry reload.
    return f"{DOMAIN}_{entry_id}_switch_{unit_id}_{button}_{event_type}"


def entry_scoped_unique_id(entry_id: str, legacy_unique_id: str) -> str:
    """Make entity identity stable across mutable MQTT network names."""
    return f"{entry_id}_{legacy_unique_id}"


def is_valid_network_name(value: object) -> bool:
    """Allow one literal MQTT topic level, never a wildcard or path."""
    return (
        isinstance(value, str)
        and bool(value)
        and not any(character in value for character in ("/", "+", "#", "\x00"))
    )
