"""Private handling for the sanitized Casambi switch-event contract."""

from __future__ import annotations

import json

MAX_SWITCH_VALUE = 255
SUPPORTED_SWITCH_EVENTS = (
    "PRESS",
    "RELEASE",
    "HOLD",
    "RELEASE_AFTER_HOLD",
)


def decode_switch_event(payload: str | bytes) -> tuple[int, int, str] | None:
    """Decode the exact public bridge contract, failing closed."""
    try:
        value = json.loads(payload)
    except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
        return None
    if not isinstance(value, dict) or set(value) != {"unit_id", "button", "event"}:
        return None
    unit_id = value["unit_id"]
    button = value["button"]
    event_type = value["event"]
    if (
        not isinstance(unit_id, int)
        or isinstance(unit_id, bool)
        or not 0 <= unit_id <= MAX_SWITCH_VALUE
        or not isinstance(button, int)
        or isinstance(button, bool)
        or not 0 <= button <= MAX_SWITCH_VALUE
        or event_type not in SUPPORTED_SWITCH_EVENTS
    ):
        return None
    return unit_id, button, event_type
