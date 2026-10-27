"""
Versioned, non-actuating bridge health protocol shared by the bridge and HA.

Pure Python on purpose: the bridge imports this module without Home
Assistant, and Home Assistant imports it without the bridge. Every parser
fails closed -- a payload that does not match the exact v1 shape is ``None``,
never a partially trusted value. See docs/health-protocol.md.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

PROTOCOL_VERSION = 1

GET_STATUS_ACTION = "GET_STATUS"
PUBLISH_ENTITIES_ACTION = "PUBLISH_ENTITIES"

PHASE_STARTING = "starting"
PHASE_DISCOVERING = "discovering"
PHASE_CONNECTING = "connecting"
PHASE_READY = "ready"
PHASE_DEGRADED = "degraded"
PHASE_OFFLINE = "offline"
PHASES = frozenset(
    {
        PHASE_STARTING,
        PHASE_DISCOVERING,
        PHASE_CONNECTING,
        PHASE_READY,
        PHASE_DEGRADED,
        PHASE_OFFLINE,
    }
)

LINK_CONNECTED = "connected"
LINK_DISCONNECTED = "disconnected"
LINK_UNKNOWN = "unknown"
LINK_STATES = frozenset({LINK_CONNECTED, LINK_DISCONNECTED, LINK_UNKNOWN})

# Fixed failure categories. Never free text: these travel over MQTT and into
# Home Assistant diagnostics, so they must not be able to carry private data.
ERROR_NONE = "none"
ERROR_BLE_DISCONNECTED = "ble_disconnected"
ERROR_BLE_DISCOVERY_IN_PROGRESS = "ble_discovery_in_progress"
ERROR_BLE_DISCOVERY_FAILED = "ble_discovery_failed"
ERROR_BLE_CONNECT_FAILED = "ble_connect_failed"
ERROR_NETWORK_NOT_FOUND = "network_not_found"
ERROR_CONFIGURATION = "configuration"
ERROR_MQTT = "mqtt_error"
ERROR_SNAPSHOT_PUBLISH_FAILED = "snapshot_publish_failed"
ERROR_STATE_PUBLISH_FAILED = "state_publish_failed"
ERROR_CONNECTION_LOST = "connection_lost"
ERROR_SHUTDOWN = "shutdown"
ERROR_UNKNOWN = "unknown"
ERROR_CATEGORIES = frozenset(
    {
        ERROR_NONE,
        ERROR_BLE_DISCONNECTED,
        ERROR_BLE_DISCOVERY_IN_PROGRESS,
        ERROR_BLE_DISCOVERY_FAILED,
        ERROR_BLE_CONNECT_FAILED,
        ERROR_NETWORK_NOT_FOUND,
        ERROR_CONFIGURATION,
        ERROR_MQTT,
        ERROR_SNAPSHOT_PUBLISH_FAILED,
        ERROR_STATE_PUBLISH_FAILED,
        ERROR_CONNECTION_LOST,
        ERROR_SHUTDOWN,
        ERROR_UNKNOWN,
    }
)

REPLY_STATUS = "status"
REPLY_SNAPSHOT = "snapshot"
REPLY_TYPES = frozenset({REPLY_STATUS, REPLY_SNAPSHOT})

RESULT_OK = "ok"
RESULT_FAILED = "failed"
RESULT_RATE_LIMITED = "rate_limited"
RESULT_BUSY = "busy"
RESULTS = frozenset({RESULT_OK, RESULT_FAILED, RESULT_RATE_LIMITED, RESULT_BUSY})

_REQUEST_ID = re.compile(r"[A-Za-z0-9._:-]{1,64}")
_INSTANCE_ID = re.compile(r"[0-9a-f]{32}")
MAX_COUNTER = 2**53 - 1

STATUS_KEYS = frozenset(
    {
        "protocol_version",
        "instance_id",
        "sequence",
        "phase",
        "ble",
        "mqtt",
        "uptime_s",
        "snapshot",
        "error",
        "time",
    }
)
SNAPSHOT_KEYS = frozenset({"generation", "units", "scenes", "failed", "age_s"})
REPLY_KEYS = frozenset(
    {"protocol_version", "type", "request_id", "instance_id", "result", "status"}
)
SNAPSHOT_REPLY_KEYS = REPLY_KEYS | {"units", "scenes", "failed", "generation"}


def status_topic(prefix: str, network_name: str) -> str:
    """Retained bridge status, heartbeat and last-will topic."""
    return f"{prefix}/{network_name}/status"


def reply_topic(prefix: str, network_name: str) -> str:
    """Non-retained correlated replies to GET_STATUS and snapshot requests."""
    return f"{prefix}/{network_name}/health/reply"


def is_valid_request_id(value: object) -> bool:
    """Allow 1-64 characters from a small fixed ASCII set only."""
    return isinstance(value, str) and _REQUEST_ID.fullmatch(value) is not None


def is_valid_instance_id(value: object) -> bool:
    return isinstance(value, str) and _INSTANCE_ID.fullmatch(value) is not None


def _is_counter(value: object) -> bool:
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value <= MAX_COUNTER
    )


def _is_optional_counter(value: object) -> bool:
    return value is None or _is_counter(value)


def _load_object(payload: str | bytes | bytearray) -> dict[str, Any] | None:
    try:
        value = json.loads(payload)
    except (TypeError, ValueError, UnicodeDecodeError):
        return None
    return value if isinstance(value, dict) else None


@dataclass(frozen=True, slots=True)
class SnapshotSummary:
    """Counters of the last completed snapshot, never unit identities."""

    generation: int = 0
    units: int = 0
    scenes: int = 0
    failed: int = 0
    age_s: int | None = None

    def as_dict(self) -> dict[str, int | None]:
        return {
            "generation": self.generation,
            "units": self.units,
            "scenes": self.scenes,
            "failed": self.failed,
            "age_s": self.age_s,
        }


@dataclass(frozen=True, slots=True)
class BridgeStatus:
    instance_id: str
    sequence: int | None
    phase: str
    ble: str
    mqtt: str
    uptime_s: int | None
    snapshot: SnapshotSummary
    error: str
    time: str | None = None
    protocol_version: int = PROTOCOL_VERSION

    def as_dict(self) -> dict[str, Any]:
        return {
            "protocol_version": self.protocol_version,
            "instance_id": self.instance_id,
            "sequence": self.sequence,
            "phase": self.phase,
            "ble": self.ble,
            "mqtt": self.mqtt,
            "uptime_s": self.uptime_s,
            "snapshot": self.snapshot.as_dict(),
            "error": self.error,
            "time": self.time,
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class HealthReply:
    type: str
    request_id: str
    instance_id: str
    result: str
    status: BridgeStatus
    units: int | None = None
    scenes: int | None = None
    failed: int | None = None
    generation: int | None = None

    def to_json(self) -> str:
        data: dict[str, Any] = {
            "protocol_version": PROTOCOL_VERSION,
            "type": self.type,
            "request_id": self.request_id,
            "instance_id": self.instance_id,
            "result": self.result,
            "status": self.status.as_dict(),
        }
        if self.type == REPLY_SNAPSHOT:
            data.update(
                {
                    "units": self.units,
                    "scenes": self.scenes,
                    "failed": self.failed,
                    "generation": self.generation,
                }
            )
        return json.dumps(data, separators=(",", ":"))


def _parse_snapshot(value: object) -> SnapshotSummary | None:
    if not isinstance(value, dict) or set(value) != SNAPSHOT_KEYS:
        return None
    counters = (value["generation"], value["units"], value["scenes"], value["failed"])
    if not all(_is_counter(counter) for counter in counters):
        return None
    if not _is_optional_counter(value["age_s"]):
        return None
    return SnapshotSummary(*counters, age_s=value["age_s"])


def protocol_version_of(payload: str | bytes | bytearray) -> int | None:
    """
    Return the declared version of an otherwise unparsed health payload.

    Lets a consumer tell "a newer bridge speaking a protocol I do not know"
    (unsupported) from "garbage on the topic" (ignored) without trusting any
    other field of the message.
    """
    value = _load_object(payload)
    if value is None:
        return None
    version = value.get("protocol_version")
    return version if _is_counter(version) else None


def _status_from_dict(value: object) -> BridgeStatus | None:
    if not isinstance(value, dict) or set(value) != STATUS_KEYS:
        return None
    if value["protocol_version"] != PROTOCOL_VERSION or isinstance(
        value["protocol_version"], bool
    ):
        return None
    snapshot = _parse_snapshot(value["snapshot"])
    time = value["time"]
    if (
        not is_valid_instance_id(value["instance_id"])
        or not _is_optional_counter(value["sequence"])
        or value["phase"] not in PHASES
        or value["ble"] not in LINK_STATES
        or value["mqtt"] not in LINK_STATES
        or not _is_optional_counter(value["uptime_s"])
        or snapshot is None
        or value["error"] not in ERROR_CATEGORIES
        or not (time is None or (isinstance(time, str) and len(time) <= 40))  # noqa: PLR2004
    ):
        return None
    # Only the last will may omit its sequence: it is fixed when the MQTT
    # client is created, so it cannot carry a live counter.
    if value["sequence"] is None and value["phase"] != PHASE_OFFLINE:
        return None
    return BridgeStatus(
        instance_id=value["instance_id"],
        sequence=value["sequence"],
        phase=value["phase"],
        ble=value["ble"],
        mqtt=value["mqtt"],
        uptime_s=value["uptime_s"],
        snapshot=snapshot,
        error=value["error"],
        time=time,
    )


def parse_status(payload: str | bytes | bytearray) -> BridgeStatus | None:
    """Decode one exact v1 status payload, failing closed."""
    return _status_from_dict(_load_object(payload))


def parse_reply(  # noqa: PLR0911
    payload: str | bytes | bytearray,
) -> HealthReply | None:
    """Decode one exact v1 reply payload, failing closed."""
    value = _load_object(payload)
    if value is None:
        return None
    reply_type = value.get("type")
    expected_keys = SNAPSHOT_REPLY_KEYS if reply_type == REPLY_SNAPSHOT else REPLY_KEYS
    if set(value) != expected_keys or reply_type not in REPLY_TYPES:
        return None
    if value["protocol_version"] != PROTOCOL_VERSION or isinstance(
        value["protocol_version"], bool
    ):
        return None
    status = _status_from_dict(value["status"])
    if (
        status is None
        or not is_valid_request_id(value["request_id"])
        or not is_valid_instance_id(value["instance_id"])
        or value["instance_id"] != status.instance_id
        or value["result"] not in RESULTS
    ):
        return None
    if reply_type == REPLY_SNAPSHOT:
        counters = (
            value["units"],
            value["scenes"],
            value["failed"],
            value["generation"],
        )
        if not all(_is_counter(counter) for counter in counters):
            return None
        return HealthReply(
            reply_type,
            value["request_id"],
            value["instance_id"],
            value["result"],
            status,
            *counters,
        )
    return HealthReply(
        reply_type,
        value["request_id"],
        value["instance_id"],
        value["result"],
        status,
    )


def get_status_command(request_id: str) -> str:
    return json.dumps({"action": GET_STATUS_ACTION, "request_id": request_id})


def snapshot_command(request_id: str) -> str:
    return json.dumps({"action": PUBLISH_ENTITIES_ACTION, "request_id": request_id})
