"""
Per-entry, non-actuating observation of the bridge and of this entry's consumer.

Three signals are kept strictly apart:

(a) bridge evidence: live v1 status heartbeats and correlated probe replies;
(b) raw delivery: a unit message reached Home Assistant's MQTT client;
(c) consumption: the entry's real unit callback finished handling a valid
    unit message (an unchanged dimmer value counts -- it was handled).

Only (c) can make the consumer healthy. Freshness always uses the monotonic
receipt time on this side; the bridge's UTC ``time`` field is never trusted.
The only commands this module can publish are ``GET_STATUS`` and the
correlated ``PUBLISH_ENTITIES`` snapshot, neither of which controls a unit.
"""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from . import health_protocol as hp

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

DEFAULT_STALE_SECONDS = 120
DEFAULT_STARTUP_GRACE_SECONDS = 90
DEFAULT_PROBE_INTERVAL_SECONDS = 300
DEFAULT_CONSUMER_PROBE_INTERVAL_SECONDS = 900
DEFAULT_PROBE_DEADLINE_SECONDS = 20
MIN_PROBE_INTERVAL_SECONDS = 300
TICK_SECONDS = 5
RETIRED_INSTANCE_MEMORY = 8

CONF_STALE_SECONDS = "health_stale_seconds"
CONF_STARTUP_GRACE_SECONDS = "health_startup_grace_seconds"
CONF_PROBE_INTERVAL_SECONDS = "health_probe_interval_seconds"
CONF_CONSUMER_PROBE_INTERVAL_SECONDS = "health_consumer_probe_interval_seconds"
CONF_PROBE_DEADLINE_SECONDS = "health_probe_deadline_seconds"

# (default, minimum, maximum) for every validated setting.
SETTING_RANGES: dict[str, tuple[int, int, int]] = {
    CONF_STALE_SECONDS: (DEFAULT_STALE_SECONDS, 60, 3600),
    CONF_STARTUP_GRACE_SECONDS: (DEFAULT_STARTUP_GRACE_SECONDS, 30, 1800),
    CONF_PROBE_INTERVAL_SECONDS: (
        DEFAULT_PROBE_INTERVAL_SECONDS,
        MIN_PROBE_INTERVAL_SECONDS,
        86400,
    ),
    CONF_CONSUMER_PROBE_INTERVAL_SECONDS: (
        DEFAULT_CONSUMER_PROBE_INTERVAL_SECONDS,
        MIN_PROBE_INTERVAL_SECONDS,
        86400,
    ),
    CONF_PROBE_DEADLINE_SECONDS: (DEFAULT_PROBE_DEADLINE_SECONDS, 5, 120),
}


@dataclass(frozen=True, slots=True)
class HealthSettings:
    stale_seconds: int = DEFAULT_STALE_SECONDS
    startup_grace_seconds: int = DEFAULT_STARTUP_GRACE_SECONDS
    probe_interval_seconds: int = DEFAULT_PROBE_INTERVAL_SECONDS
    consumer_probe_interval_seconds: int = DEFAULT_CONSUMER_PROBE_INTERVAL_SECONDS
    probe_deadline_seconds: int = DEFAULT_PROBE_DEADLINE_SECONDS

    @classmethod
    def from_options(cls, options: Mapping[str, Any]) -> HealthSettings:
        """Use each stored option only if it is an in-range integer."""
        values: dict[str, int] = {}
        for key, (default, minimum, maximum) in SETTING_RANGES.items():
            value = options.get(key, default)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or not minimum <= value <= maximum
            ):
                value = default
            values[key.removeprefix("health_")] = value
        return cls(**values)


class BridgeState(StrEnum):
    STARTING = "starting"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"
    UNSUPPORTED = "unsupported"


class ProbeKind(StrEnum):
    STATUS = "status"
    CONSUMER = "consumer"


class ProbeOutcome(StrEnum):
    OK = "ok"
    NO_REPLY = "no_reply"
    BRIDGE_FAILED = "bridge_failed"
    BRIDGE_BUSY = "bridge_busy"
    DELIVERY_INCOMPLETE = "delivery_incomplete"
    CONSUMER_PARTIAL = "consumer_partial"
    CONSUMER_STALE = "consumer_stale"
    NO_UNITS = "no_units"


class BridgeStatusTracker:
    """
    Decide bridge freshness from live, ordered v1 status messages only.

    Never healthy from: a retained message (it is history, delivered on every
    (re)subscribe), a message from an instance that has already been
    replaced, a sequence that does not increase, an ``offline`` last will, or
    anything older than ``stale_seconds`` by the local monotonic clock.
    """

    def __init__(self, settings: HealthSettings, started_at: float) -> None:
        self.settings = settings
        self.started_at = started_at
        self.status: hp.BridgeStatus | None = None
        self.received_at: float | None = None
        self.retained: hp.BridgeStatus | None = None
        self.unsupported_version = False
        self.ignored = 0
        self._retired: list[str] = []
        # Highest sequence seen from the current instance. The last will has
        # no sequence and never lowers it, so a late or redelivered older
        # heartbeat can never undo an ``offline``.
        self._max_sequence: int | None = None

    @property
    def instance_id(self) -> str | None:
        return None if self.status is None else self.status.instance_id

    def on_message(self, payload: str | bytes, *, retain: bool, now: float) -> bool:  # noqa: PLR0911
        """Record one status message; return whether it was accepted as live."""
        version = hp.protocol_version_of(payload)
        if version is not None and version != hp.PROTOCOL_VERSION:
            self.unsupported_version = True
            return False
        status = hp.parse_status(payload)
        if status is None:
            self.ignored += 1
            return False
        if retain:
            self.retained = status
            return False
        if status.instance_id in self._retired:
            self.ignored += 1
            return False
        current = self.status
        if current is not None and status.instance_id == current.instance_id:
            if current.phase == hp.PHASE_OFFLINE and status.sequence is not None:
                # After the last will, only a NEW instance can be live again.
                self.ignored += 1
                return False
            if (
                status.sequence is not None
                and self._max_sequence is not None
                and status.sequence <= self._max_sequence
            ):
                self.ignored += 1
                return False
        else:
            if current is not None:
                self._retired.append(current.instance_id)
                del self._retired[:-RETIRED_INSTANCE_MEMORY]
            self._max_sequence = None
        if status.sequence is not None:
            self._max_sequence = status.sequence
        self.status = status
        self.received_at = now
        return True

    def evaluate(self, now: float) -> BridgeState:  # noqa: PLR0911
        in_grace = now - self.started_at < self.settings.startup_grace_seconds
        if self.status is None or self.received_at is None:
            if in_grace:
                return BridgeState.STARTING
            if self.unsupported_version:
                return BridgeState.UNSUPPORTED
            # A retained status alone is history: it may be left over from a
            # v1 bridge that has since been rolled back. Without one live v1
            # message since setup this can only be "unknown".
            return BridgeState.UNKNOWN
        if self.status.phase == hp.PHASE_OFFLINE:
            return BridgeState.UNAVAILABLE
        if now - self.received_at > self.settings.stale_seconds:
            return BridgeState.UNAVAILABLE
        if self.status.phase == hp.PHASE_READY:
            return BridgeState.HEALTHY
        if self.status.phase == hp.PHASE_DEGRADED:
            return BridgeState.DEGRADED
        return BridgeState.STARTING


@dataclass
class PendingProbe:
    kind: ProbeKind
    request_id: str
    sent_at: float
    deadline_at: float
    reply: hp.HealthReply | None = None
    raw_topics: set[str] = field(default_factory=set)
    processed_topics: set[str] = field(default_factory=set)


@dataclass(frozen=True, slots=True)
class ProbeResult:
    kind: ProbeKind
    outcome: ProbeOutcome
    finished_at: float
    expected_units: int | None = None
    raw_units: int = 0
    processed_units: int = 0


def is_unit_event_topic(topic: str, event_prefix: str) -> bool:
    """Return whether this is a unit topic, never the bare legacy tombstone."""
    return topic.startswith(event_prefix) and len(topic) > len(event_prefix)


class ProbeTracker:
    """
    One correlated probe at a time, accepted only by its own fresh reply.

    A reply is accepted only when its request ID is the pending one, it
    arrives before the deadline, and its instance matches the live bridge
    instance (when one is known). A consumer probe then additionally needs
    the entry's real callback to have handled at least as many distinct unit
    topics as the bridge confirmed publishing. A reply alone never proves
    consumption, and an MQTT PUBACK is never a reply.
    """

    def __init__(
        self,
        settings: HealthSettings,
        token: Callable[[], str] = lambda: f"ha-{secrets.token_hex(8)}",
    ) -> None:
        self.settings = settings
        self._token = token
        self.pending: PendingProbe | None = None
        self.rejected_replies = 0

    def start(self, kind: ProbeKind, now: float) -> PendingProbe | None:
        if self.pending is not None:
            return None
        self.pending = PendingProbe(
            kind=kind,
            request_id=self._token(),
            sent_at=now,
            deadline_at=now + self.settings.probe_deadline_seconds,
        )
        return self.pending

    def on_reply(
        self, payload: str | bytes, *, now: float, instance_id: str | None
    ) -> ProbeResult | None:
        reply = hp.parse_reply(payload)
        pending = self.pending
        if (
            reply is None
            or pending is None
            or pending.reply is not None
            or reply.request_id != pending.request_id
            or now > pending.deadline_at
            or (instance_id is not None and reply.instance_id != instance_id)
            or reply.type
            != (
                hp.REPLY_STATUS
                if pending.kind is ProbeKind.STATUS
                else hp.REPLY_SNAPSHOT
            )
        ):
            self.rejected_replies += 1
            return None
        pending.reply = reply
        if reply.result in {hp.RESULT_RATE_LIMITED, hp.RESULT_BUSY}:
            return self._finish(ProbeOutcome.BRIDGE_BUSY, now)
        if reply.result != hp.RESULT_OK:
            return self._finish(ProbeOutcome.BRIDGE_FAILED, now)
        if pending.kind is ProbeKind.STATUS:
            return self._finish(ProbeOutcome.OK, now)
        if not reply.units:
            # Nothing was published, so nothing can prove consumption.
            return self._finish(ProbeOutcome.NO_UNITS, now)
        return self.check(now)

    def record_raw(self, topic: str) -> None:
        if self.pending is not None and self.pending.kind is ProbeKind.CONSUMER:
            self.pending.raw_topics.add(topic)

    def record_processed(self, topic: str, now: float) -> ProbeResult | None:
        if self.pending is None or self.pending.kind is not ProbeKind.CONSUMER:
            return None
        self.pending.processed_topics.add(topic)
        return self.check(now)

    def check(self, now: float) -> ProbeResult | None:  # noqa: PLR0911
        """Finish the pending probe if it has succeeded or its deadline passed."""
        pending = self.pending
        if pending is None:
            return None
        reply = pending.reply
        if (
            pending.kind is ProbeKind.CONSUMER
            and reply is not None
            and reply.units
            and len(pending.processed_topics) >= reply.units
        ):
            return self._finish(ProbeOutcome.OK, now)
        if now <= pending.deadline_at:
            return None
        if reply is None:
            return self._finish(ProbeOutcome.NO_REPLY, now)
        units = reply.units or 0
        if len(pending.raw_topics) < units:
            return self._finish(ProbeOutcome.DELIVERY_INCOMPLETE, now)
        if not pending.processed_topics:
            # Everything reached Home Assistant and nothing was handled.
            return self._finish(ProbeOutcome.CONSUMER_STALE, now)
        # Some units were handled: the consumer runs, but a few payloads were
        # rejected or failed. Degraded, never a reason to reload the entry.
        return self._finish(ProbeOutcome.CONSUMER_PARTIAL, now)

    def cancel(self) -> None:
        self.pending = None

    def _finish(self, outcome: ProbeOutcome, now: float) -> ProbeResult:
        pending = self.pending
        assert pending is not None  # noqa: S101
        self.pending = None
        reply = pending.reply
        return ProbeResult(
            kind=pending.kind,
            outcome=outcome,
            finished_at=now,
            expected_units=None if reply is None else reply.units,
            raw_units=len(pending.raw_topics),
            processed_units=len(pending.processed_topics),
        )


@dataclass
class ConsumerCounters:
    raw_messages: int = 0
    processed_messages: int = 0
    last_raw_at: float | None = None
    last_processed_at: float | None = None


class HealthObserver:
    """
    Pure decision core for one config entry (no Home Assistant objects).

    The Home Assistant wiring feeds it messages and ticks and publishes the
    probe commands it asks for; everything it decides is testable with a
    fake clock.
    """

    def __init__(
        self,
        settings: HealthSettings,
        event_prefix: str,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        token: Callable[[], str] | None = None,
    ) -> None:
        self.settings = settings
        self.event_prefix = event_prefix
        self._monotonic = monotonic
        self.started_at = monotonic()
        self.bridge = BridgeStatusTracker(settings, self.started_at)
        self.probes = (
            ProbeTracker(settings) if token is None else ProbeTracker(settings, token)
        )
        self.consumer = ConsumerCounters()
        self.last_result: dict[ProbeKind, ProbeResult] = {}
        self.last_ok: dict[ProbeKind, float] = {}
        self.listeners: list[Callable[[ProbeResult], None]] = []
        self._next_due: dict[ProbeKind, float] = {
            ProbeKind.STATUS: self.started_at + settings.startup_grace_seconds,
            ProbeKind.CONSUMER: self.started_at + settings.startup_grace_seconds,
        }

    def now(self) -> float:
        return self._monotonic()

    def bridge_state(self) -> BridgeState:
        state = self.bridge.evaluate(self.now())
        if state is BridgeState.UNKNOWN and self._never_answered():
            return BridgeState.UNSUPPORTED
        return state

    def _never_answered(self) -> bool:
        """Return whether a status probe got no reply and no v1 status exists."""
        result = self.last_result.get(ProbeKind.STATUS)
        return (
            result is not None
            and result.outcome is ProbeOutcome.NO_REPLY
            and ProbeKind.STATUS not in self.last_ok
        )

    def on_status(self, payload: str | bytes, *, retain: bool) -> None:
        self.bridge.on_message(payload, retain=retain, now=self.now())

    def on_reply(self, payload: str | bytes) -> None:
        self._publish_result(
            self.probes.on_reply(
                payload, now=self.now(), instance_id=self.bridge.instance_id
            )
        )

    def on_raw_unit(self, topic: str, payload: str | bytes) -> None:
        if not payload or not is_unit_event_topic(topic, self.event_prefix):
            return
        self.consumer.raw_messages += 1
        self.consumer.last_raw_at = self.now()
        self.probes.record_raw(topic)

    def on_processed_unit(self, topic: str) -> None:
        if not is_unit_event_topic(topic, self.event_prefix):
            return
        now = self.now()
        self.consumer.processed_messages += 1
        self.consumer.last_processed_at = now
        self._publish_result(self.probes.record_processed(topic, now))

    def tick(self) -> PendingProbe | None:
        """Expire probes and return a new probe to send, if one is due."""
        now = self.now()
        self._publish_result(self.probes.check(now))
        if self.probes.pending is not None:
            return None
        # Most overdue first, so neither probe kind can starve the other.
        for kind in sorted(self._next_due, key=self._next_due.__getitem__):
            if now < self._next_due[kind]:
                continue
            if (
                kind is ProbeKind.CONSUMER
                and self.bridge.evaluate(now) is not BridgeState.HEALTHY
            ):
                continue
            return self.start_probe(kind)
        return None

    def start_probe(self, kind: ProbeKind) -> PendingProbe | None:
        now = self.now()
        probe = self.probes.start(kind, now)
        if probe is not None:
            interval = (
                self.settings.consumer_probe_interval_seconds
                if kind is ProbeKind.CONSUMER
                else self.settings.probe_interval_seconds
            )
            self._next_due[kind] = now + interval
        return probe

    def _publish_result(self, result: ProbeResult | None) -> None:
        if result is None:
            return
        self.last_result[result.kind] = result
        if result.outcome is ProbeOutcome.OK:
            self.last_ok[result.kind] = result.finished_at
        for listener in list(self.listeners):
            listener(result)

    def diagnostics(self) -> dict[str, Any]:
        """Integers, fixed enums and ages only: no IDs, topics or payloads."""
        now = self.now()

        def age(value: float | None) -> int | None:
            return None if value is None else int(now - value)

        status = self.bridge.status
        return {
            "protocol_version": hp.PROTOCOL_VERSION,
            "bridge_state": self.bridge_state().value,
            "bridge_phase": None if status is None else status.phase,
            "bridge_error": None if status is None else status.error,
            "bridge_ble": None if status is None else status.ble,
            "bridge_snapshot_units": None if status is None else status.snapshot.units,
            "bridge_status_age_s": age(self.bridge.received_at),
            "ignored_status_messages": self.bridge.ignored,
            "rejected_replies": self.probes.rejected_replies,
            "probe_pending": None
            if self.probes.pending is None
            else self.probes.pending.kind.value,
            "last_probe": {
                kind.value: {
                    "outcome": result.outcome.value,
                    "age_s": age(result.finished_at),
                    "expected_units": result.expected_units,
                    "raw_units": result.raw_units,
                    "processed_units": result.processed_units,
                }
                for kind, result in self.last_result.items()
            },
            "last_ok_age_s": {
                kind.value: age(value) for kind, value in self.last_ok.items()
            },
            "consumer": {
                "raw_messages": self.consumer.raw_messages,
                "processed_messages": self.consumer.processed_messages,
                "last_raw_age_s": age(self.consumer.last_raw_at),
                "last_processed_age_s": age(self.consumer.last_processed_at),
            },
        }
