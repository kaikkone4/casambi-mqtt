"""
Bridge-owned decoder for Casambi switch-event packets.

Why this module exists
----------------------
casambi-bt 0.3.2 parses switch-event packets with an incorrect frame model. It
reads the frame as ``message_type | flags | parameter`` and derives the button
number from nibbles of ``parameter``. In the real layout that byte is the
*opcode*, so opcode 0x40 (FunctionNotifyInput0) is reported as ``button=4``
purely because 4 is its high nibble, and its frame walk desynchronises because
the frame length it computes is not the real one. Every physical control on a
Casambi-paired EnOcean PTM215B/PTM216B therefore reaches MQTT as button 4.

casambi-bt 0.4.0b4 fixes this with a rewritten decoder, but 0.4.0b* is a
pre-release and the rest of casambi-bt 0.3.2 (discovery, connection lifecycle,
unit state, lights, scenes, cache/session) is working in production. Upgrading
the whole library to a beta to fix one parser would put that stable path at
risk, so this module reimplements only the switch-event decode and is installed
over the single ``parseSwitchEvents`` seam. Nothing else in casambi-bt is
touched.

Attribution
-----------
The invocation-frame layout, the opcode ranges, the two-stream behaviour and
the retransmit-suppression approach are adapted from casambi-bt, Copyright the
casambi-bt authors (https://github.com/lkempf/casambi-bt), licensed under the
Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
Specifically from ``CasambiBt/_invocation.py`` and ``CasambiBt/_switch.py`` as
published in the 0.4.0b4 pre-release. casambi-bt documents that layout as
derived from the Casambi Android application.

Changes made relative to that source: the decode is restructured into a pure
function plus a small stateful class, the clock is injectable so the retransmit
window is testable, the emitted object carries only the fields this bridge
publishes, and unknown or truncated input yields no event at all rather than an
``UNKNOWN`` placeholder.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from enum import Enum, unique
from time import monotonic as _monotonic
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from collections.abc import Callable

# Invocation frame: flags:u16 big-endian, opcode:u8, origin:u16, target:u16,
# age:u16, origin_handle:u8 when flags & 0x0200, then flags & 0x3f payload bytes.
_HEADER_LENGTH: Final[int] = 9
_FLAG_HAS_ORIGIN_HANDLE: Final[int] = 0x0200
_PAYLOAD_LENGTH_MASK: Final[int] = 0x3F

# Opcode ranges. The logical control is the offset from the range base.
BUTTON_EVENT_BASE: Final[int] = 29  # FunctionButtonEvent0
BUTTON_EVENT_LAST: Final[int] = 36  # FunctionButtonEvent7
INPUT_EVENT_BASE: Final[int] = 64  # FunctionNotifyInput0
INPUT_EVENT_LAST: Final[int] = 71  # FunctionNotifyInput7

# The low byte of `target` selects the stream a frame belongs to.
TARGET_TYPE_BUTTON: Final[int] = 0x06
TARGET_TYPE_INPUT: Final[int] = 0x12

# A physical action is retransmitted several times on both streams. All copies
# share one `origin`; a new action carries a new one. The window only stops a
# stale entry from masking a genuine later action that reuses an origin.
RETRANSMIT_WINDOW_SECONDS: Final[float] = 0.5

# The button stream marks a release in bit 1 of `origin`.
_ORIGIN_RELEASE_BIT: Final[int] = 0x02


@unique
class SwitchEventPhase(Enum):
    """
    The published phases, valued by their input-stream codes.

    There is deliberately no UNKNOWN member: a code this bridge cannot name is
    not something it should publish.
    """

    PRESS = 0x01
    RELEASE = 0x02
    HOLD = 0x09
    RELEASE_AFTER_HOLD = 0x0C


@dataclass(frozen=True)
class InvocationFrame:
    """One parsed invocation frame."""

    flags: int
    opcode: int
    origin: int
    target: int
    age: int
    origin_handle: int | None
    payload: bytes

    @property
    def payload_len(self) -> int:
        return self.flags & _PAYLOAD_LENGTH_MASK

    @property
    def unit_id(self) -> int:
        return self.target >> 8

    @property
    def target_type(self) -> int:
        return self.target & 0xFF


@dataclass(frozen=True)
class SwitchEvent:
    """
    Exactly the fields the bridge publishes, and nothing else.

    Shaped to what casambi-mqtt already consumes from casambi-bt's callback:
    ``unit_id``, ``button`` and ``event.name``.
    """

    unit_id: int
    button: int
    event: SwitchEventPhase


def parse_invocation_stream(data: bytes) -> list[InvocationFrame]:
    """Parse a decrypted switch-event packet body into invocation frames."""
    frames: list[InvocationFrame] = []
    pos = 0

    while len(data) - pos >= _HEADER_LENGTH:
        flags = int.from_bytes(data[pos : pos + 2], "big")
        opcode = data[pos + 2]
        origin = int.from_bytes(data[pos + 3 : pos + 5], "big")
        target = int.from_bytes(data[pos + 5 : pos + 7], "big")
        age = int.from_bytes(data[pos + 7 : pos + 9], "big")
        pos += _HEADER_LENGTH

        origin_handle: int | None = None
        if flags & _FLAG_HAS_ORIGIN_HANDLE:
            if pos >= len(data):
                # Truncated: stop rather than guess at a resynchronisation.
                break
            origin_handle = data[pos]
            pos += 1

        payload_len = flags & _PAYLOAD_LENGTH_MASK
        if pos + payload_len > len(data):
            break
        payload = data[pos : pos + payload_len]
        pos += payload_len

        frames.append(
            InvocationFrame(
                flags=flags,
                opcode=opcode,
                origin=origin,
                target=target,
                age=age,
                origin_handle=origin_handle,
                payload=payload,
            )
        )

    return frames


class SwitchEventDecoder:
    """
    Turn switch-event packets into one event per physical action.

    Canonicalisation policy:

    * The button stream (0x06) is authoritative for PRESS and RELEASE.
    * The input stream (0x12) contributes only HOLD and RELEASE_AFTER_HOLD. Its
      PRESS and RELEASE codes are dropped, because it emits a release code part
      way through a hold, which would otherwise surface as a release that never
      physically happened.
    * Retransmissions are suppressed per (unit, control, origin).
    """

    def __init__(self, *, monotonic: Callable[[], float] = _monotonic) -> None:
        self._monotonic = monotonic
        self._seen: dict[tuple[int, int, int], float] = {}

    def reset(self) -> None:
        """Forget retransmit state, for use after a reconnect."""
        self._seen.clear()

    def _is_retransmit(self, unit_id: int, control: int, origin: int) -> bool:
        now = self._monotonic()
        key = (unit_id, control, origin)
        previous = self._seen.get(key)
        if previous is not None and now - previous < RETRANSMIT_WINDOW_SECONDS:
            return True
        self._seen[key] = now
        self._expire(now)
        return False

    def _expire(self, now: float) -> None:
        """Drop entries that can no longer suppress anything."""
        for key, seen_at in list(self._seen.items()):
            if now - seen_at >= RETRANSMIT_WINDOW_SECONDS:
                del self._seen[key]

    def decode(self, data: bytes) -> list[SwitchEvent]:
        """Return the events one packet genuinely represents."""
        events: list[SwitchEvent] = []
        for frame in parse_invocation_stream(data):
            event = self._decode_frame(frame)
            if event is not None:
                events.append(event)
        return events

    def _decode_frame(self, frame: InvocationFrame) -> SwitchEvent | None:
        target_type = frame.target_type
        opcode = frame.opcode

        if target_type == TARGET_TYPE_BUTTON and (
            BUTTON_EVENT_BASE <= opcode <= BUTTON_EVENT_LAST
        ):
            control = opcode - BUTTON_EVENT_BASE
            phase = (
                SwitchEventPhase.RELEASE
                if frame.origin & _ORIGIN_RELEASE_BIT
                else SwitchEventPhase.PRESS
            )
        elif target_type == TARGET_TYPE_INPUT and (
            INPUT_EVENT_BASE <= opcode <= INPUT_EVENT_LAST
        ):
            if not frame.payload:
                return None
            try:
                phase = SwitchEventPhase(frame.payload[0])
            except ValueError:
                return None
            if phase in (SwitchEventPhase.PRESS, SwitchEventPhase.RELEASE):
                # The button stream owns these phases.
                return None
            control = opcode - INPUT_EVENT_BASE
        else:
            return None

        if self._is_retransmit(frame.unit_id, control, frame.origin):
            return None

        return SwitchEvent(
            unit_id=frame.unit_id,
            button=control + 1,
            event=phase,
        )


def install_switch_event_decoder(
    *, monotonic: Callable[[], float] = _monotonic
) -> None:
    """
    Replace casambi-bt's switch-event parser with this decoder.

    ``CasambiBt._client`` binds ``parseSwitchEvents`` at import time and calls
    it for switch-event packets only, so rebinding that one name is the
    narrowest seam that corrects the defect. Discovery, the connection
    lifecycle, unit state, lights and scenes all run through different code and
    are left exactly as they are.

    Raises RuntimeError if the seam is absent, so a future casambi-bt that
    moved it fails loudly at startup instead of silently publishing button 4
    again.
    """
    client: Any = sys.modules.get("CasambiBt._client")
    if client is None:
        import CasambiBt._client as client  # noqa: PLC0415

    if not hasattr(client, "parseSwitchEvents"):
        message = "casambi-bt no longer exposes the switch-event parser seam"
        raise RuntimeError(message)

    if getattr(client.parseSwitchEvents, "_casambi_mqtt_decoder", False):
        return

    decoder = SwitchEventDecoder(monotonic=monotonic)

    def parse_switch_events(
        data: bytes, packet_seq: int, raw_packet: bytes
    ) -> list[SwitchEvent]:
        """Match casambi-bt's parser signature; ignore the raw packet."""
        return decoder.decode(data)

    parse_switch_events._casambi_mqtt_decoder = True  # noqa: SLF001
    client.parseSwitchEvents = parse_switch_events
