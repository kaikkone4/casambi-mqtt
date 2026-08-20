"""Tests for the bridge-owned Casambi switch-event decoder.

Every frame here is synthetic and hand-built from the documented invocation
layout. There are no captures, addresses, identifiers, keys or credentials in
this file, and none may be added to it.
"""

import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from switch_decoder import (  # noqa: E402
    BUTTON_EVENT_BASE,
    BUTTON_EVENT_LAST,
    INPUT_EVENT_BASE,
    INPUT_EVENT_LAST,
    RETRANSMIT_WINDOW_SECONDS,
    TARGET_TYPE_BUTTON,
    TARGET_TYPE_INPUT,
    SwitchEventDecoder,
    SwitchEventPhase,
    install_switch_event_decoder,
    parse_invocation_stream,
)

UNIT_ID = 7
PRESS_ORIGIN = 0x1100
RELEASE_ORIGIN = 0x1102  # bit 1 set marks a release on the button stream


def frame(
    *,
    opcode,
    origin,
    target_type,
    unit_id=UNIT_ID,
    age=0,
    payload=b"",
    origin_handle=None,
    flags_extra=0,
    declared_payload_len=None,
):
    """Build one invocation frame from the documented field layout."""
    payload_len = len(payload) if declared_payload_len is None else declared_payload_len
    flags = flags_extra | (payload_len & 0x3F)
    if origin_handle is not None:
        flags |= 0x0200
    out = flags.to_bytes(2, "big")
    out += bytes([opcode])
    out += origin.to_bytes(2, "big")
    out += (((unit_id & 0xFF) << 8) | target_type).to_bytes(2, "big")
    out += age.to_bytes(2, "big")
    if origin_handle is not None:
        out += bytes([origin_handle])
    return out + payload


def button_frame(index, *, release=False, unit_id=UNIT_ID, origin=None):
    """A 0x06 button-stream frame: the authoritative press/release source."""
    if origin is None:
        origin = RELEASE_ORIGIN if release else PRESS_ORIGIN
    return frame(
        opcode=BUTTON_EVENT_BASE + index,
        origin=origin,
        target_type=TARGET_TYPE_BUTTON,
        unit_id=unit_id,
    )


def input_frame(index, phase, *, unit_id=UNIT_ID, origin=PRESS_ORIGIN):
    """A 0x12 input-stream frame: carries the phase code in payload[0]."""
    return frame(
        opcode=INPUT_EVENT_BASE + index,
        origin=origin,
        target_type=TARGET_TYPE_INPUT,
        unit_id=unit_id,
        payload=bytes([phase]),
    )


class Clock:
    """A clock that only moves when a test moves it."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def decoder(clock=None):
    return SwitchEventDecoder(monotonic=clock or Clock())


def published(events):
    """Reduce to what the MQTT contract actually carries."""
    return [(e.unit_id, e.button, e.event.name) for e in events]


# 1. invocation-frame parsing


def test_parses_field_layout_and_payload_boundary():
    data = frame(opcode=31, origin=0xABCD, target_type=TARGET_TYPE_BUTTON, age=9, payload=b"\x01\x02")
    (parsed,) = parse_invocation_stream(data)
    assert parsed.opcode == 31
    assert parsed.origin == 0xABCD
    assert parsed.target == ((UNIT_ID << 8) | TARGET_TYPE_BUTTON)
    assert parsed.age == 9
    assert parsed.payload == b"\x01\x02"
    assert parsed.payload_len == 2
    assert parsed.origin_handle is None


def test_parses_optional_origin_handle_without_shifting_payload():
    data = frame(
        opcode=31,
        origin=1,
        target_type=TARGET_TYPE_INPUT,
        payload=b"\x09",
        origin_handle=0x5A,
    )
    (parsed,) = parse_invocation_stream(data)
    assert parsed.origin_handle == 0x5A
    assert parsed.payload == b"\x09"


def test_parses_several_frames_from_one_packet():
    data = button_frame(0) + input_frame(0, SwitchEventPhase.HOLD.value)
    first, second = parse_invocation_stream(data)
    assert first.target & 0xFF == TARGET_TYPE_BUTTON
    assert second.target & 0xFF == TARGET_TYPE_INPUT


# 2 & 3. opcode -> logical button


@pytest.mark.parametrize("index", range(8))
def test_button_stream_opcodes_map_to_buttons_one_to_eight(index):
    assert BUTTON_EVENT_BASE + index <= BUTTON_EVENT_LAST
    events = decoder().decode(button_frame(index))
    assert published(events) == [(UNIT_ID, index + 1, "PRESS")]


@pytest.mark.parametrize("index", range(8))
def test_input_stream_opcodes_map_to_the_same_buttons(index):
    assert INPUT_EVENT_BASE + index <= INPUT_EVENT_LAST
    events = decoder().decode(input_frame(index, SwitchEventPhase.HOLD.value))
    assert published(events) == [(UNIT_ID, index + 1, "HOLD")]


# 4. the regression this fix exists for


def test_opcode_0x40_is_input_zero_and_never_button_four():
    assert INPUT_EVENT_BASE == 0x40
    events = decoder().decode(input_frame(0, SwitchEventPhase.HOLD.value))
    assert published(events) == [(UNIT_ID, 1, "HOLD")]
    assert all(event.button != 4 for event in events)


def test_button_four_requires_the_fourth_control():
    events = decoder().decode(button_frame(3))
    assert published(events) == [(UNIT_ID, 4, "PRESS")]


# 5. distinct controls stay distinct


def test_distinct_opcodes_produce_distinct_buttons():
    seen = set()
    for index in range(8):
        (event,) = decoder().decode(button_frame(index))
        seen.add(event.button)
    assert seen == {1, 2, 3, 4, 5, 6, 7, 8}


# 6. one physical action -> one canonical event


def test_both_streams_for_one_action_yield_a_single_press():
    packet = button_frame(2) + input_frame(2, SwitchEventPhase.PRESS.value)
    assert published(decoder().decode(packet)) == [(UNIT_ID, 3, "PRESS")]


def test_input_stream_release_alone_is_not_published():
    assert decoder().decode(input_frame(2, SwitchEventPhase.RELEASE.value)) == []


# 7. retransmits


def test_retransmitted_frames_do_not_duplicate_an_action():
    dec = decoder()
    packet = button_frame(0) + input_frame(0, SwitchEventPhase.PRESS.value)
    first = dec.decode(packet)
    again = dec.decode(packet)
    assert published(first) == [(UNIT_ID, 1, "PRESS")]
    assert again == []


def test_a_later_action_with_a_new_origin_is_not_suppressed():
    dec = decoder()
    dec.decode(button_frame(0, origin=0x2000))
    later = dec.decode(button_frame(0, origin=0x2004))
    assert published(later) == [(UNIT_ID, 1, "PRESS")]


def test_the_same_origin_is_accepted_again_once_the_window_has_passed():
    clock = Clock()
    dec = decoder(clock)
    dec.decode(button_frame(0))
    clock.advance(RETRANSMIT_WINDOW_SECONDS * 2)
    assert published(dec.decode(button_frame(0))) == [(UNIT_ID, 1, "PRESS")]


def test_same_origin_on_a_different_button_is_still_reported():
    dec = decoder()
    dec.decode(button_frame(0))
    other = dec.decode(button_frame(1))
    assert published(other) == [(UNIT_ID, 2, "PRESS")]


# 8. a genuine press/release pair must always survive


def test_press_then_release_is_never_collapsed():
    dec = decoder()
    press = dec.decode(button_frame(1))
    release = dec.decode(button_frame(1, release=True))
    assert published(press) == [(UNIT_ID, 2, "PRESS")]
    assert published(release) == [(UNIT_ID, 2, "RELEASE")]


def test_press_and_release_in_one_packet_both_survive():
    packet = button_frame(1) + button_frame(1, release=True)
    assert published(decoder().decode(packet)) == [
        (UNIT_ID, 2, "PRESS"),
        (UNIT_ID, 2, "RELEASE"),
    ]


# 9. hold


def test_hold_sequence_keeps_its_phases_without_a_spurious_release():
    dec = decoder()
    result = []
    # press
    result += dec.decode(button_frame(0) + input_frame(0, SwitchEventPhase.PRESS.value))
    # the input stream emits a release code mid-hold; it must be ignored
    result += dec.decode(input_frame(0, SwitchEventPhase.RELEASE.value, origin=0x1200))
    # hold, then the real release after hold
    result += dec.decode(input_frame(0, SwitchEventPhase.HOLD.value, origin=0x1300))
    result += dec.decode(
        input_frame(0, SwitchEventPhase.RELEASE_AFTER_HOLD.value, origin=0x1400)
    )
    result += dec.decode(button_frame(0, release=True))

    assert published(result) == [
        (UNIT_ID, 1, "PRESS"),
        (UNIT_ID, 1, "HOLD"),
        (UNIT_ID, 1, "RELEASE_AFTER_HOLD"),
        (UNIT_ID, 1, "RELEASE"),
    ]


# 10. fail closed


@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"\x00\x01\x1d",
        frame(opcode=31, origin=1, target_type=TARGET_TYPE_BUTTON)[:-2],
        frame(opcode=31, origin=1, target_type=TARGET_TYPE_BUTTON, declared_payload_len=40),
        frame(opcode=0x7F, origin=1, target_type=TARGET_TYPE_BUTTON),
        frame(opcode=BUTTON_EVENT_BASE, origin=1, target_type=0x33),
        frame(opcode=INPUT_EVENT_BASE, origin=1, target_type=TARGET_TYPE_INPUT),
        frame(
            opcode=INPUT_EVENT_BASE,
            origin=1,
            target_type=TARGET_TYPE_INPUT,
            payload=b"\xee",
        ),
    ],
)
def test_invalid_frames_publish_nothing(data):
    assert decoder().decode(data) == []


def test_a_valid_frame_after_a_broken_one_is_still_lost_not_guessed():
    """Truncation ends the stream: no resynchronisation guessing."""
    broken = frame(
        opcode=BUTTON_EVENT_BASE, origin=1, target_type=TARGET_TYPE_BUTTON,
        declared_payload_len=40,
    )
    assert decoder().decode(broken + button_frame(0)) == []


# 11. the public contract


def test_decoded_event_exposes_only_contract_fields():
    (event,) = decoder().decode(button_frame(0))
    assert {f for f in vars(event)} == {"unit_id", "button", "event"}


def test_phase_names_match_the_published_event_vocabulary():
    assert {phase.name for phase in SwitchEventPhase} == {
        "PRESS",
        "RELEASE",
        "HOLD",
        "RELEASE_AFTER_HOLD",
    }


# the install seam


def _fake_client_module():
    module = types.ModuleType("CasambiBt._client")
    module.parseSwitchEvents = lambda data, seq, raw: ["original"]
    return module


def test_install_replaces_only_the_switch_parser(monkeypatch):
    client = _fake_client_module()
    monkeypatch.setitem(sys.modules, "CasambiBt._client", client)
    install_switch_event_decoder()
    assert client.parseSwitchEvents is not None
    events = client.parseSwitchEvents(button_frame(0), 1, b"")
    assert published(events) == [(UNIT_ID, 1, "PRESS")]


def test_install_is_idempotent(monkeypatch):
    client = _fake_client_module()
    monkeypatch.setitem(sys.modules, "CasambiBt._client", client)
    install_switch_event_decoder()
    first = client.parseSwitchEvents
    install_switch_event_decoder()
    assert client.parseSwitchEvents is first


def test_install_fails_closed_when_the_seam_is_missing(monkeypatch):
    module = types.ModuleType("CasambiBt._client")
    monkeypatch.setitem(sys.modules, "CasambiBt._client", module)
    with pytest.raises(RuntimeError):
        install_switch_event_decoder()


# the bridge wiring


def _load_server():
    from test_server import load_server_module

    return load_server_module()


def test_building_a_connection_installs_the_decoder(monkeypatch):
    """The bridge must correct the parser before any packet can arrive."""
    server = _load_server()
    client = _fake_client_module()
    monkeypatch.setitem(sys.modules, "CasambiBt._client", client)
    server.create_casambi_connection()
    assert client.parseSwitchEvents(button_frame(0), 1, b"") != ["original"]


def test_decoded_events_sanitize_to_the_published_contract():
    """A decoded event must survive the bridge's own sanitiser unchanged."""
    server = _load_server()
    (event,) = decoder().decode(button_frame(3))
    assert server.sanitize_switch_event(event) == {
        "unit_id": UNIT_ID,
        "button": 4,
        "event": "PRESS",
    }


def test_the_old_button_four_payload_now_carries_the_first_control():
    """The regression, end to end: opcode 0x40 used to publish button 4."""
    server = _load_server()
    (event,) = decoder().decode(input_frame(0, SwitchEventPhase.HOLD.value))
    assert server.sanitize_switch_event(event) == {
        "unit_id": UNIT_ID,
        "button": 1,
        "event": "HOLD",
    }


def test_sanitiser_still_rejects_anything_outside_the_contract():
    server = _load_server()

    class NotAnEvent:
        unit_id = UNIT_ID
        button = 1

    assert server.sanitize_switch_event(NotAnEvent()) is None
