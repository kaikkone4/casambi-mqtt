import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPT = Path(__file__).parents[1] / "scripts" / "casambi_switch_probe.py"
SPEC = importlib.util.spec_from_file_location("casambi_switch_probe", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def test_sanitize_switch_event_whitelists_documented_fields_and_values():
    event = {
        "event": "button_press",
        "button": 2,
        "unit_id": 7,
        "address": "AA:BB:CC:DD:EE:FF",
        "raw_packet": b"secret packet",
        "payload_hex": "deadbeef",
        "origin": 123,
    }

    assert probe.sanitize_switch_event(event) == {
        "event": "button_press",
        "button": 2,
        "unit_id": 7,
    }
    assert probe.sanitize_switch_event(
        {"event": "not\na-record", "button": True, "unit_id": "address"}
    ) == {}


@pytest.mark.asyncio
async def test_listen_is_90_seconds_and_only_emits_sanitized_switch_records(capsys):
    calls = []

    class FakeCasambi:
        handler = None

        def registerSwitchEventHandler(self, handler):
            calls.append("register")
            self.handler = handler

        def unregisterSwitchEventHandler(self, handler):
            assert handler is self.handler
            calls.append("unregister")

        async def connect(self, target, password):
            assert target == "private-address"
            assert password == "super-secret"
            calls.append("connect")

        async def disconnect(self):
            calls.append("disconnect")

    casa = FakeCasambi()

    async def fake_sleep(seconds):
        assert seconds == 90
        casa.handler(
            {
                "event": "button_release",
                "button": 4,
                "unit_id": 11,
                "address": "private-address",
                "raw_packet": b"raw",
                "payload_hex": "cafe",
            }
        )
        calls.append("sleep")

    await probe.listen(casa, "private-address", "super-secret", sleep=fake_sleep)

    assert calls == ["register", "connect", "sleep", "unregister", "disconnect"]
    output = capsys.readouterr().out
    prefix, payload = output.rstrip("\n").split(" ", 1)
    assert prefix == "SWITCH_EVENT"
    assert json.loads(payload) == {
        "button": 4,
        "event": "button_release",
        "unit_id": 11,
    }
    assert "private-address" not in output
    assert "super-secret" not in output
    assert "raw" not in output


@pytest.mark.asyncio
async def test_resolve_target_normalizes_colonless_address_before_connect():
    async def discovery_must_not_run():
        pytest.fail("discovery must not run for a configured address")

    target = await probe.resolve_target("aabbccddeeff", discovery_must_not_run)

    assert target == "AA:BB:CC:DD:EE:FF"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "address",
    ["aa:bb:cc:dd:ee:ff", "AA-BB-CC-DD-EE-FF", "aabb.ccdd.eeff"],
)
async def test_resolve_target_strips_standard_address_separators(address):
    async def discovery_must_not_run():
        pytest.fail("discovery must not run for a configured address")

    target = await probe.resolve_target(address, discovery_must_not_run)

    assert target == "AA:BB:CC:DD:EE:FF"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "address",
    [
        "AABBCCDDEEF",
        "AABBCCDDEEFF0",
        "AABBCCDDEEFG",
        "AA BB CC DD EE FF",
        "AA_BB_CC_DD_EE_FF",
    ],
)
async def test_resolve_target_rejects_non_twelve_hex_address(address):
    async def discovery_must_not_run():
        pytest.fail("discovery must not run for a configured address")

    with pytest.raises(probe.ProbeError):
        await probe.resolve_target(address, discovery_must_not_run)


def test_invalid_address_uses_fixed_sanitized_failure(monkeypatch, capsys):
    private_address = "private-invalid-address"
    monkeypatch.setenv("CASAMBI_PASSWORD", "super-secret")
    monkeypatch.setenv("CASAMBI_ADDR", private_address)
    monkeypatch.setattr(probe, "version", lambda distribution: probe.REQUIRED_VERSION)
    monkeypatch.setitem(
        sys.modules,
        "CasambiBt",
        SimpleNamespace(Casambi=object, discover=lambda: None),
    )

    assert probe.main() == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "casambi switch probe: failed\n"
    assert private_address not in captured.err


def test_environment_contract_has_no_password_fallback(monkeypatch):
    monkeypatch.delenv("CASAMBI_PASSWORD", raising=False)
    monkeypatch.setenv("CASAMBI_NETWORK_PASSWORD", "must-not-be-used")

    with pytest.raises(probe.ConfigurationError):
        probe.read_environment()
