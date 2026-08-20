import importlib.util
import json
from pathlib import Path

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


def test_environment_contract_has_no_password_fallback(monkeypatch):
    monkeypatch.delenv("CASAMBI_PASSWORD", raising=False)
    monkeypatch.setenv("CASAMBI_NETWORK_PASSWORD", "must-not-be-used")

    with pytest.raises(probe.ConfigurationError):
        probe.read_environment()
