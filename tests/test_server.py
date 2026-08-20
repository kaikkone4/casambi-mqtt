import asyncio
import importlib.util
import json
import sys
import types
from enum import Enum
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load_server_module():
    """Load server.py without requiring the platform BLE dependency in CI."""
    fake_casambi = types.ModuleType("CasambiBt")
    for name in (
        "UnitControlType",
        "UnitControl",
        "UnitType",
        "UnitState",
        "Unit",
        "Scene",
    ):
        setattr(fake_casambi, name, type(name, (), {}))
    fake_casambi.Casambi = type("Casambi", (), {})

    async def discover():
        return []

    fake_casambi.discover = discover
    previous = sys.modules.get("CasambiBt")
    sys.modules["CasambiBt"] = fake_casambi
    sys.path.insert(0, str(ROOT))
    try:
        # server.py shares serializable DTOs with the HA integration. Load only
        # those pure modules here; importing the integration package itself
        # would require a complete Home Assistant runtime.
        for package, package_path in (
            ("custom_components", ROOT / "custom_components"),
            ("custom_components.casambi_mqtt", ROOT / "custom_components/casambi_mqtt"),
            (
                "custom_components.casambi_mqtt.entities",
                ROOT / "custom_components/casambi_mqtt/entities",
            ),
        ):
            module = types.ModuleType(package)
            module.__path__ = [str(package_path)]
            sys.modules[package] = module
        for name, source in (
            (
                "custom_components.casambi_mqtt.const",
                ROOT / "custom_components/casambi_mqtt/const.py",
            ),
            (
                "custom_components.casambi_mqtt.entities.commands",
                ROOT / "custom_components/casambi_mqtt/entities/commands.py",
            ),
            (
                "custom_components.casambi_mqtt.entities.entities",
                ROOT / "custom_components/casambi_mqtt/entities/entities.py",
            ),
        ):
            spec = importlib.util.spec_from_file_location(name, source)
            assert spec and spec.loader
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)

        spec = importlib.util.spec_from_file_location(
            "casambi_server_test", ROOT / "server.py"
        )
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if previous is None:
            sys.modules.pop("CasambiBt", None)
        else:
            sys.modules["CasambiBt"] = previous


@pytest.fixture
def server():
    return load_server_module()


class SwitchEventType(Enum):
    PRESS = 0x01
    RELEASE = 0x02
    HOLD = 0x09
    RELEASE_AFTER_HOLD = 0x0C
    UNKNOWN = 0xFFFF


def switch_event(event_type=SwitchEventType.PRESS, **overrides):
    values = {
        "event": event_type,
        "button": 2,
        "unit_id": 7,
        "address": "AA:BB:CC:DD:EE:FF",
        "network_id": "private-network",
        "name": "Kitchen",
        "extra_data": b"secret payload",
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


@pytest.mark.asyncio
async def test_switch_event_publisher_uses_sanitized_non_retained_mqtt_contract(server):
    client = RecordingMqttClient()
    publisher = server.SwitchEventPublisher(client)

    await publisher.publish(switch_event())

    assert client.messages == [
        (
            "casambi/default/switch_events",
            {
                "payload": '{"unit_id":7,"button":2,"event":"PRESS"}',
                "qos": 1,
                "retain": False,
            },
        )
    ]


@pytest.mark.asyncio
async def test_switch_event_publisher_drops_duplicate_bursts_but_keeps_release(server):
    client = RecordingMqttClient()
    times = iter([10.0, 10.1, 10.2, 10.6])
    publisher = server.SwitchEventPublisher(client, monotonic=lambda: next(times))

    await publisher.publish(switch_event())
    await publisher.publish(switch_event())
    await publisher.publish(switch_event(SwitchEventType.RELEASE))
    await publisher.publish(switch_event())

    assert [json.loads(kwargs["payload"])["event"] for _, kwargs in client.messages] == [
        "PRESS",
        "RELEASE",
        "PRESS",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event",
    [
        types.SimpleNamespace(event=SwitchEventType.UNKNOWN, button=1, unit_id=2),
        types.SimpleNamespace(
            event=types.SimpleNamespace(name="PRIVATE_EVENT"), button=1, unit_id=2
        ),
        types.SimpleNamespace(event=SwitchEventType.PRESS, button=True, unit_id=2),
        types.SimpleNamespace(event=SwitchEventType.PRESS, button=1, unit_id="2"),
        object(),
    ],
)
async def test_switch_event_publisher_drops_unknown_or_malformed_events(
    server, event
):
    client = RecordingMqttClient()

    await server.SwitchEventPublisher(client).publish(event)

    assert client.messages == []


@pytest.mark.parametrize(
    "event_type",
    [
        SwitchEventType.PRESS,
        SwitchEventType.RELEASE,
        SwitchEventType.HOLD,
        SwitchEventType.RELEASE_AFTER_HOLD,
    ],
)
def test_switch_probe_emits_supported_event_enum_button_and_unit_id(
    server, event_type, capsys
):
    event = types.SimpleNamespace(
        event=event_type,
        button=2,
        unit_id=7,
        address="AA:BB:CC:DD:EE:FF",
        network_id="private-network",
        name="Kitchen",
        extra_data=b"secret payload",
    )

    server.emit_switch_event(event)

    assert json.loads(capsys.readouterr().out) == {
        "event": event_type.name,
        "button": 2,
        "unit_id": 7,
    }


@pytest.mark.parametrize(
    "event",
    [
        types.SimpleNamespace(
            event=SwitchEventType.UNKNOWN, button=1, unit_id=2
        ),
        types.SimpleNamespace(
            event=types.SimpleNamespace(name="PRIVATE_EVENT"), button=1, unit_id=2
        ),
        types.SimpleNamespace(event=SwitchEventType.PRESS, button=True, unit_id=2),
        types.SimpleNamespace(event=SwitchEventType.PRESS, button=1, unit_id="2"),
        object(),
    ],
)
def test_switch_probe_drops_unknown_or_malformed_events(server, event, capsys):
    server.emit_switch_event(event)

    assert capsys.readouterr().out == ""


@pytest.mark.asyncio
async def test_switch_probe_uses_bridge_connection_and_90_second_handler_lifecycle(
    server, monkeypatch
):
    calls = []
    device = types.SimpleNamespace(address="configured-address")

    async def discover():
        calls.append("discover")
        return [device]

    class FakeCasambi:
        async def connect(self, target, password):
            assert target is device
            assert password == "configured-password"
            calls.append("connect")

        def registerSwitchEventHandler(self, callback):
            assert callback is server.emit_switch_event
            calls.append("register")

        def unregisterSwitchEventHandler(self, callback):
            assert callback is server.emit_switch_event
            calls.append("unregister")

        async def disconnect(self):
            calls.append("disconnect")

    def casambi_factory():
        calls.append("construct-default-cache")
        return FakeCasambi()

    async def sleep(seconds):
        assert seconds == 90
        calls.append("sleep")

    monkeypatch.setattr(server, "NETWORK_ADDRESS", device.address)
    monkeypatch.setattr(server, "NETWORK_PASSWORD", "configured-password")
    monkeypatch.setattr(server, "discover", discover)
    monkeypatch.setattr(server, "Casambi", casambi_factory)
    monkeypatch.setattr(
        server.aiomqtt,
        "Client",
        lambda *args, **kwargs: pytest.fail("probe must not construct an MQTT client"),
    )

    await server.run_switch_event_probe(sleep=sleep)

    assert calls == [
        "discover",
        "construct-default-cache",
        "connect",
        "register",
        "sleep",
        "unregister",
        "disconnect",
    ]


@pytest.mark.asyncio
async def test_switch_probe_unregisters_and_disconnects_when_listening_fails(
    server, monkeypatch
):
    calls = []
    device = types.SimpleNamespace(address="private-address")

    class FakeCasambi:
        async def connect(self, target, password):
            calls.append("connect")

        def registerSwitchEventHandler(self, callback):
            calls.append("register")

        def unregisterSwitchEventHandler(self, callback):
            calls.append("unregister")

        async def disconnect(self):
            calls.append("disconnect")

    async def discover():
        return [device]

    async def failing_sleep(seconds):
        calls.append("sleep")
        raise RuntimeError("private-address secret payload")

    monkeypatch.setattr(server, "NETWORK_ADDRESS", device.address)
    monkeypatch.setattr(server, "discover", discover)
    monkeypatch.setattr(server, "Casambi", FakeCasambi)

    with pytest.raises(RuntimeError):
        await server.run_switch_event_probe(sleep=failing_sleep)

    assert calls == ["connect", "register", "sleep", "unregister", "disconnect"]


def test_switch_probe_cli_returns_only_fixed_sanitized_failure(
    server, monkeypatch, capsys
):
    async def fail_with_sensitive_details():
        raise RuntimeError("AA:BB:CC:DD:EE:FF Kitchen secret payload")

    async def bridge_must_not_run():
        pytest.fail("probe mode must not start the default bridge")

    monkeypatch.setattr(server, "run_switch_event_probe", fail_with_sensitive_details)
    monkeypatch.setattr(server, "main", bridge_must_not_run)

    assert server.cli(["switch-event-probe"]) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "casambi switch probe: failed\n"
    assert "AA:BB:CC:DD:EE:FF" not in captured.err
    assert "Kitchen" not in captured.err
    assert "secret payload" not in captured.err


def test_cli_without_probe_mode_runs_default_bridge_unchanged(server, monkeypatch):
    calls = []

    async def bridge_main():
        calls.append("bridge")

    async def probe_must_not_run():
        pytest.fail("default invocation must not start probe mode")

    monkeypatch.setattr(server, "main", bridge_main)
    monkeypatch.setattr(server, "run_switch_event_probe", probe_must_not_run)

    assert server.cli([]) == 0
    assert calls == ["bridge"]


def test_shared_device_selection_preserves_default_bridge_last_match(server, monkeypatch):
    first = types.SimpleNamespace(address="configured")
    other = types.SimpleNamespace(address="other")
    last = types.SimpleNamespace(address="configured")
    monkeypatch.setattr(server, "NETWORK_ADDRESS", "configured")

    assert server.configured_network_device([first, other, last]) is last


class FakeUnitType:
    id = 1
    manufacturer = "Casambi"
    mode = "Dim"
    model = "Test dimmer"
    stateLength = 1
    controls = []


class FakeUnitState:
    def __init__(self, dimmer):
        self.dimmer = dimmer


class FakeUnit:
    def __init__(self, address, name, dimmer):
        self.address = address
        self.deviceId = 1
        self.is_on = dimmer > 0
        self.name = name
        self.online = True
        self.state = FakeUnitState(dimmer)
        self.uuid = f"uuid-{address}"
        self.unitType = FakeUnitType()


class FakeScene:
    def __init__(self, scene_id, name):
        self.sceneId = scene_id
        self.name = name


class FakeCasa:
    def __init__(self):
        self.units = [FakeUnit("unit-a", "A", 0), FakeUnit("unit-b", "B", 128)]
        self.scenes = [FakeScene(1, "Scene one"), FakeScene(2, "Scene two")]
        self.set_level_calls = []

    async def setLevel(self, unit, value):
        self.set_level_calls.append((unit, value))


def command_message(payload: bytes) -> types.SimpleNamespace:
    return types.SimpleNamespace(payload=payload, topic="casambi/default/commands")


class RecordingMqttClient:
    def __init__(self):
        self.messages = []
        self.in_flight = 0
        self.max_in_flight = 0

    async def publish(self, topic, **kwargs):
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        await asyncio.sleep(0)
        self.messages.append((topic, kwargs))
        self.in_flight -= 1


class EmptyMessageStream:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class FailingMessageStream(EmptyMessageStream):
    async def __anext__(self):
        raise RuntimeError("mqtt session lost")


class BridgeMqttClient(RecordingMqttClient):
    def __init__(self, stream=None):
        super().__init__()
        self.incoming_messages = stream or EmptyMessageStream()
        self.subscriptions = []

    @property
    def messages(self):
        return self.incoming_messages

    @messages.setter
    def messages(self, value):
        self.published_messages = value

    async def publish(self, topic, **kwargs):
        self.published_messages.append((topic, kwargs))

    async def subscribe(self, topic):
        self.subscriptions.append(topic)


class LifecycleCasa(FakeCasa):
    def __init__(self):
        super().__init__()
        self.lifecycle = []

    def registerUnitChangedHandler(self, callback):
        self.lifecycle.append(("register-unit", callback))

    def unregisterUnitChangedHandler(self, callback):
        self.lifecycle.append(("unregister-unit", callback))

    def registerSwitchEventHandler(self, callback):
        self.lifecycle.append(("register-switch", callback))

    def unregisterSwitchEventHandler(self, callback):
        self.lifecycle.append(("unregister-switch", callback))


@pytest.mark.asyncio
async def test_connected_bridge_registers_and_cleans_up_both_callback_types(server):
    casa = LifecycleCasa()
    client = BridgeMqttClient()

    await server.run_connected_bridge(casa, client)

    assert client.subscriptions == ["casambi/default/commands"]
    assert [entry[0] for entry in casa.lifecycle] == [
        "register-unit",
        "register-switch",
        "unregister-switch",
        "unregister-unit",
    ]
    assert casa.lifecycle[0][1] is casa.lifecycle[3][1]
    assert casa.lifecycle[1][1] is casa.lifecycle[2][1]
    assert isinstance(casa.lifecycle[1][1], server.SwitchEventPublisher)
    assert [topic for topic, _ in client.published_messages] == [
        "casambi/default/events/",
        "casambi/default/events/unit-a",
        "casambi/default/events/unit-b",
        "casambi/default/scenes/1",
        "casambi/default/scenes/2",
    ]


@pytest.mark.asyncio
async def test_connected_bridge_unregisters_before_reconnect_after_failure(server):
    casa = LifecycleCasa()

    with pytest.raises(RuntimeError, match="mqtt session lost"):
        await server.run_connected_bridge(casa, BridgeMqttClient(FailingMessageStream()))
    await server.run_connected_bridge(casa, BridgeMqttClient())

    assert [entry[0] for entry in casa.lifecycle] == [
        "register-unit",
        "register-switch",
        "unregister-switch",
        "unregister-unit",
        "register-unit",
        "register-switch",
        "unregister-switch",
        "unregister-unit",
    ]


@pytest.mark.asyncio
async def test_publish_entities_publishes_units_and_scenes_sequentially(server):
    client = RecordingMqttClient()

    published = await server.publish_entities(FakeCasa(), client)

    assert published == (2, 2)
    assert client.max_in_flight == 1
    assert [topic for topic, _ in client.messages] == [
        "casambi/default/events/",
        "casambi/default/events/unit-a",
        "casambi/default/events/unit-b",
        "casambi/default/scenes/1",
        "casambi/default/scenes/2",
    ]
    assert client.messages[0][1] == {"payload": b"", "qos": 1, "retain": True}
    assert all(
        kwargs["qos"] == 1 and kwargs["retain"] is True for _, kwargs in client.messages
    )


@pytest.mark.asyncio
async def test_publish_entities_handles_missing_unit_state(server):
    unit = FakeUnit("unit-no-state", "Missing state", 0)
    unit.state = None
    casa = FakeCasa()
    casa.units = [unit]
    casa.scenes = []
    client = RecordingMqttClient()

    await server.publish_entities(casa, client)

    assert client.messages[1][0] == "casambi/default/events/unit-no-state"
    assert '"dimmer": null' in client.messages[1][1]["payload"]


@pytest.mark.asyncio
async def test_process_command_ignores_malformed_payload(server):
    client = RecordingMqttClient()
    message = command_message(b"{not-json")

    await server.process_command(message, FakeCasa(), client)

    assert client.messages == []


@pytest.mark.asyncio
async def test_addressless_units_publish_to_distinct_uuid_topics(server):
    first = FakeUnit("", "First", 10)
    first.uuid = "uuid/first"
    second = FakeUnit("", "Second", 20)
    second.uuid = "uuid second"
    casa = FakeCasa()
    casa.units = [first, second]
    casa.scenes = []
    client = RecordingMqttClient()

    published = await server.publish_entities(casa, client)

    assert published == (2, 0)
    assert [topic for topic, _ in client.messages] == [
        "casambi/default/events/",
        "casambi/default/events/uuid/uuid%2Ffirst",
        "casambi/default/events/uuid/uuid%20second",
    ]
    assert all(
        '"address": ""' in payload["payload"] for _, payload in client.messages[1:]
    )


@pytest.mark.asyncio
async def test_set_level_resolves_addressless_unit_by_uuid(server):
    first = FakeUnit("", "First", 0)
    first.uuid = "uuid-first"
    second = FakeUnit("", "Second", 0)
    second.uuid = "uuid-second"
    casa = FakeCasa()
    casa.units = [first, second]

    message = types.SimpleNamespace(
        payload=b'{"action":"SET_LEVEL","address":"","unit_uuid":"uuid-second","value":128}'
    )
    await server.process_command(message, casa, RecordingMqttClient())

    assert casa.set_level_calls == [(second, 128)]


@pytest.mark.asyncio
async def test_set_level_rejects_uuid_address_mismatch(server):
    unit = FakeUnit("real-address", "A", 0)
    unit.uuid = "uuid-a"
    casa = FakeCasa()
    casa.units = [unit]

    message = types.SimpleNamespace(
        payload=b'{"action":"SET_LEVEL","address":"wrong-address","unit_uuid":"uuid-a","value":128}'
    )
    await server.process_command(message, casa, RecordingMqttClient())

    assert casa.set_level_calls == []


@pytest.mark.asyncio
async def test_duplicate_addressless_uuid_is_never_published_or_commanded(server):
    first = FakeUnit("", "First", 10)
    second = FakeUnit("", "Second", 20)
    first.uuid = second.uuid = "duplicate"
    casa = FakeCasa()
    casa.units = [first, second]
    casa.scenes = []
    client = RecordingMqttClient()

    assert await server.publish_entities(casa, client) == (0, 0)
    assert [topic for topic, _ in client.messages] == ["casambi/default/events/"]

    message = types.SimpleNamespace(
        payload=b'{"action":"SET_LEVEL","address":"","unit_uuid":"duplicate","value":128}'
    )
    await server.process_command(message, casa, client)
    assert casa.set_level_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [b"[]", b"null", b'"text"'])
async def test_process_command_ignores_non_object_json(server, payload):
    casa = FakeCasa()
    await server.process_command(
        types.SimpleNamespace(payload=payload), casa, RecordingMqttClient()
    )
    assert casa.set_level_calls == []


@pytest.mark.asyncio
async def test_process_command_ignores_unexpected_topic(server):
    casa = FakeCasa()
    message = command_message(b'{"action":"SET_LEVEL","address":"unit-a","value":128}')
    message.topic = "casambi/other/commands"
    await server.process_command(message, casa, RecordingMqttClient())
    assert casa.set_level_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        b'{"action":"SET_LEVEL","address":"unit-a"}',
        b'{"action":"SET_LEVEL","address":"unit-a","value":"128"}',
        b'{"action":"SET_LEVEL","address":"unit-a","value":256}',
        b'{"action":"SET_LEVEL","address":"unit-a","value":1,"extra":true}',
        b'{"action":"SET_LEVEL","address":"","value":1}',
        b'{"action":"TURN_ON"}',
        b'{"action":"SET_SCENE"}',
    ],
)
async def test_process_command_ignores_invalid_command_schema(server, payload):
    casa = FakeCasa()
    await server.process_command(
        types.SimpleNamespace(payload=payload), casa, RecordingMqttClient()
    )
    assert casa.set_level_calls == []
