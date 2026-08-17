import asyncio
import importlib.util
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load_server_module():
    """Load server.py without requiring the platform BLE dependency in CI."""
    fake_casambi = types.ModuleType("CasambiBt")
    for name in ("UnitControlType", "UnitControl", "UnitType", "UnitState", "Unit", "Scene"):
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
            ("custom_components.casambi_mqtt.entities", ROOT / "custom_components/casambi_mqtt/entities"),
        ):
            module = types.ModuleType(package)
            module.__path__ = [str(package_path)]
            sys.modules[package] = module
        for name, source in (
            ("custom_components.casambi_mqtt.entities.commands", ROOT / "custom_components/casambi_mqtt/entities/commands.py"),
            ("custom_components.casambi_mqtt.entities.entities", ROOT / "custom_components/casambi_mqtt/entities/entities.py"),
        ):
            spec = importlib.util.spec_from_file_location(name, source)
            assert spec and spec.loader
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)

        spec = importlib.util.spec_from_file_location("casambi_server_test", ROOT / "server.py")
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


@pytest.mark.asyncio
async def test_publish_entities_publishes_units_and_scenes_sequentially(server):
    client = RecordingMqttClient()

    published = await server.publish_entities(FakeCasa(), client)

    assert published == (2, 2)
    assert client.max_in_flight == 1
    assert [topic for topic, _ in client.messages] == [
        "casambi/default/events/unit-a",
        "casambi/default/events/unit-b",
        "casambi/default/scenes/1",
        "casambi/default/scenes/2",
    ]
    assert all(kwargs["qos"] == 1 and kwargs["retain"] is True for _, kwargs in client.messages)


@pytest.mark.asyncio
async def test_publish_entities_handles_missing_unit_state(server):
    unit = FakeUnit("unit-no-state", "Missing state", 0)
    unit.state = None
    casa = FakeCasa()
    casa.units = [unit]
    casa.scenes = []
    client = RecordingMqttClient()

    await server.publish_entities(casa, client)

    assert client.messages[0][0] == "casambi/default/events/unit-no-state"
    assert '"dimmer": null' in client.messages[0][1]["payload"]


@pytest.mark.asyncio
async def test_process_command_ignores_malformed_payload(server):
    client = RecordingMqttClient()
    message = types.SimpleNamespace(payload=b"{not-json")

    await server.process_command(message, FakeCasa(), client)

    assert client.messages == []
