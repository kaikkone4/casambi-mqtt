"""
Bridge side of the v1 health protocol (issue #26).

Every test here drives the bridge with fakes only: no broker, no radio, and a
Casambi double that fails the test if any control operation is reached.
"""

import asyncio
import json
import logging
import types

import aiomqtt
import pytest
from test_bridge_lifecycle import (
    BlockingMessageStream,
    BridgeMqttClient,
    DisconnectCasa,
    load_server_module,
    make_unit,
    wait_for_registration,
    wait_until,
)

STATUS_TOPIC = "casambi/default/status"
REPLY_TOPIC = "casambi/default/health/reply"
COMMAND_TOPIC = "casambi/default/commands"


@pytest.fixture
def server():
    return load_server_module()


@pytest.fixture
def hp(server):
    import sys

    return sys.modules["custom_components.casambi_mqtt.health_protocol"]


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now


class FakeScene:
    def __init__(self, scene_id, name="Scene"):
        self.sceneId = scene_id
        self.name = name


class NoActuationCasa(DisconnectCasa):
    """Casambi double that records any control operation as a test failure."""

    def __init__(self, units=None, scenes=None):
        super().__init__()
        self.units = units if units is not None else [make_unit("a"), make_unit("b")]
        self.scenes = scenes if scenes is not None else [FakeScene(1)]
        self.actuations = []

    async def setLevel(self, unit, value):
        self.actuations.append("setLevel")

    async def turnOn(self, unit):
        self.actuations.append("turnOn")

    async def switchToScene(self, scene):
        self.actuations.append("switchToScene")


def command(payload):
    if not isinstance(payload, bytes):
        payload = json.dumps(payload).encode()
    return types.SimpleNamespace(payload=payload, topic=COMMAND_TOPIC)


def replies(client, hp):
    return [
        hp.parse_reply(kwargs["payload"])
        for topic, kwargs in client.published_messages
        if topic == REPLY_TOPIC
    ]


def statuses(client, hp):
    return [
        hp.parse_status(kwargs["payload"])
        for topic, kwargs in client.published_messages
        if topic == STATUS_TOPIC
    ]


# ---------------------------------------------------------------------------
# Status contract
# ---------------------------------------------------------------------------


def test_status_is_versioned_and_monotonic_per_instance(server, hp):
    clock = FakeClock()
    health = server.BridgeHealth(monotonic=clock, wall_clock=lambda: "x")
    first = health.build_status()
    clock.now += 42.9
    second = health.build_status()

    assert first.protocol_version == hp.PROTOCOL_VERSION == 1
    assert hp.is_valid_instance_id(first.instance_id)
    assert (first.sequence, second.sequence) == (1, 2)
    assert second.uptime_s == 42
    assert first.phase == hp.PHASE_STARTING
    assert hp.parse_status(second.to_json()) == second


def test_new_process_never_inherits_previous_ready(server, hp):
    old = server.BridgeHealth()
    old.session_started()
    old.record_snapshot(server.SnapshotResult(units=3, scenes=1, failed=0))
    assert old.build_status().phase == hp.PHASE_READY

    new = server.BridgeHealth()
    status = new.build_status()

    assert new.instance_id != old.instance_id
    assert status.phase == hp.PHASE_STARTING
    assert status.sequence == 1
    assert status.snapshot.generation == 0


def test_last_will_is_offline_and_never_ready(server, hp):
    health = server.BridgeHealth()
    will = hp.parse_status(health.will_payload())

    assert will is not None
    assert will.phase == hp.PHASE_OFFLINE
    assert will.sequence is None
    assert will.error == hp.ERROR_CONNECTION_LOST
    assert will.instance_id == health.instance_id


def test_state_publish_streak_degrades_ready_status(server, hp):
    health = server.BridgeHealth()
    health.session_started()
    health.record_snapshot(server.SnapshotResult(units=1, scenes=0, failed=0))
    failing = [True]
    health.state_publish_failing = lambda: failing[0]

    status = health.build_status()
    assert (status.phase, status.error) == (
        hp.PHASE_DEGRADED,
        hp.ERROR_STATE_PUBLISH_FAILED,
    )
    failing[0] = False
    assert health.build_status().phase == hp.PHASE_READY


@pytest.mark.parametrize("value", ["4", "301", "abc", "", "30.5"])
def test_invalid_heartbeat_setting_is_rejected(server, monkeypatch, value):
    monkeypatch.setattr(server, "HEALTH_HEARTBEAT_SECONDS", value)
    with pytest.raises(RuntimeError, match="HEALTH_HEARTBEAT_SECONDS"):
        server.health_heartbeat_seconds()


def test_default_heartbeat_setting(server, monkeypatch):
    monkeypatch.setattr(server, "HEALTH_HEARTBEAT_SECONDS", "30")
    assert server.health_heartbeat_seconds() == 30


# ---------------------------------------------------------------------------
# GET_STATUS
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_status_replies_with_request_id_and_instance(server, hp):
    casa = NoActuationCasa()
    client = BridgeMqttClient()
    health = server.BridgeHealth()

    await server.process_command(
        command({"action": "GET_STATUS", "request_id": "probe-1"}),
        casa,
        client,
        health,
    )

    [reply] = replies(client, hp)
    assert reply.type == hp.REPLY_STATUS
    assert reply.request_id == "probe-1"
    assert reply.instance_id == health.instance_id
    assert reply.result == hp.RESULT_OK
    [(topic, kwargs)] = client.published_messages
    assert kwargs["retain"] is False
    assert kwargs["qos"] == 1
    assert casa.actuations == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"action": "GET_STATUS"},
        {"action": "GET_STATUS", "request_id": ""},
        {"action": "GET_STATUS", "request_id": "x" * 65},
        {"action": "GET_STATUS", "request_id": "has space"},
        {"action": "GET_STATUS", "request_id": "secret/topic#"},
        {"action": "GET_STATUS", "request_id": 7},
        {"action": "GET_STATUS", "request_id": None},
        {"action": "GET_STATUS", "request_id": "ok", "address": "a"},
        {"action": "PUBLISH_ENTITIES", "request_id": "ä"},
    ],
)
async def test_invalid_health_requests_get_no_reply_and_no_echo(
    server, hp, payload, caplog
):
    caplog.set_level(logging.DEBUG)
    casa = NoActuationCasa()
    client = BridgeMqttClient()

    await server.process_command(
        command(payload), casa, client, server.BridgeHealth()
    )

    assert client.published_messages == []
    assert casa.actuations == []
    for value in payload.values():
        if (
            isinstance(value, str)
            and len(value) > 2
            and value not in {"GET_STATUS", "PUBLISH_ENTITIES"}
        ):
            assert value not in caplog.text


@pytest.mark.asyncio
async def test_get_status_is_coalesced(server, hp):
    clock = FakeClock()
    health = server.BridgeHealth(monotonic=clock)
    client = BridgeMqttClient()
    casa = NoActuationCasa()

    for request_id in ("one", "two"):
        await server.process_command(
            command({"action": "GET_STATUS", "request_id": request_id}),
            casa,
            client,
            health,
        )
    clock.now += server.HEALTH_REPLY_MIN_INTERVAL
    await server.process_command(
        command({"action": "GET_STATUS", "request_id": "three"}), casa, client, health
    )

    assert [reply.request_id for reply in replies(client, hp)] == ["one", "three"]


@pytest.mark.asyncio
async def test_get_status_without_health_is_ignored(server, caplog):
    client = BridgeMqttClient()
    await server.process_command(
        command({"action": "GET_STATUS", "request_id": "x"}),
        NoActuationCasa(),
        client,
    )
    assert client.published_messages == []
    assert "bridge health is not enabled" in caplog.text


# ---------------------------------------------------------------------------
# Correlated snapshot
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_snapshot_probe_acks_only_after_every_publish(server, hp):
    casa = NoActuationCasa()
    client = BridgeMqttClient()
    health = server.BridgeHealth()

    await server.process_command(
        command({"action": "PUBLISH_ENTITIES", "request_id": "snap-1"}),
        casa,
        client,
        health,
    )

    topics = [topic for topic, _ in client.published_messages]
    assert topics == [
        "casambi/default/events/",
        "casambi/default/events/a",
        "casambi/default/events/b",
        "casambi/default/scenes/1",
        REPLY_TOPIC,
    ]
    [reply] = replies(client, hp)
    assert reply.type == hp.REPLY_SNAPSHOT
    assert reply.result == hp.RESULT_OK
    # The empty legacy tombstone is not a unit.
    assert (reply.units, reply.scenes, reply.failed) == (2, 1, 0)
    assert reply.generation == 1
    assert reply.status.phase == hp.PHASE_READY
    assert casa.actuations == []


class FailingTopicClient(BridgeMqttClient):
    def __init__(self, failing_topics):
        super().__init__()
        self.failing_topics = set(failing_topics)

    async def publish(self, topic, **kwargs):
        if topic in self.failing_topics:
            raise aiomqtt.MqttError("publish failed")
        await super().publish(topic, **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failing_topic", "expected"),
    [
        ("casambi/default/events/b", (1, 1, 1)),
        ("casambi/default/scenes/1", (2, 0, 1)),
        ("casambi/default/events/", (2, 1, 1)),
    ],
)
async def test_partial_snapshot_failure_never_acks_success(
    server, hp, failing_topic, expected
):
    client = FailingTopicClient({failing_topic})
    health = server.BridgeHealth()

    await server.process_command(
        command({"action": "PUBLISH_ENTITIES", "request_id": "snap-1"}),
        NoActuationCasa(),
        client,
        health,
    )

    [reply] = replies(client, hp)
    assert reply.result == hp.RESULT_FAILED
    assert (reply.units, reply.scenes, reply.failed) == expected
    assert reply.status.phase == hp.PHASE_DEGRADED
    assert reply.status.error == hp.ERROR_SNAPSHOT_PUBLISH_FAILED


@pytest.mark.asyncio
async def test_legacy_publish_entities_is_unchanged_with_health(server):
    client = BridgeMqttClient()
    health = server.BridgeHealth()

    await server.process_command(
        command({"action": "PUBLISH_ENTITIES"}), NoActuationCasa(), client, health
    )

    assert REPLY_TOPIC not in [topic for topic, _ in client.published_messages]
    assert len(client.published_messages) == 4
    # A legacy snapshot does not advance the probe generation.
    assert health.build_status().snapshot.generation == 0


@pytest.mark.asyncio
async def test_snapshot_probe_is_rate_limited(server, hp):
    clock = FakeClock()
    health = server.BridgeHealth(monotonic=clock)
    client = BridgeMqttClient()
    casa = NoActuationCasa()
    probe = {"action": "PUBLISH_ENTITIES", "request_id": "snap"}

    await server.process_command(command(probe), casa, client, health)
    published_after_first = len(client.published_messages)
    clock.now += 5
    await server.process_command(command(probe), casa, client, health)

    assert len(client.published_messages) == published_after_first + 1
    assert replies(client, hp)[-1].result == hp.RESULT_RATE_LIMITED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"action": "SET_LEVEL", "address": "a", "value": 1},
        {"action": "TURN_ON", "address": "a"},
        {"action": "SET_SCENE", "scene_id": 1},
        {"action": "PUBLISH_ENTITIES"},
    ],
)
async def test_legacy_commands_still_work_with_health_enabled(server, payload):
    casa = NoActuationCasa()
    client = BridgeMqttClient()

    await server.process_command(command(payload), casa, client, server.BridgeHealth())

    expected = {
        "SET_LEVEL": ["setLevel"],
        "TURN_ON": ["turnOn"],
        "SET_SCENE": ["switchToScene"],
        "PUBLISH_ENTITIES": [],
    }[payload["action"]]
    assert casa.actuations == expected


@pytest.mark.asyncio
async def test_legacy_commands_reject_a_request_id(server):
    casa = NoActuationCasa()
    await server.process_command(
        command({"action": "TURN_ON", "address": "a", "request_id": "x"}),
        casa,
        BridgeMqttClient(),
        server.BridgeHealth(),
    )
    assert casa.actuations == []


# ---------------------------------------------------------------------------
# Session lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_reports_starting_then_ready_and_answers_probes(server, hp):
    casa = NoActuationCasa()
    client = BridgeMqttClient(BlockingMessageStream())
    health = server.BridgeHealth()
    disconnected = asyncio.Event()

    bridge_task = asyncio.create_task(
        server.run_connected_bridge(casa, client, disconnected, health)
    )
    await wait_for_registration(casa)

    first_topic, first_kwargs = client.published_messages[0]
    assert first_topic == STATUS_TOPIC
    assert first_kwargs["retain"] is True
    assert hp.parse_status(first_kwargs["payload"]).phase == hp.PHASE_STARTING
    assert statuses(client, hp)[-1].phase == hp.PHASE_READY
    assert statuses(client, hp)[-1].snapshot.units == 2

    bridge_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await bridge_task
    final = statuses(client, hp)[-1]
    assert (final.phase, final.error) == (hp.PHASE_OFFLINE, hp.ERROR_SHUTDOWN)
    sequences = [status.sequence for status in statuses(client, hp)]
    assert sequences == sorted(sequences)
    assert len(set(sequences)) == len(sequences)
    assert casa.actuations == []


@pytest.mark.asyncio
async def test_heartbeat_continues_while_snapshot_is_stuck(server, hp):
    casa = NoActuationCasa()
    stuck = asyncio.Event()

    class StuckSnapshotClient(BridgeMqttClient):
        async def publish(self, topic, **kwargs):
            if topic.startswith("casambi/default/events"):
                stuck.set()
                await asyncio.Future()
            await super().publish(topic, **kwargs)

    client = StuckSnapshotClient(BlockingMessageStream())
    health = server.BridgeHealth()
    bridge_task = asyncio.create_task(
        server.run_connected_bridge(
            casa, client, asyncio.Event(), health, heartbeat_seconds=0.001
        )
    )
    await asyncio.wait_for(stuck.wait(), timeout=5)
    await wait_until(lambda: len(statuses(client, hp)) >= 3, attempts=5000)

    assert {status.phase for status in statuses(client, hp)} == {hp.PHASE_STARTING}
    bridge_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await bridge_task


@pytest.mark.asyncio
async def test_ble_disconnect_still_wins_and_reports_final_status(server, hp, caplog):
    caplog.set_level(logging.WARNING)
    casa = NoActuationCasa()
    client = BridgeMqttClient(BlockingMessageStream())
    health = server.BridgeHealth()
    disconnected = asyncio.Event()
    baseline_tasks = asyncio.all_tasks()

    bridge_task = asyncio.create_task(
        server.run_connected_bridge(casa, client, disconnected, health)
    )
    await wait_for_registration(casa)
    disconnected.set()

    with pytest.raises(server.CasambiDisconnected):
        await bridge_task

    final = statuses(client, hp)[-1]
    assert (final.phase, final.error, final.ble) == (
        hp.PHASE_DEGRADED,
        hp.ERROR_BLE_DISCONNECTED,
        hp.LINK_DISCONNECTED,
    )
    assert asyncio.all_tasks() - baseline_tasks == set()
    assert [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING] == [
        "Casambi BLE connection lost; bridge will restart"
    ]


@pytest.mark.asyncio
async def test_hanging_final_status_is_bounded_and_does_not_mask_disconnect(
    server, monkeypatch
):
    monkeypatch.setattr(server, "FINAL_STATUS_TIMEOUT", 0.01)
    casa = NoActuationCasa()
    disconnected = asyncio.Event()
    hang = asyncio.Event()

    class HangAfterDisconnectClient(BridgeMqttClient):
        async def publish(self, topic, **kwargs):
            if hang.is_set():
                await asyncio.Future()
            await super().publish(topic, **kwargs)

    client = HangAfterDisconnectClient(BlockingMessageStream())
    bridge_task = asyncio.create_task(
        server.run_connected_bridge(casa, client, disconnected, server.BridgeHealth())
    )
    await wait_for_registration(casa)
    hang.set()
    disconnected.set()

    with pytest.raises(server.CasambiDisconnected):
        await asyncio.wait_for(bridge_task, timeout=5)


@pytest.mark.asyncio
async def test_main_sets_retained_offline_last_will(server, hp, monkeypatch):
    device = types.SimpleNamespace(address="configured-address")
    created = {}

    async def discover():
        return [device]

    class Casa:
        async def connect(self, device, password):
            pass

        def registerDisconnectCallback(self, callback):
            pass

        def unregisterDisconnectCallback(self, callback):
            pass

        async def disconnect(self):
            pass

    class ClientCtx:
        def __init__(self, *args, **kwargs):
            created.update(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return False

    async def run_connected_bridge(casa, client, disconnected, health, **kwargs):
        created["health"] = health
        created["heartbeat_seconds"] = kwargs["heartbeat_seconds"]
        raise asyncio.CancelledError

    monkeypatch.setattr(server, "NETWORK_ADDRESS", device.address)
    monkeypatch.setattr(server, "discover", discover)
    monkeypatch.setattr(server, "create_casambi_connection", Casa)
    monkeypatch.setattr(server.aiomqtt, "Client", ClientCtx)
    monkeypatch.setattr(server, "run_connected_bridge", run_connected_bridge)

    await server.main()

    will = created["will"]
    assert will.topic == STATUS_TOPIC
    assert will.retain is True
    assert will.qos == 1
    status = hp.parse_status(will.payload)
    assert status.phase == hp.PHASE_OFFLINE
    assert status.instance_id == created["health"].instance_id
    assert created["heartbeat_seconds"] == 30


@pytest.mark.asyncio
async def test_health_logging_carries_no_request_or_network_data(server, caplog):
    caplog.set_level(logging.DEBUG)
    client = FailingTopicClient({STATUS_TOPIC})
    health = server.BridgeHealth()

    assert await health.publish_status(client) is False
    assert await health.publish_status(client) is False

    warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert warnings == ["Bridge status publish failed"]
    assert "default" not in caplog.text


@pytest.mark.parametrize("swallowed_by_main", [False, True])
def test_sigterm_is_a_clean_exit_in_any_phase(server, monkeypatch, swallowed_by_main):
    import os
    import signal

    cleaned = []

    async def main():
        try:
            asyncio.get_running_loop().call_soon(os.kill, os.getpid(), signal.SIGTERM)
            await asyncio.Future()
        except asyncio.CancelledError:
            cleaned.append("cleanup")
            if not swallowed_by_main:
                raise

    monkeypatch.setattr(server, "main", main)

    assert server.cli([]) == 0
    assert cleaned == ["cleanup"]
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL
