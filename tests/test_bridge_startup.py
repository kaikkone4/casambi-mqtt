"""
Bridge startup state machine, bounded retries and local health record (#27).

No radio, broker or systemd: discovery, connect and MQTT are fakes.
"""

import asyncio
import json
import logging
import sys
import types

import aiomqtt
import pytest
from test_bridge_lifecycle import load_server_module

DEVICE = types.SimpleNamespace(address="configured-address")


@pytest.fixture
def server(monkeypatch):
    module = load_server_module()
    monkeypatch.setattr(module, "NETWORK_ADDRESS", DEVICE.address)
    monkeypatch.setattr(module, "HEALTH_RECORD_PATH", None)
    return module


@pytest.fixture
def hp(server):
    return sys.modules["custom_components.casambi_mqtt.health_protocol"]


# Error shapes exactly as casambi-bt 0.3.2 / bleak produce them.
class BleakError(Exception):
    pass


class BleakDBusError(BleakError):
    def __init__(self, dbus_error, details):
        super().__init__(dbus_error, details)

    @property
    def dbus_error(self):
        return self.args[0]


class CasambiBtError(RuntimeError):
    pass


class BluetoothError(CasambiBtError):
    pass


class AuthenticationError(CasambiBtError):
    pass


class NetworkNotFoundError(CasambiBtError):
    pass


def wrapped_in_progress(details="Operation already in progress"):
    """casambi-bt: raise BluetoothError(e.dbus_error, e.dbus_error_details) from e."""
    try:
        try:
            raise BleakDBusError("org.bluez.Error.InProgress", details)
        except BleakDBusError as error:
            raise BluetoothError(error.dbus_error, details) from error
    except BluetoothError as wrapped:
        return wrapped


class FakeCasa:
    def __init__(self, connect=None):
        self.calls = []
        self._connect = connect

    async def connect(self, device, password):
        self.calls.append("connect")
        if self._connect is not None:
            await self._connect()

    def registerDisconnectCallback(self, callback):
        self.calls.append("register-disconnect")

    def unregisterDisconnectCallback(self, callback):
        self.calls.append("unregister-disconnect")

    async def disconnect(self):
        self.calls.append("disconnect")


class StopSession(Exception):
    pass


class RecordingClient:
    """aiomqtt.Client stand-in for the startup failure report."""

    instances = []

    def __init__(self, *args, **kwargs):
        self.published = []
        RecordingClient.instances.append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def publish(self, topic, **kwargs):
        self.published.append((topic, kwargs))


@pytest.fixture(autouse=True)
def _reset_clients():
    RecordingClient.instances = []


def patch_bridge(server, monkeypatch, *, discover, casa=None):
    casa = casa or FakeCasa()
    monkeypatch.setattr(server, "discover", discover)
    monkeypatch.setattr(server, "create_casambi_connection", lambda: casa)
    monkeypatch.setattr(server.aiomqtt, "Client", RecordingClient)
    monkeypatch.setattr(server.random, "random", lambda: 0.0)

    async def run_connected_bridge(*args, **kwargs):
        raise StopSession

    monkeypatch.setattr(server, "run_connected_bridge", run_connected_bridge)
    return casa


class Sleeps:
    def __init__(self):
        self.calls = []

    async def __call__(self, seconds):
        self.calls.append(seconds)
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "details", ["Operation already in progress", "Toiminto on jo käynnissä", ""]
)
def test_wrapped_in_progress_is_classified_by_dbus_code_not_text(server, hp, details):
    error = wrapped_in_progress(details)
    assert server.bluez_error_code(error) == "org.bluez.Error.InProgress"
    for stage in (hp.PHASE_DISCOVERING, hp.PHASE_CONNECTING):
        assert (
            server.classify_startup_error(error, stage)
            == hp.ERROR_BLE_DISCOVERY_IN_PROGRESS
        )


def test_in_progress_text_without_the_code_is_not_trusted(server, hp):
    error = BluetoothError("Operation already in progress")
    assert server.bluez_error_code(error) is None
    assert (
        server.classify_startup_error(error, hp.PHASE_DISCOVERING)
        == hp.ERROR_BLE_DISCOVERY_FAILED
    )


@pytest.mark.parametrize(
    ("error", "stage", "expected"),
    [
        (AuthenticationError("Login failed: 401"), "connecting", "configuration"),
        (NetworkNotFoundError(), "connecting", "network_not_found"),
        (BluetoothError(), "connecting", "ble_connect_failed"),
        (BleakError("x"), "discovering", "ble_discovery_failed"),
        (TimeoutError(), "discovering", "ble_discovery_failed"),
        (TimeoutError(), "connecting", "ble_connect_failed"),
        (ValueError("x"), "connecting", "unknown"),
    ],
)
def test_startup_errors_map_to_fixed_categories(server, error, stage, expected):
    assert server.classify_startup_error(error, stage) == expected


def test_backoff_schedule_is_bounded_with_small_jitter(server):
    assert [server.startup_backoff(n, lambda: 0.0) for n in range(7)] == [
        1,
        2,
        4,
        8,
        16,
        30,
        30,
    ]
    assert server.startup_backoff(4, lambda: 1.0) == pytest.approx(19.2)
    assert server.startup_backoff(10, lambda: 1.0) == pytest.approx(36)


# ---------------------------------------------------------------------------
# Startup state machine
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_repeated_in_progress_ends_in_controlled_failure(
    server, hp, monkeypatch, caplog
):
    attempts = []

    async def discover():
        attempts.append("scan")
        raise wrapped_in_progress()

    casa = patch_bridge(server, monkeypatch, discover=discover)
    sleeps = Sleeps()
    health = server.BridgeHealth()

    with pytest.raises(server.StartupFailedError) as failure:
        await server.main(sleep=sleeps, health=health)

    assert failure.value.category == hp.ERROR_BLE_DISCOVERY_IN_PROGRESS
    assert len(attempts) == server.STARTUP_ATTEMPTS == 6
    assert sleeps.calls == [1, 2, 4, 8, 16]
    assert casa.calls == []  # never tried to connect
    assert (health.phase, health.error) == (
        hp.PHASE_OFFLINE,
        hp.ERROR_BLE_DISCOVERY_IN_PROGRESS,
    )
    # The failure is visible to Home Assistant although no session existed.
    [client] = RecordingClient.instances
    [(topic, kwargs)] = client.published
    reported = hp.parse_status(kwargs["payload"])
    assert topic == "casambi/default/status"
    assert (reported.phase, reported.error) == (
        hp.PHASE_OFFLINE,
        hp.ERROR_BLE_DISCOVERY_IN_PROGRESS,
    )
    assert "configured-address" not in caplog.text
    assert "already in progress" not in caplog.text


def test_cli_exit_codes_for_startup_and_configuration_failures(server, monkeypatch):
    async def startup_failure():
        raise server.StartupFailedError("ble_discovery_in_progress")

    async def auth_failure():
        raise server.StartupFailedError("configuration")

    async def bad_setting():
        raise server.BridgeConfigurationError("HEALTH_HEARTBEAT_SECONDS")

    for main, expected in (
        (startup_failure, server.EXIT_BLE_STARTUP_FAILED),
        (auth_failure, server.EXIT_CONFIGURATION),
        (bad_setting, server.EXIT_CONFIGURATION),
    ):
        monkeypatch.setattr(server, "main", main)
        assert server.cli([]) == expected


@pytest.mark.asyncio
async def test_transient_failure_then_success_continues(server, hp, monkeypatch):
    results = [wrapped_in_progress(), [DEVICE]]

    async def discover():
        result = results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    casa = patch_bridge(server, monkeypatch, discover=discover)
    sleeps = Sleeps()

    with pytest.raises(StopSession):
        await server.main(sleep=sleeps, health=server.BridgeHealth())

    assert sleeps.calls == [1, 0]  # one backoff, then main's startup yield
    assert casa.calls[:2] == ["connect", "register-disconnect"]
    assert RecordingClient.instances  # the normal session client


@pytest.mark.asyncio
async def test_configured_network_missing_is_retried_then_reported(
    server, hp, monkeypatch
):
    async def discover():
        return [types.SimpleNamespace(address="someone-else")]

    patch_bridge(server, monkeypatch, discover=discover)
    with pytest.raises(server.StartupFailedError) as failure:
        await server.main(sleep=Sleeps(), health=server.BridgeHealth())
    assert failure.value.category == hp.ERROR_NETWORK_NOT_FOUND


@pytest.mark.asyncio
async def test_discovery_hang_is_bounded_and_leaves_no_task(server, hp, monkeypatch):
    monkeypatch.setattr(server, "DISCOVERY_DEADLINE_SECONDS", 0.01)
    started = []

    async def discover():
        started.append(1)
        await asyncio.Future()

    patch_bridge(server, monkeypatch, discover=discover)
    baseline = asyncio.all_tasks()

    with pytest.raises(server.StartupFailedError) as failure:
        await server.main(sleep=Sleeps(), health=server.BridgeHealth())

    assert failure.value.category == hp.ERROR_BLE_DISCOVERY_FAILED
    assert len(started) == 6
    assert asyncio.all_tasks() - baseline == set()


@pytest.mark.asyncio
async def test_connect_hang_is_bounded_and_cleaned_up_between_attempts(
    server, hp, monkeypatch
):
    monkeypatch.setattr(server, "CONNECT_DEADLINE_SECONDS", 0.01)

    async def discover():
        return [DEVICE]

    async def hang():
        await asyncio.Future()

    casa = patch_bridge(server, monkeypatch, discover=discover, casa=FakeCasa(hang))
    baseline = asyncio.all_tasks()

    with pytest.raises(server.StartupFailedError) as failure:
        await server.main(sleep=Sleeps(), health=server.BridgeHealth())

    assert failure.value.category == hp.ERROR_BLE_CONNECT_FAILED
    # connect, cleanup, connect, cleanup, ... and the final teardown.
    assert casa.calls == ["connect", "disconnect"] * 6 + ["disconnect"]
    assert asyncio.all_tasks() - baseline == set()


@pytest.mark.asyncio
async def test_authentication_failure_is_not_retried(server, hp, monkeypatch):
    async def discover():
        return [DEVICE]

    async def reject():
        raise AuthenticationError("Login failed")

    casa = patch_bridge(server, monkeypatch, discover=discover, casa=FakeCasa(reject))
    sleeps = Sleeps()

    with pytest.raises(server.StartupFailedError) as failure:
        await server.main(sleep=sleeps, health=server.BridgeHealth())

    assert failure.value.category == hp.ERROR_CONFIGURATION
    assert casa.calls.count("connect") == 1
    assert sleeps.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("where", ["discover", "backoff", "connect"])
async def test_cancellation_in_every_startup_phase_is_clean(
    server, hp, monkeypatch, where
):
    reached = asyncio.Event()

    async def discover():
        if where == "discover":
            reached.set()
            await asyncio.Future()
        if where == "backoff":
            raise wrapped_in_progress()
        return [DEVICE]

    async def connect():
        reached.set()
        await asyncio.Future()

    async def sleep(seconds):
        if where == "backoff":
            reached.set()
            await asyncio.Future()

    casa = patch_bridge(server, monkeypatch, discover=discover, casa=FakeCasa(connect))
    health = server.BridgeHealth()
    baseline = asyncio.all_tasks()
    task = asyncio.create_task(server.main(sleep=sleep, health=health))
    await asyncio.wait_for(reached.wait(), timeout=5)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert (health.phase, health.error) == (hp.PHASE_OFFLINE, hp.ERROR_SHUTDOWN)
    assert asyncio.all_tasks() - baseline == set()
    assert RecordingClient.instances == []  # a shutdown is not reported as failure
    if where == "connect":
        assert casa.calls[-1] == "disconnect"


@pytest.mark.asyncio
async def test_hanging_broker_cannot_delay_failed_startup_exit(server, monkeypatch):
    monkeypatch.setattr(server, "STARTUP_REPORT_TIMEOUT", 0.01)

    class HangingClient(RecordingClient):
        async def __aenter__(self):
            await asyncio.Future()

    monkeypatch.setattr(server.aiomqtt, "Client", HangingClient)
    health = server.BridgeHealth()
    health.set_phase("offline", "ble_discovery_in_progress")

    await asyncio.wait_for(server.report_startup_failure(health), timeout=5)


@pytest.mark.asyncio
async def test_failed_startup_report_survives_broker_errors(server, monkeypatch):
    class FailingClient(RecordingClient):
        async def __aenter__(self):
            raise aiomqtt.MqttError("refused")

    monkeypatch.setattr(server.aiomqtt, "Client", FailingClient)
    await server.report_startup_failure(server.BridgeHealth())


# ---------------------------------------------------------------------------
# Local health record
# ---------------------------------------------------------------------------


def test_health_record_is_atomic_identified_and_private(server, hp, tmp_path):
    path = tmp_path / "health.json"
    record = server.HealthRecord(str(path), monotonic=lambda: 123.5)
    health = server.BridgeHealth(record=record, monotonic=lambda: 123.5)

    health.set_phase(hp.PHASE_DISCOVERING, hp.ERROR_BLE_DISCOVERY_IN_PROGRESS)
    data = json.loads(path.read_text())

    assert data["record_version"] == 1
    assert data["instance_id"] == health.instance_id
    assert data["pid"] == server.os.getpid()
    assert data["start_ticks"] == server.process_start_ticks()
    assert data["boot_id"] == server.current_boot_id()
    assert (data["phase"], data["error"]) == ("discovering", "ble_discovery_in_progress")
    assert data["updated_monotonic"] == 123.5
    assert data["ever_ready"] is False
    assert list(tmp_path.iterdir()) == [path]  # no temporary file left behind
    assert "configured-address" not in path.read_text()

    health.session_started()
    health.record_snapshot(server.SnapshotResult(units=1, scenes=0, failed=0))
    data = json.loads(path.read_text())
    assert data["phase"] == "ready"
    assert data["ever_ready"] is True
    assert data["ready_since_monotonic"] == 123.5


def test_health_record_write_failure_is_logged_once(server, tmp_path, caplog):
    record = server.HealthRecord(str(tmp_path / "missing-dir" / "health.json"))
    health = server.BridgeHealth(record=record)
    caplog.set_level(logging.WARNING)

    health.set_phase("discovering")
    health.set_phase("connecting")

    assert caplog.text.count("Unable to write the local bridge health record") == 1


def test_process_start_ticks_handles_spaces_and_parens_in_comm(server, monkeypatch):
    stat = "42 (we ird) name) S " + " ".join(str(n) for n in range(4, 30))
    monkeypatch.setattr(server, "_read_text", lambda path: stat)
    # Field 22 overall is the 20th field after the command name.
    assert server.process_start_ticks(42) == 22
