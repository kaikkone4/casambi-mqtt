"""
Reliability-hardening tests: logging routing, BLE disconnect lifecycle,
bounded publication concurrency, and bridge-owned diagnostics/privacy.

Kept separate from test_server.py per the hardening spec, at the cost of a
small amount of duplicated test infrastructure (module loader, fake Casambi
doubles) so the two suites do not depend on each other's internals.
"""

import asyncio
import importlib.util
import io
import logging
import re
import sys
import types
from pathlib import Path

import aiomqtt
import aiomqtt.client
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
            "casambi_bridge_lifecycle_test", ROOT / "server.py"
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


@pytest.fixture(autouse=True)
def _clean_third_party_loggers():
    """
    Prevent one test's configure_logging() from polluting another's.

    Snapshots/restores the *actual* logger aiomqtt routes through --
    aiomqtt.client.MQTT_LOGGER, named "mqtt" -- not a logger merely named
    "aiomqtt" (nothing in the dependency chain ever logs to that name).
    Paho shares this same "mqtt" logger via Client.enable_logger(), so this
    also covers paho's records.

    Also resets logging.disable(): the switch-event-probe CLI path
    (pre-existing, unrelated to this hardening work) calls
    logging.disable(logging.CRITICAL) as a probe-mode privacy measure and
    never re-enables it, since in production the process exits afterwards.
    In a shared test process that permanently silences every logger for the
    rest of the session unless undone here.
    """
    logging.disable(logging.NOTSET)
    mqtt_logger = aiomqtt.client.MQTT_LOGGER
    casambi_logger = logging.getLogger("CasambiBt")
    saved = {
        "mqtt_handlers": list(mqtt_logger.handlers),
        "mqtt_level": mqtt_logger.level,
        "mqtt_propagate": mqtt_logger.propagate,
        "casambi_handlers": list(casambi_logger.handlers),
        "casambi_filters": list(casambi_logger.filters),
        "casambi_level": casambi_logger.level,
        "casambi_propagate": casambi_logger.propagate,
    }
    yield
    logging.disable(logging.NOTSET)
    mqtt_logger.handlers = saved["mqtt_handlers"]
    mqtt_logger.level = saved["mqtt_level"]
    mqtt_logger.propagate = saved["mqtt_propagate"]
    casambi_logger.handlers = saved["casambi_handlers"]
    casambi_logger.filters = saved["casambi_filters"]
    casambi_logger.level = saved["casambi_level"]
    casambi_logger.propagate = saved["casambi_propagate"]


# ---------------------------------------------------------------------------
# Shared fakes
# ---------------------------------------------------------------------------


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
    def __init__(self, address, name="Unit", dimmer=0, uuid=None):
        self.address = address
        self.deviceId = 1
        self.is_on = dimmer > 0
        self.name = name
        self.online = True
        self.state = FakeUnitState(dimmer)
        self.uuid = uuid if uuid is not None else f"uuid-{address}"
        self.unitType = FakeUnitType()


def make_unit(address, *, name="Unit", dimmer=0, uuid=None):
    return FakeUnit(address, name=name, dimmer=dimmer, uuid=uuid)


def make_switch_event(button=0, unit_id=1, event_name="PRESS"):
    return types.SimpleNamespace(
        event=types.SimpleNamespace(name=event_name),
        button=button % 256,
        unit_id=unit_id % 256,
    )


class DisconnectCasa:
    """Fake Casambi connection tracking all three handler kinds."""

    def __init__(self):
        self.units = []
        self.scenes = []
        self.lifecycle = []
        self._unit_cb = None
        self._switch_cb = None
        self._disconnect_cb = None

    def registerUnitChangedHandler(self, callback):
        self.lifecycle.append(("register-unit", callback))
        self._unit_cb = callback

    def unregisterUnitChangedHandler(self, callback):
        self.lifecycle.append(("unregister-unit", callback))

    def registerSwitchEventHandler(self, callback):
        self.lifecycle.append(("register-switch", callback))
        self._switch_cb = callback

    def unregisterSwitchEventHandler(self, callback):
        self.lifecycle.append(("unregister-switch", callback))

    def registerDisconnectCallback(self, callback):
        self.lifecycle.append(("register-disconnect", callback))
        self._disconnect_cb = callback

    def unregisterDisconnectCallback(self, callback):
        self.lifecycle.append(("unregister-disconnect", callback))


class EmptyMessageStream:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class BlockingMessageStream:
    """Never yields a message; simulates awaiting messages indefinitely."""

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.Future()


class BridgeMqttClient:
    def __init__(self, stream=None):
        self.incoming_messages = stream or EmptyMessageStream()
        self.published_messages = []
        self.subscriptions = []

    @property
    def messages(self):
        return self.incoming_messages

    async def publish(self, topic, **kwargs):
        self.published_messages.append((topic, kwargs))

    async def subscribe(self, topic):
        self.subscriptions.append(topic)


class TrackingClient:
    """Records publishes, tracking concurrent in-flight calls."""

    def __init__(self, gate=None):
        self.calls = []
        self.in_flight = 0
        self.max_in_flight = 0
        self.gate = gate

    async def publish(self, topic, **kwargs):
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        if self.gate is not None:
            await self.gate
        else:
            await asyncio.sleep(0)
        self.calls.append((topic, kwargs))
        self.in_flight -= 1


async def wait_until(predicate, *, attempts=500):
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0)
    pytest.fail("condition not met in time")


THIRTY_SECOND_HANDLER_ATTEMPTS = 50


async def wait_for_registration(casa, *, attribute="_disconnect_cb"):
    for _ in range(THIRTY_SECOND_HANDLER_ATTEMPTS):
        if getattr(casa, attribute) is not None:
            return
        await asyncio.sleep(0)
    pytest.fail("handler was never registered")


# ---------------------------------------------------------------------------
# A. Logging routing
# ---------------------------------------------------------------------------


class _Capture(logging.Handler):
    """
    A handler appended *after* configure_logging(), on the same logger.

    Python's logging dispatches to a logger's handlers in insertion order,
    and filtering mutates the record in place -- so a capture handler added
    after the real (possibly pre-existing, possibly this test's own)
    filtering handler always observes the already-redacted record,
    regardless of which specific handler instance did the filtering. This
    sidesteps needing to know exactly which handler object is "the" real
    one when tests run alongside a suite that may have already configured
    logging once for the process.
    """

    def __init__(self, fmt="[%(name)s] %(message)s"):
        super().__init__()
        self.setFormatter(logging.Formatter(fmt))
        self.lines = []

    def emit(self, record):
        self.lines.append(self.format(record))


def test_configure_logging_redacts_casambi_bt_hex_and_digits(server):
    server.configure_logging()
    capture = _Capture()
    logging.getLogger("CasambiBt").addHandler(capture)
    try:
        logging.getLogger("CasambiBt._client").error(
            "Invalid signature for packet b'deadbeef1234'!"
        )
    finally:
        logging.getLogger("CasambiBt").removeHandler(capture)
    output = "\n".join(capture.lines)
    # The formatted line legitimately carries the logger name (letters); only
    # the message portion after "] " must be digit-free and byte-free.
    message_part = output.split("] ", 1)[-1]
    assert "deadbeef" not in message_part
    assert not re.search(r"\d", message_part)
    assert "[CasambiBt._client]" in output


def test_configure_logging_drops_traceback_and_redacts_address(server):
    server.configure_logging()
    capture = _Capture()
    logging.getLogger("CasambiBt").addHandler(capture)
    try:
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            logging.getLogger("CasambiBt").error(
                "Failed to connect to AA:BB:CC:DD:EE:FF.", exc_info=True
            )
    finally:
        logging.getLogger("CasambiBt").removeHandler(capture)
    output = "\n".join(capture.lines)
    assert "AA:BB" not in output
    assert "Traceback" not in output
    assert "boom" not in output


def test_configure_logging_redacts_continuation_lines_entirely(server):
    """
    CasambiBt/_network.py:228 logs
    f"Update failed: {res.status_code}\n{res.text}" -- an HTTP error body
    from the Casambi cloud on its own line after the status code. The
    character allowlist alone keeps ordinary letters, so an error body like
    {"error":"invalid session","network":"<name>"} would survive redaction
    as plain words, leaking a network name. Only the first line is kept.
    """
    server.configure_logging()
    capture = _Capture()
    logging.getLogger("CasambiBt").addHandler(capture)
    try:
        logging.getLogger("CasambiBt._network").error(
            "Update failed: 502\n"
            '{"error":"invalid session","network":"Villa Meridian Estate"}'
        )
    finally:
        logging.getLogger("CasambiBt").removeHandler(capture)
    output = "\n".join(capture.lines)
    assert "Villa Meridian Estate" not in output
    assert "Villa" not in output
    assert "Meridian" not in output


def test_configure_logging_routes_the_real_mqtt_logger_aiomqtt_uses(server):
    """
    aiomqtt.Client, constructed without an explicit logger= kwarg (as the
    bridge does), defaults to aiomqtt.client.MQTT_LOGGER --
    logging.getLogger("mqtt"), not a logger literally named "aiomqtt". Assert
    identity against the real object so a future aiomqtt rename fails this
    test instead of silently regressing back to "nothing routes anywhere".
    """
    server.configure_logging()
    assert aiomqtt.client.MQTT_LOGGER.handlers == [server.handler]
    assert aiomqtt.client.MQTT_LOGGER.propagate is False


def test_configure_logging_routes_pending_publish_warning_unredacted(server):
    server.configure_logging()
    capture = _Capture()
    aiomqtt.client.MQTT_LOGGER.addHandler(capture)
    try:
        aiomqtt.client.MQTT_LOGGER.warning(
            "There are %d pending publish calls.", 48
        )
    finally:
        aiomqtt.client.MQTT_LOGGER.removeHandler(capture)
    output = "\n".join(capture.lines)
    assert "48" in output
    assert "[mqtt]" in output


def test_configure_logging_suppresses_paho_debug_topic_logging(server):
    """
    aiomqtt.Client.__init__ calls self._client.enable_logger(logger) with
    that same MQTT_LOGGER, so paho shares it. Paho's own WARNING/ERROR sites
    carry no topics or payloads (audited: only an unrecognised-command byte),
    so the logger is safe to leave unfiltered -- but at DEBUG, paho logs the
    full topic and byte count for every publish
    ("Sending PUBLISH ... '%s' ... (%d bytes)"). The WARNING level
    configure_logging() sets is what keeps that out; this is a regression
    test for that level, using a topic-shaped payload so it would actually
    catch the level being loosened.
    """
    server.configure_logging()
    capture = _Capture()
    aiomqtt.client.MQTT_LOGGER.addHandler(capture)
    try:
        aiomqtt.client.MQTT_LOGGER.debug(
            "Sending PUBLISH (d0, q1, r0, m4), "
            "'casambi/default/events/AA:BB:CC:DD:EE:FF', ... (12 bytes)"
        )
    finally:
        aiomqtt.client.MQTT_LOGGER.removeHandler(capture)
    assert capture.lines == []


def test_configure_logging_suppresses_casambi_bt_info_and_debug(server):
    server.configure_logging()
    capture = _Capture()
    logging.getLogger("CasambiBt").addHandler(capture)
    try:
        logger = logging.getLogger("CasambiBt")
        logger.info("Registered switch event handler x")
        logger.debug("Removed disconnect callback x")
    finally:
        logging.getLogger("CasambiBt").removeHandler(capture)
    assert capture.lines == []


def test_configure_logging_is_idempotent(server):
    before = list(logging.getLogger("CasambiBt").handlers)
    server.configure_logging()
    after_first = list(logging.getLogger("CasambiBt").handlers)
    server.configure_logging()
    after_second = list(logging.getLogger("CasambiBt").handlers)
    assert len(after_first) == len(before) + 1
    assert after_second == after_first


def test_configure_logging_still_routes_mqtt_logger_if_casambi_bt_already_has_a_handler(
    server,
):
    """
    Regression for guarding idempotency on casambi_logger.handlers being
    non-empty: if anything else ever attaches a handler to "CasambiBt"
    first, that guard would trip and the mqtt/paho logger would silently
    never get configured. The guard must be independent of that logger's
    handler list.
    """
    foreign = logging.NullHandler()
    logging.getLogger("CasambiBt").addHandler(foreign)
    try:
        server.configure_logging()
    finally:
        logging.getLogger("CasambiBt").removeHandler(foreign)
    assert aiomqtt.client.MQTT_LOGGER.handlers == [server.handler]


# ---------------------------------------------------------------------------
# B. Disconnect lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unexpected_ble_disconnect_raises_and_cleans_up(server, caplog):
    caplog.set_level(logging.WARNING)
    casa = DisconnectCasa()
    client = BridgeMqttClient(BlockingMessageStream())
    baseline_tasks = asyncio.all_tasks()

    bridge_task = asyncio.create_task(server.run_connected_bridge(casa, client))
    await wait_for_registration(casa)

    casa._disconnect_cb()

    with pytest.raises(server.CasambiDisconnected):
        await bridge_task

    assert [entry[0] for entry in casa.lifecycle] == [
        "register-unit",
        "register-switch",
        "register-disconnect",
        "unregister-disconnect",
        "unregister-switch",
        "unregister-unit",
    ]

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert warnings[0].getMessage() == "Casambi BLE connection lost; bridge will restart"
    assert not re.search(r"\d", warnings[0].getMessage())

    assert asyncio.all_tasks() - baseline_tasks == set()


def test_cli_returns_exit_code_when_ble_disconnect_propagates(server, monkeypatch):
    async def main_raises_disconnect():
        raise server.CasambiDisconnected

    monkeypatch.setattr(server, "main", main_raises_disconnect)

    assert server.EXIT_BLE_DISCONNECTED == 75
    assert server.cli([]) == server.EXIT_BLE_DISCONNECTED


@pytest.mark.asyncio
async def test_cancelling_bridge_task_is_clean_shutdown_with_no_warning(server, caplog):
    caplog.set_level(logging.WARNING)
    casa = DisconnectCasa()
    client = BridgeMqttClient(BlockingMessageStream())
    baseline_tasks = asyncio.all_tasks()

    bridge_task = asyncio.create_task(server.run_connected_bridge(casa, client))
    await wait_for_registration(casa)

    bridge_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await bridge_task

    assert [entry[0] for entry in casa.lifecycle] == [
        "register-unit",
        "register-switch",
        "register-disconnect",
        "unregister-disconnect",
        "unregister-switch",
        "unregister-unit",
    ]
    assert [r for r in caplog.records if r.levelno == logging.WARNING] == []
    assert asyncio.all_tasks() - baseline_tasks == set()


# ---------------------------------------------------------------------------
# C. Bounded publication
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unit_publisher_bounds_concurrency_across_48_distinct_units(server):
    client = TrackingClient()
    publisher = server.UnitStatePublisher(client)
    await publisher.start()

    units = [make_unit(f"unit-{i:02d}") for i in range(48)]
    for unit in units:
        publisher.submit(unit)

    await wait_until(lambda: len(client.calls) == 48)

    assert client.max_in_flight == 1
    assert sorted(topic for topic, _ in client.calls) == sorted(
        server.unit_event_topic(u) for u in units
    )

    baseline_tasks = asyncio.all_tasks()
    await publisher.aclose(drain_timeout=0.2)
    assert asyncio.all_tasks() - baseline_tasks == set()


@pytest.mark.asyncio
async def test_unit_publisher_coalesces_48_updates_to_the_same_unit(server):
    client = TrackingClient()
    publisher = server.UnitStatePublisher(client)
    await publisher.start()

    snapshots = [make_unit("unit-x", dimmer=level) for level in range(48)]
    for snapshot in snapshots:
        publisher.submit(snapshot)

    await wait_until(lambda: len(client.calls) >= 1)
    await asyncio.sleep(0.02)

    assert len(client.calls) == 1
    payload = client.calls[0][1]["payload"]
    assert '"dimmer": 47' in payload

    await publisher.aclose(drain_timeout=0.2)


@pytest.mark.asyncio
async def test_unit_publisher_stays_bounded_during_slow_broker_ack(server):
    gate = asyncio.get_running_loop().create_future()
    client = TrackingClient(gate=gate)
    exceptions = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: exceptions.append(context))
    try:
        publisher = server.UnitStatePublisher(client)
        await publisher.start()

        publisher.submit(make_unit("unit-slow", dimmer=0))
        await asyncio.sleep(0)
        assert client.in_flight == 1

        for level in range(1, 50):
            publisher.submit(make_unit("unit-slow", dimmer=level))
            await asyncio.sleep(0)
            assert client.in_flight <= 1
            assert len(publisher._pending) <= 1  # noqa: SLF001

        assert client.max_in_flight == 1

        gate.set_result(None)
        await wait_until(lambda: len(client.calls) >= 1)
        await asyncio.sleep(0.02)

        await publisher.aclose(drain_timeout=0.2)
        assert exceptions == []
    finally:
        loop.set_exception_handler(previous_handler)


@pytest.mark.asyncio
async def test_unit_publisher_survives_mqtt_and_runtime_publish_failures(server, caplog):
    caplog.set_level(logging.WARNING)

    class FlakyClient:
        def __init__(self):
            self.calls = []

        async def publish(self, topic, **kwargs):
            if "mqtt-fail" in topic:
                raise aiomqtt.MqttError("boom")
            if "runtime-fail" in topic:
                raise RuntimeError("boom")
            self.calls.append((topic, kwargs))

    client = FlakyClient()
    publisher = server.UnitStatePublisher(client)
    await publisher.start()

    publisher.submit(make_unit("unit-mqtt-fail"))
    await wait_until(lambda: any(r.levelno == logging.WARNING for r in caplog.records))

    publisher.submit(make_unit("unit-runtime-fail"))
    publisher.submit(make_unit("unit-ok"))
    await wait_until(lambda: len(client.calls) == 1)

    assert publisher.failed == 1
    await publisher.aclose(drain_timeout=0.2)


@pytest.mark.asyncio
async def test_switch_publisher_drops_and_warns_once_when_queue_full(server, caplog):
    caplog.set_level(logging.WARNING)
    client = BridgeMqttClient()
    publisher = server.SwitchEventPublisher(client)
    await publisher.start()

    overflow = 10
    total = server.SWITCH_EVENT_QUEUE_MAX + overflow
    for i in range(total):
        publisher(make_switch_event(button=i))

    assert publisher.dropped == overflow
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    # Bridge-owned line: fixed text plus an integer count only -- no event
    # fields (button/unit_id/event name never appear).
    assert warnings[0].getMessage() == (
        "Dropping Casambi switch events due to a full queue: 1"
    )

    await wait_until(lambda: len(client.published_messages) > 0)
    await publisher.aclose(drain_timeout=0.2)


# ---------------------------------------------------------------------------
# D. Diagnostics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_heartbeat_warns_on_loop_lag_with_integer_milliseconds_only(server, caplog):
    caplog.set_level(logging.WARNING)
    diagnostics = server.BridgeDiagnostics()

    monotonic_values = iter([0.0, 6.5])

    def monotonic():
        return next(monotonic_values)

    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 2:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await server.run_heartbeat(diagnostics, sleep=fake_sleep, monotonic=monotonic)

    assert sleep_calls == [server.HEARTBEAT_SECONDS, server.HEARTBEAT_SECONDS]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert warnings[0].getMessage() == "Event loop lag detected: 1500 ms"
    assert diagnostics.loop_lag_ms_max == 1500


@pytest.mark.asyncio
async def test_heartbeat_emits_periodic_diagnostics_info_line(server, caplog):
    caplog.set_level(logging.INFO)
    diagnostics = server.BridgeDiagnostics()
    diagnostics.unit_publish_confirmed = 12
    diagnostics.ble_disconnects = 2

    monotonic_values = iter([0.0, 5.0, 305.0])

    def monotonic():
        return next(monotonic_values)

    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)
        if len(sleep_calls) >= 3:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await server.run_heartbeat(diagnostics, sleep=fake_sleep, monotonic=monotonic)

    info_records = [r for r in caplog.records if r.levelno == logging.INFO]
    assert len(info_records) == 1
    message = info_records[0].getMessage()
    assert "unit_publish_confirmed=12" in message
    assert "ble_disconnects=2" in message
    assert not re.search(r"casambi|AA:BB", message, re.IGNORECASE)


@pytest.mark.asyncio
async def test_bridge_owned_and_redacted_logging_never_leaks_private_data(
    server, caplog, monkeypatch
):
    caplog.set_level(logging.DEBUG)
    server.configure_logging()

    fake_network_name = "Villa-Meridian-Private-Network"
    fake_unit_name = "Kitchen Pendant Over Island"
    fake_uuid = "9f2c6b7acafebabe1234-secret-unit-uuid"
    fake_mac = "AA:BB:CC:DD:EE:FF"
    monkeypatch.setattr(server, "NETWORK_NAME", fake_network_name)

    class Capture(logging.Handler):
        def __init__(self):
            super().__init__()
            self.lines = []

        def emit(self, record):
            self.lines.append(self.format(record))

    casambi_capture = Capture()
    casambi_capture.setFormatter(logging.Formatter("%(message)s"))
    casambi_logger = logging.getLogger("CasambiBt")
    casambi_logger.addHandler(casambi_capture)
    try:
        # (a) CasambiBt redaction: this is the realistic threat per the audit
        # -- raw bytes/hex and addresses interpolated into f-strings.
        casambi_logger.error(
            f"Invalid signature for packet b'deadbeefcafebabe{fake_mac.replace(':', '')}'!"
        )
        try:
            raise RuntimeError(f"unit {fake_mac} lost")
        except RuntimeError:
            casambi_logger.error("Unkown connection failure.", exc_info=True)

        # (b) bridge-owned code paths: prove the fixed-text-plus-integers
        # design actually holds when the data flowing through it is hot.
        caplog.clear()
        casa = DisconnectCasa()
        casa.units = [
            make_unit(fake_mac, name=fake_unit_name, uuid=fake_uuid, dimmer=10)
        ]
        client = BridgeMqttClient(BlockingMessageStream())

        bridge_task = asyncio.create_task(server.run_connected_bridge(casa, client))
        await wait_for_registration(casa)
        casa._unit_cb(
            make_unit(fake_mac, name=fake_unit_name, uuid=fake_uuid, dimmer=200)
        )
        casa._disconnect_cb()
        with pytest.raises(server.CasambiDisconnected):
            await bridge_task

        switch_publisher = server.SwitchEventPublisher(client)
        for i in range(server.SWITCH_EVENT_QUEUE_MAX + 3):
            switch_publisher(make_switch_event(button=i))

        diagnostics = server.BridgeDiagnostics()
        unit_publisher = server.UnitStatePublisher(client)

        blobs = list(casambi_capture.lines)
        blobs += [r.getMessage() for r in caplog.records]
        blobs.append(repr(server.CasambiDisconnected()))
        blobs.append(str(server.CasambiDisconnected()))
        blobs.append(repr(switch_publisher))
        blobs.append(repr(unit_publisher))
        blobs.append(repr(diagnostics))
        blobs.append(str(diagnostics))

        markers = [
            "casambi/",
            "AA:BB",
            fake_network_name,
            fake_unit_name,
            fake_uuid,
            "b'",
            "password",
            "secret",
        ]
        for blob in blobs:
            lowered = blob.lower()
            for marker in markers:
                assert marker.lower() not in lowered, (marker, blob)
            assert not re.search(r"[0-9a-f]{8,}", blob), blob
    finally:
        casambi_logger.removeHandler(casambi_capture)
