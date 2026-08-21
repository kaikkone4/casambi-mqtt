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
import json
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


async def wait_for_registration(casa, *, attribute="_switch_cb"):
    """
    Poll (bounded, no fixed sleep duration) until a handler is registered.

    Defaults to "_switch_cb" -- the last handler session_body registers --
    rather than "_disconnect_cb": the BLE disconnect callback is owned and
    registered by main() now, not by run_connected_bridge, so casa's
    _disconnect_cb attribute is never set by these tests at all.
    """
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
async def test_ble_disconnect_during_command_loop_raises_and_cleans_up(server, caplog):
    """
    disconnected is now owned by main(): run_connected_bridge only receives
    an already-existing event and races the whole session against it. This
    test drives that event directly (no more casa.registerDisconnectCallback
    indirection, since run_connected_bridge no longer touches it at all).
    """
    caplog.set_level(logging.WARNING)
    casa = DisconnectCasa()
    client = BridgeMqttClient(BlockingMessageStream())
    disconnected = asyncio.Event()
    baseline_tasks = asyncio.all_tasks()

    bridge_task = asyncio.create_task(
        server.run_connected_bridge(casa, client, disconnected)
    )
    await wait_for_registration(casa)

    disconnected.set()

    with pytest.raises(server.CasambiDisconnected):
        await bridge_task

    assert [entry[0] for entry in casa.lifecycle] == [
        "register-unit",
        "register-switch",
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
    disconnected = asyncio.Event()
    baseline_tasks = asyncio.all_tasks()

    bridge_task = asyncio.create_task(
        server.run_connected_bridge(casa, client, disconnected)
    )
    await wait_for_registration(casa)

    bridge_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await bridge_task

    assert [entry[0] for entry in casa.lifecycle] == [
        "register-unit",
        "register-switch",
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
async def test_unit_publisher_survives_publish_failures_and_worker_keeps_going(
    server, caplog, monkeypatch
):
    """
    Both failure kinds (aiomqtt.MqttError and a generic exception) must be
    survived by the worker, counted truthfully, and -- since blocker 3 now
    requeues on failure -- eventually succeed once the transient failure
    clears, proving the retry path is genuinely self-healing rather than
    just "logged and dropped".
    """
    monkeypatch.setattr(server, "PUBLISH_RETRY_SECONDS", 0.01)
    caplog.set_level(logging.WARNING)

    class FlakyOnceClient:
        """Each named failure topic fails exactly once, then succeeds."""

        def __init__(self):
            self.calls = []
            self._failed_once = set()

        async def publish(self, topic, **kwargs):
            if topic not in self._failed_once and (
                "mqtt-fail" in topic or "runtime-fail" in topic
            ):
                self._failed_once.add(topic)
                if "mqtt-fail" in topic:
                    raise aiomqtt.MqttError("boom")
                raise RuntimeError("boom")
            self.calls.append((topic, kwargs))

    client = FlakyOnceClient()
    diagnostics = server.BridgeDiagnostics()
    publisher = server.UnitStatePublisher(client, diagnostics=diagnostics)
    await publisher.start()

    publisher.submit(make_unit("unit-mqtt-fail"))
    await wait_until(lambda: any(r.levelno == logging.WARNING for r in caplog.records))
    # A publish failure -- MqttError or otherwise -- must be counted
    # truthfully as failed, never as confirmed, per blocker 3.
    assert diagnostics.unit_publish_confirmed == 0
    assert diagnostics.unit_publish_failed >= 1

    publisher.submit(make_unit("unit-runtime-fail"))
    publisher.submit(make_unit("unit-ok"))
    await wait_until(lambda: len(client.calls) == 3, attempts=5000)

    assert publisher.failed >= 2
    assert sorted(topic for topic, _ in client.calls) == sorted(
        [
            server.unit_event_topic(make_unit("unit-mqtt-fail")),
            server.unit_event_topic(make_unit("unit-runtime-fail")),
            server.unit_event_topic(make_unit("unit-ok")),
        ]
    )
    await publisher.aclose(drain_timeout=0.5)


@pytest.mark.asyncio
async def test_unit_publisher_requeues_failed_publish_without_losing_newest_state(
    server, monkeypatch
):
    """
    Regression for blocker 3: publish_unit's suppress_errors=False path lets
    aiomqtt.MqttError reach the worker, which must requeue the unit rather
    than lose it -- but a fresher value submitted while the failed publish
    was still in flight must survive untouched by the stale requeue.
    """
    monkeypatch.setattr(server, "PUBLISH_RETRY_SECONDS", 0.01)

    class FlakyThenOkClient:
        def __init__(self):
            self.calls = []
            self.should_fail = True
            self.publisher = None  # bound after construction

        async def publish(self, topic, **kwargs):
            if self.should_fail:
                self.should_fail = False
                # Simulate a newer update landing while this attempt is
                # in flight and about to fail.
                self.publisher.submit(make_unit("unit-retry", dimmer=99))
                raise aiomqtt.MqttError("boom")
            self.calls.append((topic, kwargs))

    client = FlakyThenOkClient()
    diagnostics = server.BridgeDiagnostics()
    publisher = server.UnitStatePublisher(client, diagnostics=diagnostics)
    client.publisher = publisher
    await publisher.start()

    publisher.submit(make_unit("unit-retry", dimmer=1))

    await wait_until(lambda: diagnostics.unit_publish_failed >= 1)
    assert diagnostics.unit_publish_confirmed == 0
    assert len(client.calls) == 0
    # The newer value (dimmer=99), submitted mid-failure, must survive; the
    # requeued stale value (dimmer=1) must NOT overwrite it.
    assert len(publisher._pending) == 1  # noqa: SLF001
    pending_unit = next(iter(publisher._pending.values()))  # noqa: SLF001
    assert pending_unit.state.dimmer == 99

    await wait_until(lambda: len(client.calls) == 1, attempts=3000)
    payload = json.loads(client.calls[0][1]["payload"])
    assert payload["state"]["dimmer"] == 99
    assert diagnostics.unit_publish_confirmed == 1

    await publisher.aclose(drain_timeout=0.5)


@pytest.mark.asyncio
async def test_unit_publisher_backs_off_instead_of_spinning_on_dead_broker(
    server, monkeypatch
):
    """A dead broker must produce at most one publish attempt per second."""
    monkeypatch.setattr(server, "PUBLISH_RETRY_SECONDS", 0.05)

    class AlwaysFailingClient:
        def __init__(self):
            self.attempts = 0

        async def publish(self, topic, **kwargs):
            self.attempts += 1
            raise aiomqtt.MqttError("dead broker")

    client = AlwaysFailingClient()
    publisher = server.UnitStatePublisher(client)
    await publisher.start()

    publisher.submit(make_unit("unit-a"))
    publisher.submit(make_unit("unit-b"))

    await wait_until(lambda: client.attempts >= 1)
    # Give the worker a bounded window in which, absent the backoff, it
    # would have spun through both (and re-retried) many times over.
    await asyncio.sleep(0.12)
    # At 0.05s backoff and ~0.12s elapsed, at most a handful of attempts
    # are possible -- nowhere near a tight spin loop.
    assert client.attempts <= 5

    await publisher.aclose(drain_timeout=0.2)


@pytest.mark.asyncio
async def test_unit_publisher_fifo_drain_does_not_starve_a_cold_unit(server):
    """
    Regression for blocker 4: a hot unit resubmitted on every publish must
    not starve a cold unit that was already pending -- FIFO drain plus
    in-place coalescing (submit() never deletes-then-reinserts) guarantees
    the cold unit, submitted first, is drained before the hot unit's
    churn can get ahead of it.
    """
    calls = []

    class ResubmittingClient:
        def __init__(self):
            self.publisher = None  # bound after construction

        async def publish(self, topic, **kwargs):
            await asyncio.sleep(0)
            calls.append((topic, kwargs))
            if "hot" in topic:
                self.publisher.submit(make_unit("hot", dimmer=len(calls)))

    client = ResubmittingClient()
    publisher = server.UnitStatePublisher(client)
    client.publisher = publisher
    await publisher.start()

    publisher.submit(make_unit("cold", dimmer=1))
    publisher.submit(make_unit("hot", dimmer=1))

    await wait_until(lambda: any("cold" in topic for topic, _ in calls), attempts=500)

    cold_turn_index = next(i for i, (topic, _) in enumerate(calls) if "cold" in topic)
    assert cold_turn_index < 2
    assert len([c for c in calls if "cold" in c[0]]) == 1

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
        disconnected = asyncio.Event()

        bridge_task = asyncio.create_task(
            server.run_connected_bridge(casa, client, disconnected)
        )
        await wait_for_registration(casa)
        casa._unit_cb(
            make_unit(fake_mac, name=fake_unit_name, uuid=fake_uuid, dimmer=200)
        )
        disconnected.set()
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


# ---------------------------------------------------------------------------
# A2. Command logging privacy (round 2 -- blocker 1)
# ---------------------------------------------------------------------------


class MultiMessageStream:
    """Yields each message in order, once, then stops -- no blocking."""

    def __init__(self, messages):
        self._iterator = iter(messages)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._iterator)
        except StopIteration:
            raise StopAsyncIteration from None


@pytest.mark.asyncio
async def test_bridge_command_handling_never_logs_topic_payload_or_raw_action(
    server, caplog, monkeypatch
):
    """
    Regression for blocker 1: consume_commands used to LOGGER.debug() the
    full decoded MQTT payload and topic (network name, unit address/UUID,
    scene id, brightness). That call is gone, replaced by an integer
    counter. This also exercises the two sibling leaks in process_command:
    an unrecognized action string must never be echoed back, and a caught
    exception must be logged by its type name, never str(error).

    Privacy must not be level-dependent, so caplog is captured at DEBUG.
    """
    caplog.set_level(logging.DEBUG)
    fake_network = "Secret-Vacation-House-Network"
    monkeypatch.setattr(server, "NETWORK_NAME", fake_network)

    fake_uuid = "9f2c6b7acafebabe1234-secret-unit-uuid"
    fake_mac = "AA:BB:CC:DD:EE:FF"
    scene_id = 424242
    brightness = 137
    unknown_action = "SECRETACTIONMARKER"

    topic = f"casambi/{fake_network}/commands"
    messages = [
        types.SimpleNamespace(
            topic=topic,
            payload=json.dumps(
                {
                    "action": "SET_LEVEL",
                    "address": fake_mac,
                    "unit_uuid": fake_uuid,
                    "value": brightness,
                }
            ).encode(),
        ),
        types.SimpleNamespace(
            topic=topic,
            payload=json.dumps({"action": "SET_SCENE", "scene_id": scene_id}).encode(),
        ),
        types.SimpleNamespace(
            topic=topic,
            payload=json.dumps({"action": unknown_action}).encode(),
        ),
    ]

    # casa.units/scenes are empty, so SET_LEVEL and SET_SCENE both hit the
    # except clause in process_command (ValueError / StopIteration) --
    # exercising the "type(error).__name__, not str(error)" fix too.
    casa = DisconnectCasa()
    client = BridgeMqttClient(MultiMessageStream(messages))
    disconnected = asyncio.Event()

    await server.run_connected_bridge(casa, client, disconnected)

    markers = [
        fake_network,
        fake_uuid,
        fake_mac,
        str(scene_id),
        str(brightness),
        unknown_action,
        topic,
    ]
    blobs = [r.getMessage() for r in caplog.records]
    blobs.append(repr(server.BridgeDiagnostics()))
    for blob in blobs:
        for marker in markers:
            assert marker not in blob, (marker, blob)


# ---------------------------------------------------------------------------
# B2. Disconnect lifecycle ownership and precedence (round 2 -- blocker 2)
# ---------------------------------------------------------------------------


class FakeMainCasa:
    """Fake Casambi connection for testing main()'s own disconnect handling."""

    def __init__(self):
        self.calls = []
        self._disconnect_cb = None

    async def connect(self, device, password):
        self.calls.append("connect")

    def registerDisconnectCallback(self, callback):
        self.calls.append("register-disconnect")
        self._disconnect_cb = callback

    def unregisterDisconnectCallback(self, callback):
        self.calls.append("unregister-disconnect")

    async def disconnect(self):
        self.calls.append("disconnect")


class FakeMainClientCtx:
    """Minimal async-context-manager stand-in for aiomqtt.Client."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


@pytest.mark.asyncio
async def test_disconnect_during_baseline_snapshot_aborts_and_raises(server, caplog):
    """
    Required test 1: a BLE drop during publish_entities' baseline snapshot
    must abort it and surface as CasambiDisconnected, not hang or complete
    normally -- proving run_connected_bridge races the WHOLE session body,
    not just the command loop.

    Deterministic: the fake client blocks forever on its first publish
    call, and only once the test has *confirmed* (via an Event) that the
    session task is genuinely stuck there does it fire the disconnect --
    no sleeps, no scheduling luck.
    """
    caplog.set_level(logging.WARNING)
    disconnected = asyncio.Event()
    casa = DisconnectCasa()
    casa.units = [make_unit("unit-a"), make_unit("unit-b")]
    casa.scenes = []
    reached_publish = asyncio.Event()

    class BlockingFirstPublishClient(BridgeMqttClient):
        def __init__(self):
            super().__init__()
            self.publish_calls = 0

        async def publish(self, topic, **kwargs):
            self.publish_calls += 1
            if self.publish_calls == 1:
                reached_publish.set()
                await asyncio.Future()  # blocks until cancelled
            await super().publish(topic, **kwargs)

    client = BlockingFirstPublishClient()
    bridge_task = asyncio.create_task(
        server.run_connected_bridge(casa, client, disconnected)
    )

    await asyncio.wait_for(reached_publish.wait(), timeout=5.0)
    # session_task is now deterministically blocked inside the very first
    # publish_entities call -- the snapshot has started but not finished.
    disconnected.set()

    with pytest.raises(server.CasambiDisconnected):
        await bridge_task

    # Aborted before any handler registration -- proves the abort happened
    # during the snapshot, not later during the command loop.
    assert casa.lifecycle == []
    assert client.publish_calls == 1
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert warnings[0].getMessage() == "Casambi BLE connection lost; bridge will restart"


@pytest.mark.asyncio
async def test_main_raises_disconnected_without_mqtt_connect_if_set_at_startup(
    server, monkeypatch
):
    """
    Required test 2: if the BLE link is already gone by the time main()
    would otherwise start an MQTT session, it must raise CasambiDisconnected
    without ever constructing an aiomqtt.Client or entering
    run_connected_bridge -- a BLE loss must never be attempted as an MQTT
    connect in the first place.
    """
    device = types.SimpleNamespace(address="configured-address")

    async def discover():
        return [device]

    casa = FakeMainCasa()

    def register_and_fire_immediately(callback):
        casa.calls.append("register-disconnect")
        casa._disconnect_cb = callback
        callback()  # schedules disconnected.set() via call_soon_threadsafe;
        # main()'s own post-registration yield (await sleep(0)) observes it.

    casa.registerDisconnectCallback = register_and_fire_immediately

    def client_must_not_be_constructed(*args, **kwargs):
        pytest.fail("must not construct an MQTT client when already disconnected")

    async def run_connected_bridge_must_not_run(*args, **kwargs):
        pytest.fail("must not enter a session when already disconnected")

    monkeypatch.setattr(server, "NETWORK_ADDRESS", device.address)
    monkeypatch.setattr(server, "discover", discover)
    monkeypatch.setattr(server, "create_casambi_connection", lambda: casa)
    monkeypatch.setattr(server.aiomqtt, "Client", client_must_not_be_constructed)
    monkeypatch.setattr(server, "run_connected_bridge", run_connected_bridge_must_not_run)

    with pytest.raises(server.CasambiDisconnected):
        await server.main()

    assert casa.calls == [
        "connect",
        "register-disconnect",
        "unregister-disconnect",
        "disconnect",
    ]


@pytest.mark.asyncio
async def test_race_precedence_disconnect_wins_when_both_complete_same_tick(server):
    """
    Required test 3: when asyncio.wait() returns with BOTH the session task
    (having raised aiomqtt.MqttError) and the disconnect task already done
    in the same tick -- exactly what a BLE-induced MQTT teardown looks like
    -- the disconnect must win. Deterministic: both tasks are explicitly
    awaited to real completion before the race function is ever called, so
    there is no ordering luck involved in reaching the "both done" case.
    """
    disconnected = asyncio.Event()
    disconnected.set()

    async def failing_session():
        raise aiomqtt.MqttError("boom")

    session_task = asyncio.create_task(failing_session())
    disconnect_task = asyncio.create_task(disconnected.wait())

    for _ in range(50):
        if session_task.done() and disconnect_task.done():
            break
        await asyncio.sleep(0)
    assert session_task.done()
    assert disconnect_task.done()

    with pytest.raises(server.CasambiDisconnected):
        await server._race_session_against_disconnect(session_task, disconnect_task)


@pytest.mark.asyncio
async def test_main_reconnects_normally_on_mqtt_error_when_not_disconnected(
    server, monkeypatch
):
    """
    Required test 4: an aiomqtt.MqttError with the disconnect event unset
    must still hit the ordinary reconnect path (warn + backoff + retry),
    completely unaffected by the new disconnect-precedence guard.
    """
    device = types.SimpleNamespace(address="configured-address")

    async def discover():
        return [device]

    casa = FakeMainCasa()
    attempts = []

    async def fake_run_connected_bridge(casa_arg, client_arg, disconnected_arg):
        attempts.append("run")
        if len(attempts) == 1:
            raise aiomqtt.MqttError("boom")
        raise asyncio.CancelledError  # deterministically end the loop

    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(server, "NETWORK_ADDRESS", device.address)
    monkeypatch.setattr(server, "discover", discover)
    monkeypatch.setattr(server, "create_casambi_connection", lambda: casa)
    monkeypatch.setattr(server.aiomqtt, "Client", lambda *a, **k: FakeMainClientCtx())
    monkeypatch.setattr(server, "run_connected_bridge", fake_run_connected_bridge)

    await server.main(sleep=fake_sleep)

    assert attempts == ["run", "run"]
    # sleeps[0] is the post-registration startup yield; sleeps[1] is the
    # 5-second reconnect backoff -- both real behaviour, no real delay here.
    assert sleeps == [0, 5]
    assert casa.calls == [
        "connect",
        "register-disconnect",
        "unregister-disconnect",
        "disconnect",
    ]
