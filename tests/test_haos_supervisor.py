"""HAOS controller tests: local files/processes only, never real MQTT or BLE."""

import importlib.util
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


def module():
    path = ROOT / "haos_casambi/bridge_supervisor.py"
    assert path.exists(), "HAOS supervisor is not implemented"
    spec = importlib.util.spec_from_file_location("haos_supervisor", path)
    result = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = result
    spec.loader.exec_module(result)
    return result


def test_budget_persists_across_restart_and_never_ready_resets(tmp_path):
    sup = module()
    for launch in range(5):
        with sup.RecoveryStore(tmp_path) as store:
            if launch < 4:
                assert store.charge()
                assert store.state["attempts"] == max(0, launch)
                store.mark_child({"pid": 99999999, "start_ticks": 1, "boot_id": "test"})
                store.child_ended()
            else:
                assert not store.charge()
                assert store.state["latched"]
                assert store.state["reason"] == "retry_exhausted"
    data = json.loads((tmp_path / "recovery/state.json").read_text())
    assert data["attempts"] == 3
    assert data["notification"]["pending"]["state"] == "failed"
    assert (tmp_path / "recovery").stat().st_mode & 0o777 == 0o700
    assert (tmp_path / "recovery/state.json").stat().st_mode & 0o777 == 0o600


def test_lock_reset_once_and_interrupted_intent(tmp_path):
    sup = module()
    with sup.RecoveryStore(tmp_path) as store:
        with pytest.raises(BlockingIOError):
            sup.RecoveryStore(tmp_path)
        assert store.charge()
    with sup.RecoveryStore(tmp_path) as store:
        store.reconcile()
        assert store.state["latched"]
        assert store.state["reason"] == "interrupted_spawn"
        incident = store.state["incident_id"]
        assert not store.rearm(0)
        assert not store.rearm(1)
        store.child_ended()  # This pure-policy fixture knows no child was spawned.
        assert store.rearm(1)
        assert store.state["incident_id"] != incident
        store.latch("configuration")
    with sup.RecoveryStore(tmp_path) as store:
        assert not store.rearm(1)
        assert store.state["latched"]
        assert not store.charge()
        assert store.rearm(2)
        assert store.charge()
        assert store.state["attempts"] == 0


@pytest.mark.parametrize("raw", ["{}", "null", "{", '{"schema_version": 2}'])
def test_corruption_fails_closed(tmp_path, raw):
    sup = module()
    directory = tmp_path / "recovery"
    directory.mkdir()
    (directory / "state.json").write_text(raw)
    with pytest.raises((ValueError, TypeError)):
        sup.RecoveryStore(tmp_path)


def test_failed_charge_does_not_authorize_launch(tmp_path, monkeypatch):
    sup = module()
    with sup.RecoveryStore(tmp_path) as store:

        def fail(*args):
            raise OSError("synthetic fsync failure")

        monkeypatch.setattr(sup.os, "fsync", fail)
        with pytest.raises(OSError):
            store.charge()
        assert store.failed
        assert not store.charge()
    monkeypatch.undo()
    with sup.RecoveryStore(tmp_path) as store:
        # Atomic rename never happened, so there was no authorized spawn.
        assert not store.state["launched"]


def test_owned_child_stubborn_shutdown_and_fixed_environment(tmp_path):
    import time

    sup = module()
    script = tmp_path / "server.py"
    script.write_text(
        "import os, json, signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "with open('observed.tmp', 'w') as f: f.write(json.dumps(dict(os.environ)))\n"
        "os.replace('observed.tmp', 'observed.json')\n"
        "while True: time.sleep(.01)\n"
    )
    secret = "literal$PASSWORD#and space"
    child = sup.BridgeChild(
        tmp_path, script, {"MQTT_PASSWORD": secret}, stop_timeout=0.05
    )
    child.start()
    try:
        for _ in range(200):
            if (tmp_path / "observed.json").exists():
                break
            time.sleep(0.01)
        env = json.loads((tmp_path / "observed.json").read_text())
        assert env["MQTT_PASSWORD"] == secret
        assert env["HEALTH_RECORD_PATH"] == "/dev/shm/casambi-health.json"
        assert env["PYTHON_DOTENV_DISABLED"] == "1"
        assert child.identity == sup.process_identity(child.process.pid)
        assert child.process.args == [sys.executable, str(script)]
        with pytest.raises(RuntimeError):
            child.start()
    finally:
        child.stop()
    assert child.process.returncode == -9


def test_health_requires_actual_child_identity_and_phase_aware_freshness(tmp_path):
    sup = module()
    actual = sup.process_identity(__import__("os").getpid())
    now = 10000.0
    record = dict(
        actual,
        record_version=1,
        phase="connecting",
        error="none",
        ble="disconnected",
        mqtt="disconnected",
        ever_ready=False,
        updated_monotonic=now - 500,
    )
    assert sup.health_fresh(record, actual, now, 30)
    for field, value in [("pid", 99999999), ("start_ticks", 1), ("boot_id", "wrong")]:
        assert not sup.health_fresh(dict(record, **{field: value}), actual, now, 30)
    assert not sup.health_fresh(dict(record, phase="ready"), actual, now, 30)
    assert sup.health_fresh(dict(record, phase="ready"), actual, now, 300)
    assert not sup.health_fresh(
        dict(record, updated_monotonic=now + 1), actual, now, 30
    )
    assert not sup.health_fresh(
        dict(record, updated_monotonic=float("nan")), actual, now, 30
    )
    assert sup.health_fresh(
        dict(record, phase="degraded", error="mqtt_error", updated_monotonic=now - 5),
        actual,
        now,
        30,
    )


def test_prior_live_child_is_not_killed_or_overlapped(tmp_path):
    import subprocess

    sup = module()
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        with sup.RecoveryStore(tmp_path) as store:
            assert store.charge()
            store.mark_child(sup.process_identity(process.pid))
        with sup.RecoveryStore(tmp_path) as store:
            store.reconcile()
            assert store.state["reason"] == "orphan_child"
            assert not store.charge()
            assert not store.rearm(1)
            assert process.poll() is None
    finally:
        process.terminate()
        process.wait(timeout=5)
    with sup.RecoveryStore(tmp_path) as store:
        store.reconcile()
        assert store.state["active"] is None
        assert store.rearm(1)


@pytest.mark.parametrize("code", [4, 5, 0x86, 0x87])
def test_numeric_mqtt_auth_only(code):
    import aiomqtt
    from paho.mqtt.reasoncodes import ReasonCode
    from paho.mqtt.packettypes import PacketTypes

    sup = module()
    rc = ReasonCode(PacketTypes.CONNACK, identifier=code) if code > 5 else code
    assert sup.is_mqtt_auth(aiomqtt.MqttCodeError(rc, "PRIVATE"), connecting=True)
    assert not sup.is_mqtt_auth(RuntimeError("not authorized"), connecting=True)
    assert not sup.is_mqtt_auth(
        aiomqtt.MqttCodeError(7, "bad password"), connecting=True
    )
    # paho local publish error 4 is MQTT_ERR_NO_CONN, not CONNACK auth.
    assert not sup.is_mqtt_auth(aiomqtt.MqttCodeError(4), connecting=False)
    assert not sup.is_mqtt_auth(aiomqtt.MqttError("bad password"), connecting=True)


class FakeClient:
    def __init__(self, *, connect_error=None, publish_error=None):
        self.connect_error = connect_error
        self.publish_error = publish_error
        self.published = []
        self.closed = 0

    async def __aenter__(self):
        if self.connect_error:
            raise self.connect_error
        return self

    async def __aexit__(self, *args):
        self.closed += 1

    @property
    def messages(self):
        async def messages():
            import asyncio

            await asyncio.Future()
            yield None

        return messages()

    async def publish(self, topic, payload, *, qos, retain):
        if self.publish_error:
            raise self.publish_error
        self.published.append((topic, json.loads(payload), qos, retain))


def test_notification_outage_replay_qos_ack_dedupe_and_auth(tmp_path, capsys):
    import asyncio
    import aiomqtt

    sup = module()

    async def exercise():
        clock = [0.0]
        good = FakeClient(connect_error=aiomqtt.MqttError("PRIVATE"))
        transports = [good]
        with sup.RecoveryStore(tmp_path) as store:
            store.latch("retry_exhausted")
            notifier = sup.Notifier(
                store,
                {"CASAMBI_NETWORK_NAME": "test"},
                client_factory=lambda _: transports.pop(0),
                clock=lambda: clock[0],
            )
            await notifier.tick()
            assert store.state["notification"]["pending"] is not None
            assert store.state["attempts"] == 0
            assert notifier.next_at > clock[0]
            clock[0] = notifier.next_at
            good.connect_error = None
            await notifier.tick()
            pending = store.state["notification"]["acked"]
            assert pending["state"] == "failed"
            assert store.state["notification"]["pending"] is None
            assert good.published == [("casambi/test/recovery", pending, 1, True)]
            await notifier.tick()
            assert len(good.published) == 1
            assert not transports
            await notifier.close()
        with sup.RecoveryStore(tmp_path) as store:
            good2 = FakeClient()
            notifier = sup.Notifier(
                store, {"CASAMBI_NETWORK_NAME": "test"}, client_factory=lambda _: good2
            )
            await notifier.tick()
            assert len(good2.published) == 1
            assert good2.published[0][1]["state"] == "failed"
            await notifier.close()
        # A definite CONNACK authentication rejection latches, even with no outbox.
        with sup.RecoveryStore(tmp_path / "auth") as store:
            notifier = sup.Notifier(
                store,
                {"CASAMBI_NETWORK_NAME": "test"},
                client_factory=lambda _: FakeClient(
                    connect_error=aiomqtt.MqttCodeError(5, "PRIVATE")
                ),
            )
            await notifier.tick()
            assert store.state["latched"]
            assert store.state["reason"] == "mqtt_auth"
            assert store.state["notification"]["pending"]["state"] == "failed"
            await notifier.close()

    asyncio.run(exercise())
    captured = capsys.readouterr()
    assert "event=terminal_latched reason=mqtt_auth attempts=0" in captured.err
    assert "PRIVATE" not in captured.out + captured.err


def bridge_fixture(tmp_path, source="import sys; sys.exit(76)\n"):
    import shutil

    app = tmp_path / "app"
    app.mkdir()
    const = app / "custom_components/casambi_mqtt/const.py"
    const.parent.mkdir(parents=True)
    shutil.copyfile(ROOT / "custom_components/casambi_mqtt/const.py", const)
    server = app / "server.py"
    server.write_text(source)
    values = """MQTT_BROKER=unused
MQTT_PORT=1883
MQTT_USERNAME=unused
MQTT_PASSWORD='literal$PASSWORD#keep'
CASAMBI_NETWORK_ADDRESS=AA:BB:CC:DD:EE:FF
CASAMBI_NETWORK_PASSWORD='private casambi'
CASAMBI_NETWORK_NAME=test
HEALTH_RECORD_PATH=/data/forbidden-heartbeat
"""
    (tmp_path / "options.json").write_text(
        json.dumps({"mode": "bridge", "env": values})
    )
    return server


@pytest.mark.parametrize("exit_code", [0, 1, 75, 76, 79, 78])
def test_controller_real_child_exit_budget_and_native_restart(tmp_path, exit_code):
    import asyncio

    sup = module()
    server = bridge_fixture(tmp_path, f"raise SystemExit({exit_code})\n")

    async def exercise():
        now = [0.0]
        for restart in range(5):
            controller = sup.Controller(
                tmp_path,
                server,
                clock=lambda: now[0],
                client_factory=lambda _: FakeClient(),
            )
            await controller.initialize()
            await controller.tick()
            if (exit_code == 78 and restart > 0) or restart == 4:
                assert controller.child is None
                assert controller.store.state["latched"]
                await controller.close()
                continue
            assert controller.child is not None
            process = controller.child.process
            await asyncio.to_thread(process.wait, timeout=5)
            await controller.tick()
            assert controller.child is None
            assert controller.store.state["attempts"] == (
                0 if exit_code == 78 else restart
            )
            if exit_code == 78:
                assert controller.store.state["reason"] == "configuration"
            elif restart == 3:
                assert controller.store.state["reason"] == "retry_exhausted"
            now[0] += 1000
            await controller.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("mode", ["disabled", "preflight", "invalid", "bad_env"])
def test_inert_controller_keeps_liveness_without_child_or_mqtt(
    tmp_path, mode, monkeypatch
):
    import asyncio
    import urllib.request

    sup = module()
    (tmp_path / "options.json").write_text(
        json.dumps({"mode": mode if mode != "bad_env" else "bridge"})
    )

    async def exercise():
        clock = [0.0]
        controller = sup.Controller(
            tmp_path,
            tmp_path / "missing-server.py",
            clock=lambda: clock[0],
            client_factory=lambda _: pytest.fail("MQTT called in inert mode"),
        )
        helper = sup.load_helper()
        calls = []

        async def preflight():
            calls.append("properties-read")

        helper.preflight = preflight
        monkeypatch.setattr(sup, "load_helper", lambda: helper)
        await controller.initialize()
        for _ in range(3):
            await controller.tick()
            assert controller.child is None
        assert calls == (["properties-read"] if mode == "preflight" else [])
        controller.start_http(host="127.0.0.1", port=0)
        url = f"http://127.0.0.1:{controller.http.server_address[1]}/supervisor-health"
        with urllib.request.urlopen(url, timeout=3) as response:
            assert response.status == 200
            assert response.read() == b"controller alive\n"
        clock[0] += 31
        with pytest.raises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(url, timeout=3)
        assert error.value.code == 503
        await controller.tick()
        with urllib.request.urlopen(url, timeout=3) as response:
            assert response.status == 200
        await controller.close()

    asyncio.run(exercise())


def test_auth_latch_stops_owned_child_but_outage_does_not(tmp_path):
    import asyncio
    import aiomqtt

    sup = module()
    server = bridge_fixture(tmp_path, "import time; time.sleep(60)\n")

    async def exercise():
        now = [0.0]
        client = FakeClient(connect_error=aiomqtt.MqttError("PRIVATE"))
        controller = sup.Controller(
            tmp_path, server, clock=lambda: now[0], client_factory=lambda _: client
        )
        await controller.initialize()
        await controller.tick()
        child = controller.child
        await controller.notifier.tick()
        await controller.tick()
        assert controller.child is child
        assert controller.store.state["attempts"] == 0
        client.connect_error = aiomqtt.MqttCodeError(5, "PRIVATE")
        now[0] = controller.notifier.next_at
        await controller.notifier.tick()
        await controller.tick()
        assert controller.child is None
        assert child.process.poll() is not None
        assert controller.store.state["latched"]
        for _ in range(4):
            await controller.tick()
            assert controller.child is None
        assert controller.liveness_status() == 200
        await controller.close()

    asyncio.run(exercise())


def test_run_loop_graceful_stop_does_not_reset_or_restart(tmp_path):
    import asyncio

    sup = module()
    server = bridge_fixture(tmp_path, "import time; time.sleep(60)\n")

    async def exercise():
        controller = sup.Controller(
            tmp_path, server, client_factory=lambda _: FakeClient()
        )
        running = asyncio.create_task(controller.run(host="127.0.0.1", port=0))
        try:
            for _ in range(300):
                if controller.child is not None:
                    break
                await asyncio.sleep(0.01)
            assert controller.child is not None
            child = controller.child
            controller.stopping.set()
            await asyncio.wait_for(running, timeout=15)
            assert child.process.poll() is not None
            state = json.loads((tmp_path / "recovery/state.json").read_text())
            assert state["attempts"] == 0
            assert state["launched"]
            assert state["active"] is None
            # Native watchdog/manual start share SIGTERM: neither renews budget.
            next_controller = sup.Controller(
                tmp_path, server, client_factory=lambda _: FakeClient()
            )
            await next_controller.initialize()
            await next_controller.tick()
            assert next_controller.store.state["attempts"] == 1
            await next_controller.close()
        finally:
            controller.stopping.set()
            await asyncio.wait_for(running, timeout=15)

    asyncio.run(exercise())


@pytest.mark.parametrize("marked", [False, True])
def test_second_intent_cannot_authorize_overlap(tmp_path, marked):
    sup = module()
    with sup.RecoveryStore(tmp_path) as store:
        assert store.charge()
        if marked:
            store.mark_child(sup.process_identity(__import__("os").getpid()))
        assert not store.charge()
        assert store.state["attempts"] == 0
        assert store.state["latched"]


def test_rearm_outbox_overwrites_retained_failure_only_once(tmp_path):
    import asyncio

    sup = module()

    async def exercise():
        client = FakeClient()
        with sup.RecoveryStore(tmp_path) as store:
            store.latch("configuration")
            notifier = sup.Notifier(
                store, {"CASAMBI_NETWORK_NAME": "test"}, client_factory=lambda _: client
            )
            await notifier.tick()
            assert store.rearm(1)
            assert store.state["notification"]["pending"]["state"] == "armed"
            await notifier.tick()
            assert [p[1]["state"] for p in client.published] == ["failed", "armed"]
            assert client.published[-1][1]["attempts"] == 0
            assert not store.rearm(1)
            await notifier.tick()
            assert len(client.published) == 2
            await notifier.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("failure_call", [1, 2])
def test_storage_failure_after_charge_never_spawns(tmp_path, monkeypatch, failure_call):
    import asyncio

    sup = module()
    server = bridge_fixture(tmp_path, "open('MARKER', 'a').write('spawned')\n")

    async def exercise():
        controller = sup.Controller(
            tmp_path, server, client_factory=lambda _: FakeClient()
        )
        await controller.initialize()
        calls = []
        fsync = sup.os.fsync

        def fail_nth(descriptor):
            calls.append(descriptor)
            if len(calls) == failure_call:
                raise OSError("PRIVATE")
            return fsync(descriptor)

        monkeypatch.setattr(sup.os, "fsync", fail_nth)
        await controller.tick()
        assert controller.child is None
        assert controller.mode == "failed"
        assert not (tmp_path / "MARKER").exists()
        for _ in range(5):
            await controller.tick()
        monkeypatch.undo()
        await controller.close()
        with sup.RecoveryStore(tmp_path) as store:
            store.reconcile()
            if failure_call == 2:
                # Rename completed before directory fsync failed: consume intent.
                assert store.state["latched"]
                assert not store.charge()

    asyncio.run(exercise())


def test_ambiguous_spawn_blocks_reset_until_namespace_restart(tmp_path):
    sup = module()
    with sup.RecoveryStore(tmp_path) as store:
        assert store.charge()
    with sup.RecoveryStore(tmp_path) as store:
        store.reconcile()
        assert store.state["inflight"]
        assert not store.rearm(1)
        # Simulate native container replacement, not just wrapper death. The old
        # container's process namespace was destroyed by its owner, not by us.
        store.state["namespace"] = "previous-boot:previous-pid-namespace"
        store.save()
    with sup.RecoveryStore(tmp_path) as store:
        store.reconcile()
        assert not store.state["inflight"]
        assert store.state["latched"]
        assert store.rearm(1)


def test_new_recovery_directory_is_fsynced_in_parent(tmp_path, monkeypatch):
    sup = module()
    destinations = []
    fsync = sup.os.fsync

    def capture(descriptor):
        destinations.append(sup.os.readlink(f"/proc/self/fd/{descriptor}"))
        return fsync(descriptor)

    monkeypatch.setattr(sup.os, "fsync", capture)
    with sup.RecoveryStore(tmp_path):
        pass
    assert str(tmp_path) in destinations
    assert str(tmp_path / "recovery") in destinations


def test_one_transport_reused_across_outages(tmp_path):
    import asyncio
    import aiomqtt

    sup = module()

    async def exercise():
        now = [0.0]
        client = FakeClient(connect_error=aiomqtt.MqttError("PRIVATE"))
        calls = []

        def factory(values):
            calls.append(True)
            return client

        with sup.RecoveryStore(tmp_path) as store:
            store.latch("configuration")
            notifier = sup.Notifier(
                store,
                {"CASAMBI_NETWORK_NAME": "test"},
                clock=lambda: now[0],
                client_factory=factory,
            )
            for _ in range(4):
                await notifier.tick()
                now[0] = notifier.next_at
            client.connect_error = None
            await notifier.tick()
            assert len(client.published) == 1
            assert calls == [True]
            await notifier.close()

    asyncio.run(exercise())


def test_unexpected_notifier_error_fails_closed_without_secret_traceback(
    tmp_path, capsys
):
    import asyncio

    sup = module()
    server = bridge_fixture(tmp_path, "import time; time.sleep(60)\n")

    async def exercise():
        controller = sup.Controller(
            tmp_path, server, client_factory=lambda _: FakeClient()
        )
        await controller.initialize()
        await controller.tick()
        child = controller.child

        async def crash():
            raise TypeError("PRIVATE")

        controller.notifier_task = asyncio.create_task(crash())
        await asyncio.sleep(0)
        try:
            await controller.tick()
            assert controller.child is None
            assert child.process.poll() is not None
            assert controller.mode == "failed"
            assert controller.liveness_status() == 200
        finally:
            await controller.close()

    asyncio.run(exercise())
    captured = capsys.readouterr()
    assert "PRIVATE" not in captured.out + captured.err


def test_broker_outage_vetoes_stale_health_restart_then_recovers(tmp_path, monkeypatch):
    import asyncio
    import aiomqtt

    sup = module()
    server = bridge_fixture(tmp_path, "import time; time.sleep(60)\n")
    health = tmp_path / "health.json"
    monkeypatch.setattr(sup, "HEALTH_PATH", health)

    async def exercise():
        now = [0.0]
        transport = FakeClient(connect_error=aiomqtt.MqttError("PRIVATE"))
        controller = sup.Controller(
            tmp_path, server, clock=lambda: now[0], client_factory=lambda _: transport
        )
        await controller.initialize()
        await controller.tick()
        child = controller.child
        try:
            health.write_text(
                json.dumps(
                    dict(
                        child.identity,
                        record_version=1,
                        phase="ready",
                        error="none",
                        ble="connected",
                        mqtt="connected",
                        ever_ready=True,
                        updated_monotonic=0,
                    )
                )
            )
            await controller.notifier.tick()
            now[0] = sup.STARTUP_GRACE + 1
            await controller.tick()
            assert controller.child is child
            assert controller.store.state["attempts"] == 0
            transport.connect_error = None
            await controller.notifier.tick()
            await controller.tick()
            assert controller.child is None
            assert child.process.poll() is not None
            assert controller.store.state["reason"] == "health_stale"
            next_at = controller.next_at
            await controller.tick()
            assert controller.child is None
            now[0] = next_at
            await controller.tick()
            assert controller.child is not None
            assert controller.store.state["attempts"] == 1
        finally:
            await controller.close()

    asyncio.run(exercise())


def test_four_real_children_max_even_with_brief_ready_between_crashes(
    tmp_path, monkeypatch
):
    import asyncio

    sup = module()
    server = bridge_fixture(tmp_path, "import time; time.sleep(60)\n")
    health = tmp_path / "health.json"
    monkeypatch.setattr(sup, "HEALTH_PATH", health)

    async def exercise():
        now = [0.0]
        controller = sup.Controller(
            tmp_path,
            server,
            clock=lambda: now[0],
            client_factory=lambda _: FakeClient(),
        )
        await controller.initialize()
        children = []
        try:
            await controller.notifier.tick()
            for launch in range(4):
                await controller.tick()
                child = controller.child
                assert child is not None
                children.append(child.process.pid)
                health.write_text(
                    json.dumps(
                        dict(
                            child.identity,
                            record_version=1,
                            phase="ready",
                            error="none",
                            ble="connected",
                            mqtt="connected",
                            ever_ready=True,
                            updated_monotonic=now[0],
                        )
                    )
                )
                await controller.tick()
                assert controller.child is child
                assert controller.store.state["attempts"] == launch
                child.process.terminate()
                await asyncio.to_thread(child.process.wait, timeout=5)
                await controller.tick()
                assert controller.child is None
                now[0] = controller.next_at
            assert len(set(children)) == 4
            assert controller.store.state["latched"]
            assert controller.store.state["attempts"] == 3
            for _ in range(10):
                now[0] += 10000
                await controller.tick()
                assert controller.child is None
                assert controller.liveness_status() == 200
        finally:
            await controller.close()

    asyncio.run(exercise())


def test_real_sigterm_keeps_invalid_controller_alive_until_requested_stop(tmp_path):
    import subprocess
    import time

    (tmp_path / "options.json").write_text(
        json.dumps({"mode": "bridge", "env": "PRIVATE=bad"})
    )
    script = ROOT / "haos_casambi/bridge_supervisor.py"
    code = (
        "import runpy, asyncio; from pathlib import Path; "
        f"m=runpy.run_path({str(script)!r}); "
        f"asyncio.run(m['Controller'](Path({str(tmp_path)!r})).run(host='127.0.0.1',port=0))"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    try:
        for _ in range(300):
            try:
                state = json.loads((tmp_path / "recovery/state.json").read_text())
                if state["latched"]:
                    break
            except (OSError, ValueError):
                pass
            assert process.poll() is None
            time.sleep(0.01)
        else:
            pytest.fail("controller never latched invalid configuration")
        time.sleep(0.05)
        assert process.poll() is None
        assert (
            Path(f"/proc/{process.pid}/task/{process.pid}/children").read_text().strip()
            == ""
        )
        process.terminate()
        stdout, stderr = process.communicate(timeout=15)
        assert process.returncode == 0
        assert b"PRIVATE" not in stdout + stderr
        assert b"Invalid bridge configuration" in stderr
        assert state["attempts"] == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)


def test_offline_rearm_survives_retries_restart_until_puback(tmp_path):
    import asyncio
    import aiomqtt

    sup = module()

    async def exercise():
        now = [0.0]
        client = FakeClient(connect_error=aiomqtt.MqttError("PRIVATE"))
        with sup.RecoveryStore(tmp_path) as store:
            store.latch("configuration")
            assert store.rearm(1)
            armed = dict(store.state["notification"]["pending"])
            notifier = sup.Notifier(
                store,
                {"CASAMBI_NETWORK_NAME": "test"},
                client_factory=lambda _: client,
                clock=lambda: now[0],
            )
            await notifier.tick()
            controller = sup.Controller(tmp_path, clock=lambda: now[0])
            controller.store = store
            assert store.charge()
            controller._failure("child_failure")
            assert store.state["notification"]["pending"] == armed
            assert store.charge()
            assert store.state["attempts"] == 1
            assert store.state["notification"]["pending"] == armed
            store.child_ended()
            await notifier.close()

        with sup.RecoveryStore(tmp_path) as store:
            store.reconcile()
            assert store.state["notification"]["pending"] == armed
            assert store.state["attempts"] == 1
            acknowledged = asyncio.Event()
            publishing = asyncio.Event()

            class AckClient(FakeClient):
                async def publish(self, *args, **kwargs):
                    await super().publish(*args, **kwargs)
                    publishing.set()
                    await acknowledged.wait()

            client = AckClient()
            notifier = sup.Notifier(
                store, {"CASAMBI_NETWORK_NAME": "test"}, client_factory=lambda _: client
            )
            task = asyncio.create_task(notifier.tick())
            await publishing.wait()
            assert store.state["notification"]["pending"] == armed
            disk = json.loads(store.path.read_text())
            assert disk["notification"]["pending"] == armed
            assert disk["notification"]["acked"] is None
            acknowledged.set()
            await task
            assert client.published == [("casambi/test/recovery", armed, 1, True)]
            assert armed["attempts"] == 0
            assert store.state["notification"] == {"pending": None, "acked": armed}
            await notifier.close()

        with sup.RecoveryStore(tmp_path) as store:
            assert store.state["notification"] == {"pending": None, "acked": armed}
            client = FakeClient()
            notifier = sup.Notifier(
                store, {"CASAMBI_NETWORK_NAME": "test"}, client_factory=lambda _: client
            )
            await notifier.tick()
            assert not client.published
            await notifier.close()

    asyncio.run(exercise())


def test_exit75_offline_retries_log_budget_and_failed_supersedes_armed(tmp_path, capfd):
    import asyncio
    import aiomqtt

    sup = module()
    server = bridge_fixture(
        tmp_path,
        "import sys; print('SYNTHETIC_SECRET'); "
        "print('SYNTHETIC_SECRET', file=sys.stderr); raise SystemExit(75)\n",
    )

    async def exercise():
        now = [0.0]
        controller = sup.Controller(
            tmp_path,
            server,
            clock=lambda: now[0],
            client_factory=lambda _: FakeClient(
                connect_error=aiomqtt.MqttError("SYNTHETIC_SECRET")
            ),
        )
        await controller.initialize()
        try:
            assert controller.store.rearm(1)
            await controller.notifier.tick()
            for attempt in range(4):
                await controller.tick()
                assert controller.child is not None
                assert controller.store.state["attempts"] == attempt
                await asyncio.to_thread(controller.child.process.wait, timeout=5)
                await controller.tick()
                pending = controller.store.state["notification"]["pending"]
                assert pending["state"] == ("failed" if attempt == 3 else "armed")
                assert pending["attempts"] == (3 if attempt == 3 else 0)
                now[0] = controller.next_at
            for _ in range(3):
                await controller.tick()
                assert controller.child is None
            assert controller.store.state["reason"] == "retry_exhausted"
            assert not controller.store.rearm(1)
        finally:
            await controller.close()

    asyncio.run(exercise())
    captured = capfd.readouterr()
    assert captured.err.splitlines() == [
        "event=launch_reserved reason=none attempts=0",
        "event=retry_scheduled reason=child_failure attempts=0",
        "event=launch_reserved reason=child_failure attempts=1",
        "event=retry_scheduled reason=child_failure attempts=1",
        "event=launch_reserved reason=child_failure attempts=2",
        "event=retry_scheduled reason=child_failure attempts=2",
        "event=launch_reserved reason=child_failure attempts=3",
        "event=terminal_latched reason=retry_exhausted attempts=3",
    ]
    assert "SYNTHETIC_SECRET" not in captured.out + captured.err
    assert str(tmp_path) not in captured.out + captured.err
    assert "literal$PASSWORD" not in captured.out + captured.err


@pytest.mark.parametrize(
    ("event", "reason", "attempts"),
    [
        ("SYNTHETIC_SECRET", "none", 0),
        ("terminal_latched", "SYNTHETIC_SECRET", 0),
        ("terminal_latched", "none", "SYNTHETIC_SECRET"),
        ("terminal_latched", "none", True),
        ("terminal_latched", "none", -1),
        ("terminal_latched", "none", 4),
    ],
)
def test_log_boundary_rejects_unvalidated_fields(event, reason, attempts, capsys):
    sup = module()
    with pytest.raises(ValueError):
        sup.log_event(event, reason, attempts)
    captured = capsys.readouterr()
    assert captured.out + captured.err == ""


def test_exit78_logs_safe_terminal_reason_without_broker(tmp_path, capfd):
    import asyncio
    import aiomqtt

    sup = module()
    server = bridge_fixture(
        tmp_path,
        "import sys; print('SYNTHETIC_SECRET'); "
        "print('SYNTHETIC_SECRET', file=sys.stderr); raise SystemExit(78)\n",
    )

    async def exercise():
        controller = sup.Controller(
            tmp_path,
            server,
            client_factory=lambda _: FakeClient(
                connect_error=aiomqtt.MqttError("SYNTHETIC_SECRET")
            ),
        )
        await controller.initialize()
        try:
            await controller.notifier.tick()
            await controller.tick()
            await asyncio.to_thread(controller.child.process.wait, timeout=5)
            await controller.tick()
            assert controller.store.state["latched"]
            assert controller.store.state["attempts"] == 0
        finally:
            await controller.close()

    asyncio.run(exercise())
    captured = capfd.readouterr()
    assert "event=terminal_latched reason=configuration attempts=0" in captured.err
    assert "SYNTHETIC_SECRET" not in captured.out + captured.err
    assert str(tmp_path) not in captured.out + captured.err
    assert "literal$PASSWORD" not in captured.out + captured.err


def test_latched_restart_replays_failed_with_same_incident_id(tmp_path):
    import asyncio

    sup = module()

    async def exercise():
        incident = None
        for restart in range(2):
            transport = FakeClient()
            with sup.RecoveryStore(tmp_path) as store:
                if restart == 0:
                    store.latch("configuration")
                    incident = store.state["incident_id"]
                notifier = sup.Notifier(
                    store,
                    {"CASAMBI_NETWORK_NAME": "test"},
                    client_factory=lambda _: transport,
                )
                await notifier.tick()
                assert len(transport.published) == 1
                payload = transport.published[0][1]
                assert payload["state"] == "failed"
                assert payload["incident_id"] == incident
                await notifier.tick()
                assert len(transport.published) == 1
                await notifier.close()

    asyncio.run(exercise())
