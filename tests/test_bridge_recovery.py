"""
Root-owned recovery supervisor (#27) with a fake clock and service manager.

Nothing here starts systemd, touches Bluetooth, uses sudo or reboots: the
service manager is a fake that records every call, and the real Systemctl
wrapper is only exercised with a fake ``run`` function.
"""

import importlib.util
import json
import subprocess
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BOOT = "11111111-2222-3333-4444-555555555555"
OLD = "a" * 32
OLDER = "c" * 32
NEW = "b" * 32


@pytest.fixture
def rec():
    spec = importlib.util.spec_from_file_location(
        "bridge_recovery_under_test", ROOT / "scripts/bridge_recovery.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module.log = lambda priority, message: None
    return module


@pytest.fixture
def settings(rec, tmp_path):
    return rec.Settings(
        health_record=tmp_path / "health.json",
        state_dir=tmp_path / "state",
        poll_interval=1.0,
    )


class FakeClock:
    def __init__(self, rec, wall=1_000_000.0, mono=5_000.0):
        self.wall_now = wall
        self.mono_now = mono
        self.clock = rec.Clock(
            wall=lambda: self.wall_now,
            monotonic=lambda: self.mono_now,
            sleep=self.sleep,
        )

    def sleep(self, seconds):
        self.wall_now += seconds
        self.mono_now += seconds

    def advance(self, seconds):
        self.sleep(seconds)


class FakeManager:
    """Records every systemctl call; simulates unit state."""

    def __init__(self, rec, *, fail_verb=None, stop_hangs=False, result="success"):
        self.rec = rec
        self.calls = []
        self.fail_verb = fail_verb
        self.stop_hangs = stop_hangs
        self.bridge_pid = "1234"
        self.result = result
        self.allowed = frozenset({"casambi-mqtt.service", "bluetooth.service"})
        self.on_start = None

    def action(self, verb, unit, timeout):
        assert unit in self.allowed
        self.calls.append((verb, unit))
        if verb == self.fail_verb:
            raise self.rec.ServiceManagerError(verb)
        if verb == "stop" and unit == "casambi-mqtt.service" and not self.stop_hangs:
            self.bridge_pid = "0"
        if verb == "start" and self.on_start is not None:
            self.on_start()

    def show(self, unit):
        assert unit in self.allowed
        if unit == "bluetooth.service":
            return {"ActiveState": "active", "MainPID": "77"}
        return {"MainPID": self.bridge_pid, "Result": self.result}

    @property
    def mutations(self):
        return list(self.calls)


def view(rec, instance, *, alive, phase="offline", error="ble_discovery_in_progress",
         ever_ready=False, ready_since=None):
    return rec.RecordView(
        instance_id=instance,
        pid=1234,
        phase=phase,
        error=error,
        ever_ready=ever_ready,
        ready_since=ready_since,
        updated=0.0,
        alive=alive,
    )


class Records:
    """A sequence of record observations; the last one repeats."""

    def __init__(self, *records):
        self.records = list(records)

    def __call__(self):
        if len(self.records) > 1:
            return self.records.pop(0)
        return self.records[0]


def fail_two_processes(rec, settings, manager, fake, verify=None):
    """Two distinct bridge processes died with InProgress, one minute apart."""
    first = rec.run_once(
        settings, manager, fake.clock, Records(view(rec, OLDER, alive=False))
    )
    fake.advance(60)
    reader = Records(view(rec, OLD, alive=False), *(verify or []))
    return first, rec.run_once(settings, manager, fake.clock, reader)


def ready_new(rec):
    return view(rec, NEW, alive=True, phase="ready", error="none", ever_ready=True)


# ---------------------------------------------------------------------------
# Settings and untrusted input
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "env",
    [
        {"RECOVERY_BRIDGE_UNIT": "bluetooth.service"},
        {"RECOVERY_BRIDGE_UNIT": "x.service; rm -rf /"},
        {"RECOVERY_BRIDGE_UNIT": "../evil.service"},
        {"RECOVERY_BRIDGE_UNIT": "casambi.timer"},
        {"RECOVERY_HEALTH_RECORD": "relative/health.json"},
        {"RECOVERY_MAX_RESETS_SHORT": "0"},
        {"RECOVERY_MAX_RESETS_SHORT": "3", "RECOVERY_MAX_RESETS_LONG": "2"},
        {"RECOVERY_FAILURES_TO_RESET": "1"},
        {"RECOVERY_VERIFY_SECONDS": "lots"},
    ],
)
def test_invalid_settings_are_rejected(rec, env):
    with pytest.raises(rec.SettingsError):
        rec.Settings.from_env(env)


def test_default_settings_match_the_documented_budget(rec):
    settings = rec.Settings.from_env({})
    assert settings.bridge_unit == "casambi-mqtt.service"
    assert (settings.max_resets_short, settings.short_window) == (1, 600)
    assert (settings.max_resets_long, settings.long_window) == (2, 1800)


def record_bytes(**overrides):
    data = {
        "record_version": 1,
        "instance_id": NEW,
        "pid": 1234,
        "start_ticks": 999,
        "boot_id": BOOT,
        "phase": "ready",
        "error": "none",
        "ble": "connected",
        "mqtt": "connected",
        "ever_ready": True,
        "ready_since_monotonic": 10.0,
        "updated_monotonic": 20.0,
    }
    data.update(overrides)
    return json.dumps(data).encode()


def parse(rec, raw, ticks=999):
    return rec.parse_record(raw, boot_id=BOOT, start_ticks_of=lambda pid: ticks)


def test_valid_live_record(rec):
    record = parse(rec, record_bytes())
    assert record.alive
    assert record.phase == "ready"


def test_record_of_a_replaced_or_exited_process_is_not_alive(rec):
    assert parse(rec, record_bytes(), ticks=1000).alive is False
    assert parse(rec, record_bytes(), ticks=None).alive is False


@pytest.mark.parametrize(
    "raw",
    [
        record_bytes(boot_id="99999999-2222-3333-4444-555555555555"),
        record_bytes(record_version=2),
        record_bytes(phase="rm -rf"),
        record_bytes(error="free text"),
        record_bytes(pid=True),
        record_bytes(pid=0),
        record_bytes(instance_id="../../etc"),
        record_bytes(ever_ready="yes"),
        record_bytes(updated_monotonic=-1),
        b"not json",
        b"[]",
        b"{" + b" " * 5000 + b"}",
    ],
)
def test_untrusted_record_fails_closed(rec, raw):
    assert parse(rec, raw) is None


# ---------------------------------------------------------------------------
# Decisions and full recovery
# ---------------------------------------------------------------------------


def test_repeated_ble_failure_does_exactly_stop_restart_start(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec)

    first, second = fail_two_processes(
        rec, settings, manager, fake, verify=[ready_new(rec)]
    )

    assert first == "no_repeated_ble_failure"
    assert second == "recovered"
    assert manager.mutations == [
        ("stop", "casambi-mqtt.service"),
        ("restart", "bluetooth.service"),
        ("start", "casambi-mqtt.service"),
    ]
    state = rec.load_state(settings)
    assert state.ble_failures == []
    assert len(state.resets) == 1


def test_start_limit_hit_is_reset_only_inside_a_budgeted_recovery(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, result="start-limit-hit")

    fail_two_processes(rec, settings, manager, fake, verify=[ready_new(rec)])

    assert manager.mutations == [
        ("stop", "casambi-mqtt.service"),
        ("restart", "bluetooth.service"),
        ("reset-failed", "casambi-mqtt.service"),
        ("start", "casambi-mqtt.service"),
    ]


def test_same_instance_is_counted_once(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec)
    for _ in range(5):
        rec.run_once(settings, manager, fake.clock, Records(view(rec, OLD, alive=False)))
        fake.advance(60)
    assert manager.calls == []


@pytest.mark.parametrize(
    ("error", "ever_ready"),
    [
        ("mqtt_error", True),
        ("configuration", False),
        ("network_not_found", False),
        ("unknown", False),
        ("shutdown", True),
        ("ble_disconnected", True),
    ],
)
def test_non_ble_faults_never_reset_bluetooth(rec, settings, error, ever_ready):
    fake = FakeClock(rec)
    manager = FakeManager(rec)
    for instance in ("1" * 32, "2" * 32, "3" * 32, "4" * 32):
        record = view(rec, instance, alive=False, error=error, ever_ready=ever_ready)
        rec.run_once(settings, manager, fake.clock, Records(record))
        fake.advance(30)
    assert manager.calls == []


def test_ha_consumer_fault_with_healthy_bridge_never_resets(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec)
    for _ in range(10):
        rec.run_once(settings, manager, fake.clock, Records(ready_new(rec)))
        fake.advance(60)
    assert manager.calls == []


def test_budget_survives_bridge_restarts_and_new_instances(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec)
    fail_two_processes(rec, settings, manager, fake, verify=[ready_new(rec)])
    assert len(manager.calls) == 3

    # A new process starts (not yet healthy for long), then fails again twice.
    fake.advance(60)
    rec.run_once(
        settings,
        manager,
        fake.clock,
        Records(view(rec, NEW, alive=True, phase="starting", error="none")),
    )
    for instance in ("5" * 32, "6" * 32):
        fake.advance(60)
        result = rec.run_once(
            settings, manager, fake.clock, Records(view(rec, instance, alive=False))
        )

    assert result == "budget_exhausted"
    assert len(manager.calls) == 3  # no second reset inside 10 minutes
    state = rec.load_state(settings)
    assert state.recovery_failed
    assert state.reason == "budget_exhausted"


def test_latched_failure_stops_further_resets_until_verified_healthy(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec)
    rec.save_state(settings, rec.State(recovery_failed=True, reason="budget_exhausted"))
    for instance in ("5" * 32, "6" * 32, "7" * 32):
        fake.advance(3600)
        rec.run_once(
            settings, manager, fake.clock, Records(view(rec, instance, alive=False))
        )
    assert manager.calls == []

    # A new instance that has been ready for a long, verified period clears it.
    healthy = view(
        rec,
        NEW,
        alive=True,
        phase="ready",
        error="none",
        ever_ready=True,
        ready_since=fake.mono_now - settings.healthy_reset_seconds,
    )
    rec.run_once(settings, manager, fake.clock, Records(healthy))
    assert rec.load_state(settings).recovery_failed is False


def test_two_resets_per_thirty_minutes_then_latch(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec)
    results = []
    for round_number in range(3):
        instances = [f"{round_number}{n}" * 16 for n in (1, 2)]
        for instance in instances:
            fake.advance(60)
            results.append(
                rec.run_once(
                    settings,
                    manager,
                    fake.clock,
                    Records(view(rec, instance, alive=False), ready_new(rec)),
                )
            )
        fake.advance(601)
    assert results.count("recovered") == 2
    assert results[-1] == "budget_exhausted"
    assert [call for call in manager.calls if call[0] == "restart"] == [
        ("restart", "bluetooth.service")
    ] * 2


def test_failure_mid_recovery_is_recovery_failed_and_restarts_bridge(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, fail_verb="restart")

    _, result = fail_two_processes(rec, settings, manager, fake)

    assert result == "recovery_failed"
    assert manager.mutations == [
        ("stop", "casambi-mqtt.service"),
        ("restart", "bluetooth.service"),
        ("start", "casambi-mqtt.service"),  # cleanup: never left stopped
    ]
    state = rec.load_state(settings)
    assert state.recovery_failed
    assert state.reason == "service_manager_restart"


def test_bridge_that_does_not_stop_blocks_bluetooth_reset(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, stop_hangs=True)

    _, result = fail_two_processes(rec, settings, manager, fake)

    assert result == "recovery_failed"
    assert ("restart", "bluetooth.service") not in manager.calls
    assert rec.load_state(settings).reason == "bridge_stop_timeout"


def test_unverified_recovery_is_not_success(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec)
    still_failing = view(rec, NEW, alive=True, phase="discovering")

    _, result = fail_two_processes(rec, settings, manager, fake, verify=[still_failing])

    assert result == "verify_failed"
    state = rec.load_state(settings)
    assert state.last_result == "verify_failed"
    assert len(state.resets) == 1


def test_old_record_never_verifies_a_recovery(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec)
    stale_ready = view(rec, OLD, alive=True, phase="ready", error="none")

    _, result = fail_two_processes(rec, settings, manager, fake, verify=[stale_ready])

    assert result == "verify_failed"


def test_corrupt_state_fails_closed(rec, settings):
    settings.state_dir.mkdir()
    (settings.state_dir / "state.json").write_text("{not json")
    fake = FakeClock(rec)
    manager = FakeManager(rec)

    fail_two_processes(rec, settings, manager, fake)

    assert manager.calls == []
    assert rec.load_state(settings).reason == "state_unreadable"


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def test_held_lock_means_no_second_operation(rec, settings):
    manager = FakeManager(rec)
    with rec.exclusive_lock(settings) as acquired:
        assert acquired
        result = rec.run_once(
            settings, manager, FakeClock(rec).clock, Records(view(rec, OLD, alive=False))
        )
    assert result == "locked"
    assert manager.calls == []


def test_concurrent_failure_and_timer_triggers_produce_one_recovery(rec, settings):
    fake = FakeClock(rec)
    gate = threading.Event()
    entered = threading.Event()

    class BlockingManager(FakeManager):
        def action(self, verb, unit, timeout):
            if verb == "stop":
                entered.set()
                gate.wait(5)
            super().action(verb, unit, timeout)

    manager = BlockingManager(rec)
    rec.run_once(settings, manager, fake.clock, Records(view(rec, OLDER, alive=False)))
    results = []

    def trigger():
        results.append(
            rec.run_once(
                settings,
                manager,
                fake.clock,
                Records(view(rec, OLD, alive=False), ready_new(rec)),
            )
        )

    first = threading.Thread(target=trigger)
    first.start()
    assert entered.wait(5)
    second = threading.Thread(target=trigger)
    second.start()
    second.join(5)
    gate.set()
    first.join(5)

    assert sorted(results) == ["locked", "recovered"]
    assert [call for call in manager.calls if call[0] == "restart"] == [
        ("restart", "bluetooth.service")
    ]
    # A later trigger finds nothing left to do.
    assert (
        rec.run_once(settings, manager, fake.clock, Records(ready_new(rec)))
        == "no_repeated_ble_failure"
    )


# ---------------------------------------------------------------------------
# The real systemctl wrapper (with a fake ``run``)
# ---------------------------------------------------------------------------


def test_systemctl_wrapper_uses_fixed_argv_and_allowlist(rec):
    calls = []

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, stdout="MainPID=0\nOther=x\n")

    manager = rec.Systemctl(
        frozenset({"casambi-mqtt.service", "bluetooth.service"}),
        run=run,
        executable="/usr/bin/systemctl",
    )
    manager.action("restart", "bluetooth.service", 5)
    assert manager.show("casambi-mqtt.service") == {"MainPID": "0"}
    for verb, unit in (
        ("restart", "ssh.service"),
        ("restart", "bluetooth.service; reboot"),
        ("reboot", "bluetooth.service"),
        ("isolate", "casambi-mqtt.service"),
    ):
        with pytest.raises(rec.ServiceManagerError):
            manager.action(verb, unit, 5)

    assert calls[0][0] == ["/usr/bin/systemctl", "restart", "bluetooth.service"]
    assert all("shell" not in kwargs for _, kwargs in calls)
    assert len(calls) == 2


def test_systemctl_failure_and_timeout_raise(rec):
    def failing(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 1, stdout="")

    def hanging(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, 1)

    for run in (failing, hanging):
        manager = rec.Systemctl(frozenset({"bluetooth.service"}), run=run)
        with pytest.raises(rec.ServiceManagerError):
            manager.action("restart", "bluetooth.service", 1)


def test_operator_reset_clears_the_latch(rec, settings, monkeypatch):
    rec.save_state(settings, rec.State(recovery_failed=True, reason="budget_exhausted"))
    monkeypatch.setenv("RECOVERY_STATE_DIR", str(settings.state_dir))
    monkeypatch.setenv("RECOVERY_HEALTH_RECORD", str(settings.health_record))

    assert rec.main(["reset"]) == 0

    state = rec.load_state(settings)
    assert state.recovery_failed is False
    assert state.last_result == "operator_reset"
