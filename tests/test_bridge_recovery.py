"""
Root-owned recovery supervisor (#27) with a fake clock and service manager.

Nothing here starts systemd, touches Bluetooth, uses sudo or reboots: the
service manager is a fake that records every call and simulates the unit's
systemd bookkeeping (NRestarts, ExecMainStatus, MainPID), and the real
Systemctl wrapper is only exercised with a fake ``run`` function.
"""

import importlib.util
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BOOT = "11111111-2222-3333-4444-555555555555"
OLD = "a" * 32
NEW = "b" * 32
BRIDGE = "casambi-mqtt.service"
BT = "bluetooth.service"


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
    def __init__(self, rec):
        self.wall_now = 1_000_000.0
        self.mono_now = 5_000.0
        self.boot = BOOT
        self.clock = rec.Clock(
            wall=lambda: self.wall_now,
            monotonic=lambda: self.mono_now,
            sleep=self.advance,
            boottime=lambda: self.mono_now,
            boot_id=lambda: self.boot,
        )

    def advance(self, seconds):
        self.wall_now += seconds
        self.mono_now += seconds


class FakeManager:
    """Records systemctl calls; simulates the bridge unit's systemd counters."""

    def __init__(self, rec, settings, fake, *, fail_verb=None, stop_hangs=False):
        self.rec = rec
        self.settings = settings
        self.fake = fake
        self.calls = []
        self.fail_verb = fail_verb
        self.stop_hangs = stop_hangs
        self.active = "active"
        self.main_pid = 100
        self.exec_main_pid = 100
        self.exec_main_status = 0
        self.n_restarts = 0
        self.result = "success"
        self.after_start = None

    # -- simulated bridge lifecycle -------------------------------------
    def bridge_exits(self, status, *, restarted=True):
        """
        One bridge exit as systemd handles it: ExecStopPost=+ reports it,
        then either an automatic restart (which CLEARS ExecMainStatus, as
        the real systemd does) or giving up with the status kept.
        """
        assert self.rec.record_exit(
            self.settings,
            {"EXIT_CODE": "exited", "EXIT_STATUS": str(status)},
            self.fake.clock,
        )
        self.exec_main_pid = self.main_pid
        if restarted:
            self.n_restarts += 1
            self.main_pid += 1
            self.exec_main_status = 0
            self.active = "active"
        else:
            self.n_restarts += 1  # systemd counts the attempt before refusing
            self.exec_main_status = status
            self.main_pid = 0
            self.active = "failed"
            self.result = "start-limit-hit"

    # -- Systemctl interface -----------------------------------------------
    def action(self, verb, unit, timeout):
        assert unit in {BRIDGE, BT}
        self.calls.append((verb, unit))
        if verb == self.fail_verb:
            raise self.rec.ServiceManagerError(verb)
        if unit == BRIDGE and verb == "stop" and not self.stop_hangs:
            self.main_pid = 0
            self.active = "inactive"
        if unit == BRIDGE and verb == "reset-failed":
            self.result = "success"
        if unit == BRIDGE and verb == "start":
            self.main_pid = 500
            self.n_restarts = 0  # an explicit start begins a new count
            self.active = "active"
            if self.after_start:
                self.after_start()

    def show(self, unit):
        assert unit in {BRIDGE, BT}
        if unit == BT:
            return {"ActiveState": "active", "MainPID": "77"}
        return {
            "ActiveState": self.active,
            "MainPID": str(self.main_pid),
            "Result": self.result,
            "ExecMainStatus": str(self.exec_main_status),
            "ExecMainPID": str(self.exec_main_pid),
            "NRestarts": str(self.n_restarts),
        }

    @property
    def mutations(self):
        return list(self.calls)


def record(rec, manager, instance, *, phase="discovering", error="none", alive=True,
           pid=None, updated=None):
    return rec.RecordView(
        instance_id=instance,
        pid=manager.main_pid if pid is None else pid,
        phase=phase,
        error=error,
        ever_ready=phase == "ready",
        ready_since=None,
        updated=5_000.0 if updated is None else updated,
        alive=alive,
    )


def run(rec, settings, manager, fake, reader=lambda: None):
    return rec.run_once(settings, manager, fake.clock, reader)


def ble_loop(rec, settings, manager, fake, exits=2, reader=lambda: None):
    """The bridge exits with 76 repeatedly; the timer runs after each exit."""
    result = None
    for _ in range(exits):
        manager.bridge_exits(76)
        fake.advance(70)
        result = run(rec, settings, manager, fake, reader)
    return result


def ready_after_start(rec, manager, holder):
    def on_start():
        holder["record"] = record(rec, manager, NEW, phase="ready")

    manager.after_start = on_start
    return lambda: holder.get("record")


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
    assert settings.bridge_unit == BRIDGE
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
        "mqtt_reconnects": 3,
        "ready_since_monotonic": 10.0,
        "updated_monotonic": 20.0,
    }
    data.update(overrides)
    return json.dumps(data).encode()


def parse(rec, raw, ticks=999):
    return rec.parse_record(raw, boot_id=BOOT, start_ticks_of=lambda pid: ticks)


def test_valid_live_record(rec):
    parsed = parse(rec, record_bytes())
    assert parsed.alive
    assert parsed.mqtt_reconnects == 3


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
        record_bytes(mqtt_reconnects=-1),
        b"not json",
        b"[]",
        b"{" + b" " * 5000 + b"}",
    ],
)
def test_untrusted_record_fails_closed(rec, raw):
    assert parse(rec, raw) is None


def test_record_reader_refuses_symlinks_and_fifos(rec, tmp_path):
    real = tmp_path / "real.json"
    real.write_bytes(record_bytes())
    link = tmp_path / "link.json"
    link.symlink_to(real)
    fifo = tmp_path / "fifo.json"
    os.mkfifo(fifo)

    assert rec.read_record_bytes(real) == record_bytes()
    assert rec.read_record_bytes(link) is None
    assert rec.read_record_bytes(fifo) is None  # returns at once, no hang
    assert rec.read_record_bytes(tmp_path / "missing.json") is None


# ---------------------------------------------------------------------------
# Decisions and full recovery
# ---------------------------------------------------------------------------


def test_repeated_ble_exits_do_exactly_stop_restart_start(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    holder = {}
    reader = ready_after_start(rec, manager, holder)

    manager.bridge_exits(76)
    fake.advance(70)
    assert run(rec, settings, manager, fake, reader) == "no_repeated_ble_failure"
    manager.bridge_exits(76)
    fake.advance(70)
    assert run(rec, settings, manager, fake, reader) == "recovered"

    assert manager.mutations == [("stop", BRIDGE), ("restart", BT), ("start", BRIDGE)]
    state = rec.load_state(settings)
    assert state.ble_failures == []
    assert len(state.resets) == 1


def test_failures_missed_between_timer_runs_are_still_counted(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    holder = {}
    reader = ready_after_start(rec, manager, holder)
    manager.bridge_exits(76)
    manager.bridge_exits(76)
    manager.bridge_exits(76)
    fake.advance(200)
    assert run(rec, settings, manager, fake, reader) == "recovered"


def test_start_limit_hit_bridge_is_recovered_and_reset_failed(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    holder = {}
    reader = ready_after_start(rec, manager, holder)
    manager.bridge_exits(76)
    fake.advance(70)
    run(rec, settings, manager, fake, reader)
    # systemd gives up: the last exit is not an automatic restart.
    manager.bridge_exits(76, restarted=False)
    fake.advance(70)
    assert run(rec, settings, manager, fake, reader) == "recovered"
    assert manager.mutations == [
        ("stop", BRIDGE),
        ("restart", BT),
        ("reset-failed", BRIDGE),
        ("start", BRIDGE),
    ]


def test_same_exit_is_never_counted_twice(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    manager.bridge_exits(76)
    for _ in range(5):
        fake.advance(60)
        run(rec, settings, manager, fake)
    assert manager.calls == []
    assert len(rec.load_state(settings).ble_failures) == 1


@pytest.mark.parametrize("status", [75, 78, 79, 1, 0])
def test_non_ble_exits_never_reset_bluetooth(rec, settings, status):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    for _ in range(6):
        manager.bridge_exits(status)
        fake.advance(70)
        run(rec, settings, manager, fake)
    assert manager.calls == []


def test_mqtt_outage_and_ha_consumer_fault_never_reset(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    # The bridge stays up and reports ready (MQTT reconnects are internal;
    # Home Assistant consumer faults are invisible here).
    reader = lambda: record(rec, manager, NEW, phase="ready")  # noqa: E731
    for _ in range(10):
        fake.advance(60)
        run(rec, settings, manager, fake, reader)
    assert manager.calls == []


def test_forged_record_of_another_process_is_ignored(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    rec.save_state(settings, rec.State(recovery_failed=True, reason="budget_exhausted"))
    # pid 1 really exists and its start time matches, but it is not MainPID.
    forged = lambda: record(rec, manager, NEW, phase="ready", pid=1)  # noqa: E731
    for _ in range(100):
        fake.advance(60)
        run(rec, settings, manager, fake, forged)
    assert rec.load_state(settings).recovery_failed


def test_latch_clears_only_after_the_supervisor_observed_a_long_ready(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    rec.save_state(settings, rec.State(recovery_failed=True, reason="budget_exhausted"))
    ready = lambda: record(rec, manager, NEW, phase="ready")  # noqa: E731
    run(rec, settings, manager, fake, ready)
    fake.advance(settings.healthy_reset_seconds - 60)
    run(rec, settings, manager, fake, ready)
    assert rec.load_state(settings).recovery_failed
    fake.advance(120)
    run(rec, settings, manager, fake, ready)
    assert rec.load_state(settings).recovery_failed is False


def test_new_process_does_not_refill_the_budget(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    holder = {}
    reader = ready_after_start(rec, manager, holder)
    assert ble_loop(rec, settings, manager, fake, reader=reader) == "recovered"
    assert len(manager.calls) == 3

    holder.clear()  # the new process now fails too
    manager.after_start = None
    assert ble_loop(rec, settings, manager, fake) == "budget_exhausted"
    assert len(manager.calls) == 3  # no second reset inside 10 minutes
    state = rec.load_state(settings)
    assert state.recovery_failed
    assert state.reason == "budget_exhausted"


def test_two_resets_per_thirty_minutes_then_latch(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    holder = {}
    results = []
    for _ in range(3):
        reader = ready_after_start(rec, manager, holder)
        holder.clear()
        results.append(ble_loop(rec, settings, manager, fake, reader=reader))
        fake.advance(601)
    assert results == ["recovered", "recovered", "budget_exhausted"]
    assert [call for call in manager.calls if call[0] == "restart"] == [
        ("restart", BT)
    ] * 2


def test_forward_clock_jump_does_not_refill_the_budget(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    holder = {}
    assert (
        ble_loop(rec, settings, manager, fake, reader=ready_after_start(rec, manager, holder))
        == "recovered"
    )
    holder.clear()
    manager.after_start = None
    fake.wall_now += 7 * 24 * 3600  # NTP steps the wall clock forward a week
    assert ble_loop(rec, settings, manager, fake) == "budget_exhausted"
    assert len(manager.calls) == 3


def test_failure_mid_recovery_is_recovery_failed_and_restarts_bridge(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake, fail_verb="restart")

    assert ble_loop(rec, settings, manager, fake) == "recovery_failed"
    assert manager.mutations == [
        ("stop", BRIDGE),
        ("restart", BT),
        ("start", BRIDGE),  # cleanup: never left stopped
    ]
    state = rec.load_state(settings)
    assert state.recovery_failed
    assert state.reason == "service_manager_restart"


def test_bridge_that_does_not_stop_blocks_bluetooth_reset(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake, stop_hangs=True)

    assert ble_loop(rec, settings, manager, fake) == "recovery_failed"
    assert ("restart", BT) not in manager.calls
    assert rec.load_state(settings).reason == "bridge_stop_timeout"


def test_unverified_recovery_is_not_success(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    still_failing = lambda: record(rec, manager, NEW, phase="discovering")  # noqa: E731

    assert ble_loop(rec, settings, manager, fake, reader=still_failing) == "verify_failed"
    state = rec.load_state(settings)
    assert state.last_result == "verify_failed"
    assert len(state.resets) == 1


def test_ready_record_of_a_different_process_never_verifies(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    stale = lambda: record(rec, manager, NEW, phase="ready", pid=4242)  # noqa: E731

    assert ble_loop(rec, settings, manager, fake, reader=stale) == "verify_failed"


def test_live_process_stuck_in_discovery_counts_as_ble_failure(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    holder = {}
    stuck = {"record": record(rec, manager, OLD, updated=fake.mono_now)}

    def reader():
        return holder.get("record") or stuck["record"]

    manager.after_start = lambda: holder.update(
        record=record(rec, manager, NEW, phase="ready")
    )
    fake.advance(rec.STUCK_SECONDS - 10)
    assert run(rec, settings, manager, fake, reader) == "no_repeated_ble_failure"
    fake.advance(11)
    # A process stuck that long has missed many retry cycles: that alone is
    # a repeated fault, and it never exits on its own to add more evidence.
    assert run(rec, settings, manager, fake, reader) == "recovered"


def test_bridge_systemd_gave_up_on_for_other_reasons_gets_one_plain_restart(
    rec, settings
):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    manager.bridge_exits(79, restarted=False)  # e.g. network not found
    assert run(rec, settings, manager, fake) == "bridge_restarted"
    assert manager.mutations == [("reset-failed", BRIDGE), ("start", BRIDGE)]

    manager.bridge_exits(79, restarted=False)
    fake.advance(600)
    assert run(rec, settings, manager, fake) == "bridge_failed_restart_budget_used"
    assert len(manager.calls) == 2
    assert ("restart", BT) not in manager.calls


def test_configuration_failure_is_never_restarted(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    manager.bridge_exits(78, restarted=False)
    for _ in range(5):
        fake.advance(3600)
        run(rec, settings, manager, fake)
    assert manager.calls == []


def test_corrupt_state_fails_closed(rec, settings):
    settings.state_dir.mkdir()
    (settings.state_dir / "state.json").write_text("{not json")
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)

    ble_loop(rec, settings, manager, fake, exits=4)

    assert manager.calls == []
    assert rec.load_state(settings).reason == "state_unreadable"


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def test_held_lock_means_no_second_operation(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    manager.bridge_exits(76)
    manager.bridge_exits(76)
    with rec.exclusive_lock(settings) as acquired:
        assert acquired
        result = run(rec, settings, manager, fake)
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

    manager = BlockingManager(rec, settings, fake)
    holder = {}
    reader = ready_after_start(rec, manager, holder)
    manager.bridge_exits(76)
    run(rec, settings, manager, fake, reader)
    manager.bridge_exits(76)
    results = []

    def trigger():
        results.append(run(rec, settings, manager, fake, reader))

    first = threading.Thread(target=trigger)
    first.start()
    assert entered.wait(5)
    second = threading.Thread(target=trigger)
    second.start()
    second.join(5)
    gate.set()
    first.join(5)

    assert sorted(results) == ["locked", "recovered"]
    assert [call for call in manager.calls if call[0] == "restart"] == [("restart", BT)]
    assert run(rec, settings, manager, fake, reader) == "bridge_ready"


# ---------------------------------------------------------------------------
# The real systemctl wrapper (with a fake ``run``)
# ---------------------------------------------------------------------------


def test_systemctl_wrapper_uses_fixed_argv_and_allowlist(rec):
    calls = []

    def run_fake(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(
            argv, 0, stdout="MainPID=0\nExecMainStatus=76\nOther=x\n"
        )

    manager = rec.Systemctl(
        frozenset({BRIDGE, BT}), run=run_fake, executable="/usr/bin/systemctl"
    )
    manager.action("restart", BT, 5)
    assert manager.show(BRIDGE) == {"MainPID": "0", "ExecMainStatus": "76"}
    for verb, unit in (
        ("restart", "ssh.service"),
        ("restart", "bluetooth.service; reboot"),
        ("reboot", BT),
        ("isolate", BRIDGE),
    ):
        with pytest.raises(rec.ServiceManagerError):
            manager.action(verb, unit, 5)

    assert calls[0][0] == ["/usr/bin/systemctl", "restart", BT]
    assert calls[1][0][:3] == ["/usr/bin/systemctl", "show", BRIDGE]
    assert all("shell" not in kwargs for _, kwargs in calls)
    assert len(calls) == 2


def test_systemctl_failure_and_timeout_raise(rec):
    def failing(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 1, stdout="")

    def hanging(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, 1)

    for run_fake in (failing, hanging):
        manager = rec.Systemctl(frozenset({BT}), run=run_fake)
        with pytest.raises(rec.ServiceManagerError):
            manager.action("restart", BT, 1)


def test_unreadable_unit_state_does_nothing(rec, settings):
    class Broken(FakeManager):
        def show(self, unit):
            raise self.rec.ServiceManagerError("show")

    manager = Broken(rec, settings, FakeClock(rec))
    assert run(rec, settings, manager, FakeClock(rec)) == "systemd_unavailable"
    assert manager.calls == []


def test_operator_reset_clears_the_latch(rec, settings, monkeypatch):
    rec.save_state(settings, rec.State(recovery_failed=True, reason="budget_exhausted"))
    monkeypatch.setenv("RECOVERY_STATE_DIR", str(settings.state_dir))
    monkeypatch.setenv("RECOVERY_HEALTH_RECORD", str(settings.health_record))

    assert rec.main(["reset"]) == 0

    state = rec.load_state(settings)
    assert state.recovery_failed is False
    assert state.last_result == "operator_reset"


def test_exit_hook_rejects_unknown_values(rec, settings):
    fake = FakeClock(rec)
    for env in (
        {},
        {"EXIT_CODE": "exited"},
        {"EXIT_CODE": "exited", "EXIT_STATUS": "76; reboot"},
        {"EXIT_CODE": "weird", "EXIT_STATUS": "76"},
        {"EXIT_CODE": "exited", "EXIT_STATUS": "x" * 40},
    ):
        assert rec.record_exit(settings, env, fake.clock) is False
    assert rec.consume_exits(settings) == []


def test_signal_exits_are_never_ble_failures(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    for _ in range(4):
        rec.record_exit(settings, {"EXIT_CODE": "killed", "EXIT_STATUS": "TERM"}, fake.clock)
        fake.advance(70)
        run(rec, settings, manager, fake)
    assert manager.calls == []


def test_final_exit_before_start_limit_is_counted_once(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    for _ in range(4):
        manager.bridge_exits(1)
    manager.bridge_exits(76, restarted=False)
    fake.advance(70)
    # A single BLE failure (with four unrelated ones) is not a repeated BLE fault.
    assert run(rec, settings, manager, fake) == "bridge_restarted"
    assert ("restart", BT) not in manager.calls
    assert len(rec.load_state(settings).ble_failures) == 1


def test_main_record_exit_never_fails_the_stop(rec, settings, monkeypatch):
    monkeypatch.setenv("RECOVERY_STATE_DIR", str(settings.state_dir))
    monkeypatch.setenv("EXIT_CODE", "exited")
    monkeypatch.setenv("EXIT_STATUS", "76")
    assert rec.main(["record-exit"]) == 0
    assert len(rec.consume_exits(settings)) == 1
    monkeypatch.setenv("EXIT_STATUS", "bad value")
    assert rec.main(["record-exit"]) == 0


def test_exit_caused_by_the_recovery_stop_is_not_a_failure(rec, settings):
    fake = FakeClock(rec)
    manager = FakeManager(rec, settings, fake)
    holder = {}
    reader = ready_after_start(rec, manager, holder)
    real_action = manager.action

    def action(verb, unit, timeout):
        real_action(verb, unit, timeout)
        if verb == "stop" and unit == BRIDGE:
            # Worst case: the stopped bridge's disconnect wedged and it left 76.
            rec.record_exit(
                settings, {"EXIT_CODE": "exited", "EXIT_STATUS": "76"}, fake.clock
            )

    manager.action = action
    assert ble_loop(rec, settings, manager, fake, reader=reader) == "recovered"
    fake.advance(60)
    run(rec, settings, manager, fake, reader)
    assert rec.load_state(settings).ble_failures == []
