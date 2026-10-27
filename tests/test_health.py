"""
Home Assistant side of the v1 health protocol (issue #26).

Pure decision tests drive HealthObserver with a fake monotonic clock. The
integration tests run the real config entry on Home Assistant's MQTT test
double, so subscriptions, entity callbacks and publishes are the real ones.
"""

import json
from datetime import timedelta
from functools import partial
from unittest.mock import patch

import pytest
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_mqtt_message,
    async_fire_time_changed,
)
from test_integration import unit_payload

import custom_components.casambi_mqtt as integration
from custom_components.casambi_mqtt import health_protocol as hp
from custom_components.casambi_mqtt.const import CONF_NETWORK_NAME, DOMAIN
from custom_components.casambi_mqtt.diagnostics import (
    async_get_config_entry_diagnostics,
)
from homeassistant.data_entry_flow import InvalidData
from homeassistant.util import dt as dt_util

from custom_components.casambi_mqtt.health import (
    CONF_PROBE_DEADLINE_SECONDS,
    CONF_PROBE_INTERVAL_SECONDS,
    CONF_STALE_SECONDS,
    BridgeState,
    HealthObserver,
    HealthSettings,
    ProbeKind,
    ProbeOutcome,
)

EVENT_PREFIX = "casambi/test/events/"
INSTANCE_A = "a" * 32
INSTANCE_B = "b" * 32


class FakeClock:
    def __init__(self, now=10_000.0):
        self.now = now

    def __call__(self):
        return self.now


def status(
    instance=INSTANCE_A,
    sequence=1,
    phase=hp.PHASE_READY,
    error=hp.ERROR_NONE,
    time="2026-10-27T12:00:00Z",
    units=2,
):
    return hp.BridgeStatus(
        instance_id=instance,
        sequence=sequence,
        phase=phase,
        ble=hp.LINK_CONNECTED,
        mqtt=hp.LINK_CONNECTED,
        uptime_s=5,
        snapshot=hp.SnapshotSummary(generation=1, units=units, scenes=0, failed=0),
        error=error,
        time=time,
    )


def reply(
    request_id,
    instance=INSTANCE_A,
    reply_type=hp.REPLY_STATUS,
    result=hp.RESULT_OK,
    units=2,
):
    current = status(instance=instance, sequence=99)
    if reply_type == hp.REPLY_SNAPSHOT:
        return hp.HealthReply(
            reply_type,
            request_id,
            instance,
            result,
            current,
            units=units,
            scenes=0,
            failed=0 if result == hp.RESULT_OK else 1,
            generation=1,
        ).to_json()
    return hp.HealthReply(reply_type, request_id, instance, result, current).to_json()


def observer(clock, **options):
    ids = iter(f"req-{index}" for index in range(1000))
    return HealthObserver(
        HealthSettings.from_options(options),
        EVENT_PREFIX,
        monotonic=clock,
        token=lambda: next(ids),
    )


def healthy_observer(clock):
    obs = observer(clock)
    obs.on_status(status().to_json(), retain=False)
    return obs


# ---------------------------------------------------------------------------
# Bridge status freshness (fake clock)
# ---------------------------------------------------------------------------


def test_startup_grace_then_unknown_without_any_status():
    clock = FakeClock()
    obs = observer(clock)
    assert obs.bridge_state() is BridgeState.STARTING
    clock.now += 89
    assert obs.bridge_state() is BridgeState.STARTING
    clock.now += 2
    assert obs.bridge_state() is BridgeState.UNKNOWN


def test_live_ready_status_is_healthy_until_stale():
    clock = FakeClock()
    obs = observer(clock)
    obs.on_status(status().to_json(), retain=False)
    assert obs.bridge_state() is BridgeState.HEALTHY
    clock.now += 120
    assert obs.bridge_state() is BridgeState.HEALTHY
    clock.now += 1
    assert obs.bridge_state() is BridgeState.UNAVAILABLE


def test_freshness_ignores_bridge_wall_clock_jumps():
    clock = FakeClock()
    obs = observer(clock)
    obs.on_status(status(time="1970-01-01T00:00:00Z").to_json(), retain=False)
    assert obs.bridge_state() is BridgeState.HEALTHY
    clock.now += 200
    obs.on_status(
        status(sequence=2, time="2099-12-31T23:59:59Z").to_json(), retain=False
    )
    assert obs.bridge_state() is BridgeState.HEALTHY


@pytest.mark.parametrize("retained_phase", [hp.PHASE_READY, hp.PHASE_OFFLINE])
def test_retained_status_never_makes_the_bridge_healthy(retained_phase):
    clock = FakeClock()
    obs = observer(clock)
    obs.on_status(status(phase=retained_phase).to_json(), retain=True)
    assert obs.bridge_state() is BridgeState.STARTING
    clock.now += 91
    # Retained history may be left over from a rolled-back v1 bridge.
    assert obs.bridge_state() is BridgeState.UNKNOWN


def test_rolled_back_bridge_with_leftover_retained_status_is_unsupported():
    clock = FakeClock()
    obs = observer(clock)
    obs.on_status(status().to_json(), retain=True)
    clock.now += 90
    assert obs.tick().kind is ProbeKind.STATUS
    clock.now += 21
    obs.tick()
    assert obs.bridge_state() is BridgeState.UNSUPPORTED


def test_older_heartbeat_after_last_will_cannot_restore_healthy_but_reconnect_can():
    clock = FakeClock()
    obs = observer(clock)
    obs.on_status(status(sequence=50).to_json(), retain=False)
    will = status(phase=hp.PHASE_OFFLINE, error=hp.ERROR_CONNECTION_LOST)
    obs.on_status(json.dumps(will.as_dict() | {"sequence": None}), retain=False)
    assert obs.bridge_state() is BridgeState.UNAVAILABLE
    for sequence in (3, 50):
        obs.on_status(status(sequence=sequence).to_json(), retain=False)
        assert obs.bridge_state() is BridgeState.UNAVAILABLE
    # The same process reconnecting to MQTT continues its sequence and is
    # live again; only older or repeated heartbeats are rejected.
    obs.on_status(status(sequence=51).to_json(), retain=False)
    assert obs.bridge_state() is BridgeState.HEALTHY


@pytest.mark.parametrize("version", [1.0, True, "1"])
def test_protocol_version_must_be_the_integer_one(version):
    payload = json.dumps(status().as_dict() | {"protocol_version": version})
    assert hp.parse_status(payload) is None


def test_mqtt_reconnect_replaying_retained_status_does_not_refresh():
    clock = FakeClock()
    obs = observer(clock)
    obs.on_status(status(sequence=1).to_json(), retain=False)
    clock.now += 121
    assert obs.bridge_state() is BridgeState.UNAVAILABLE
    # A resubscribe after reconnect replays the newest retained heartbeat.
    obs.on_status(status(sequence=5).to_json(), retain=True)
    assert obs.bridge_state() is BridgeState.UNAVAILABLE


def test_sequence_regression_is_ignored_and_does_not_refresh():
    clock = FakeClock()
    obs = observer(clock)
    obs.on_status(status(sequence=10).to_json(), retain=False)
    clock.now += 100
    obs.on_status(status(sequence=10).to_json(), retain=False)
    obs.on_status(status(sequence=3).to_json(), retain=False)
    clock.now += 21
    assert obs.bridge_state() is BridgeState.UNAVAILABLE
    assert obs.bridge.ignored == 2


def test_new_instance_replaces_old_and_old_instance_is_ignored_afterwards():
    clock = FakeClock()
    obs = observer(clock)
    obs.on_status(status(INSTANCE_A, sequence=50).to_json(), retain=False)
    obs.on_status(
        status(INSTANCE_B, sequence=1, phase=hp.PHASE_STARTING).to_json(),
        retain=False,
    )
    # The new process is starting: it never inherits the old one's ready.
    clock.now += 91
    assert obs.bridge_state() is BridgeState.STARTING
    obs.on_status(status(INSTANCE_A, sequence=51).to_json(), retain=False)
    assert obs.bridge.instance_id == INSTANCE_B
    assert obs.bridge_state() is BridgeState.STARTING


def test_last_will_offline_is_unavailable_immediately():
    clock = FakeClock()
    obs = observer(clock)
    obs.on_status(status().to_json(), retain=False)
    will = status(phase=hp.PHASE_OFFLINE, error=hp.ERROR_CONNECTION_LOST)
    will_dict = will.as_dict() | {"sequence": None}
    obs.on_status(json.dumps(will_dict), retain=False)
    assert obs.bridge_state() is BridgeState.UNAVAILABLE


def test_degraded_status_is_not_healthy():
    clock = FakeClock()
    obs = observer(clock)
    obs.on_status(
        status(phase=hp.PHASE_DEGRADED, error=hp.ERROR_BLE_DISCONNECTED).to_json(),
        retain=False,
    )
    assert obs.bridge_state() is BridgeState.DEGRADED


def test_newer_protocol_version_is_unsupported_not_unhealthy():
    clock = FakeClock()
    obs = observer(clock)
    obs.on_status(
        json.dumps(status().as_dict() | {"protocol_version": 2}), retain=False
    )
    clock.now += 91
    assert obs.bridge_state() is BridgeState.UNSUPPORTED


@pytest.mark.parametrize(
    "payload",
    [
        "",
        "not json",
        "[]",
        json.dumps(status().as_dict() | {"extra": 1}),
        json.dumps(status().as_dict() | {"phase": "rebooting"}),
        json.dumps(status().as_dict() | {"error": "secret text"}),
        json.dumps(status().as_dict() | {"instance_id": "short"}),
        json.dumps(status().as_dict() | {"sequence": True}),
        json.dumps(status().as_dict() | {"sequence": None}),
    ],
)
def test_malformed_status_fails_closed(payload):
    clock = FakeClock()
    obs = observer(clock)
    obs.on_status(payload, retain=False)
    assert obs.bridge.status is None


# ---------------------------------------------------------------------------
# Correlated probes
# ---------------------------------------------------------------------------


def test_status_probe_waits_for_grace_and_respects_interval():
    clock = FakeClock()
    obs = observer(clock)
    assert obs.tick() is None
    clock.now += 90
    probe = obs.tick()
    assert probe is not None
    assert probe.kind is ProbeKind.STATUS
    # At most one probe at a time.
    assert obs.tick() is None
    obs.on_reply(reply(probe.request_id))
    assert obs.last_result[ProbeKind.STATUS].outcome is ProbeOutcome.OK
    clock.now += 299
    assert obs.tick() is None
    clock.now += 1
    assert obs.tick().kind is ProbeKind.STATUS


@pytest.mark.parametrize(
    "bad_reply",
    [
        lambda request_id: reply("someone-else"),
        lambda request_id: reply(request_id, instance=INSTANCE_B),
        lambda request_id: reply(request_id, reply_type=hp.REPLY_SNAPSHOT),
        lambda request_id: json.dumps(
            json.loads(reply(request_id)) | {"request_id": "x" * 65}
        ),
        lambda request_id: json.dumps(
            {k: v for k, v in json.loads(reply(request_id)).items() if k != "request_id"}
        ),
        lambda request_id: "garbage",
    ],
)
def test_wrong_replies_never_complete_a_probe(bad_reply):
    clock = FakeClock()
    obs = healthy_observer(clock)
    probe = obs.start_probe(ProbeKind.STATUS)

    obs.on_reply(bad_reply(probe.request_id))

    assert ProbeKind.STATUS not in obs.last_result
    assert obs.probes.rejected_replies == 1
    clock.now += 21
    obs.tick()
    assert obs.last_result[ProbeKind.STATUS].outcome is ProbeOutcome.NO_REPLY


def test_late_reply_is_rejected():
    clock = FakeClock()
    obs = healthy_observer(clock)
    probe = obs.start_probe(ProbeKind.STATUS)
    clock.now += 21
    obs.on_reply(reply(probe.request_id))
    assert ProbeKind.STATUS not in obs.last_ok


def test_reply_after_probe_expired_cannot_complete_a_later_probe():
    clock = FakeClock()
    obs = healthy_observer(clock)
    first = obs.start_probe(ProbeKind.STATUS)
    clock.now += 21
    obs.tick()
    clock.now += 300
    second = obs.tick()
    obs.on_reply(reply(first.request_id))
    assert obs.probes.pending is second
    assert ProbeKind.STATUS not in obs.last_ok


def test_old_bridge_without_protocol_is_unsupported_not_failing():
    clock = FakeClock()
    obs = observer(clock)
    clock.now += 90
    obs.tick()
    clock.now += 21
    obs.tick()
    assert obs.bridge_state() is BridgeState.UNSUPPORTED
    # No consumer probe is ever started against an unsupported bridge.
    clock.now += 10_000
    probe = obs.tick()
    assert probe is None or probe.kind is ProbeKind.STATUS


def test_consumer_probe_ok_on_unchanged_processed_state():
    clock = FakeClock()
    obs = healthy_observer(clock)
    probe = obs.start_probe(ProbeKind.CONSUMER)
    for topic in (f"{EVENT_PREFIX}a", f"{EVENT_PREFIX}b"):
        obs.on_raw_unit(topic, "same payload as before")
        obs.on_processed_unit(topic)
    obs.on_reply(reply(probe.request_id, reply_type=hp.REPLY_SNAPSHOT, units=2))
    assert obs.last_result[ProbeKind.CONSUMER].outcome is ProbeOutcome.OK


def test_frozen_consumer_is_stale_although_raw_and_status_work():
    clock = FakeClock()
    obs = healthy_observer(clock)
    status_probe = obs.start_probe(ProbeKind.STATUS)
    obs.on_reply(reply(status_probe.request_id))
    assert obs.last_result[ProbeKind.STATUS].outcome is ProbeOutcome.OK

    probe = obs.start_probe(ProbeKind.CONSUMER)
    obs.on_raw_unit(f"{EVENT_PREFIX}a", "x")
    obs.on_raw_unit(f"{EVENT_PREFIX}b", "x")
    obs.on_reply(reply(probe.request_id, reply_type=hp.REPLY_SNAPSHOT, units=2))
    assert ProbeKind.CONSUMER not in obs.last_result
    clock.now += 21
    obs.tick()

    result = obs.last_result[ProbeKind.CONSUMER]
    assert result.outcome is ProbeOutcome.CONSUMER_STALE
    assert (result.raw_units, result.processed_units) == (2, 0)
    assert obs.bridge_state() is BridgeState.HEALTHY
    assert ProbeKind.CONSUMER not in obs.last_ok


def test_snapshot_without_units_never_proves_consumption():
    clock = FakeClock()
    obs = healthy_observer(clock)
    probe = obs.start_probe(ProbeKind.CONSUMER)
    obs.on_reply(reply(probe.request_id, reply_type=hp.REPLY_SNAPSHOT, units=0))
    assert obs.last_result[ProbeKind.CONSUMER].outcome is ProbeOutcome.NO_UNITS
    assert ProbeKind.CONSUMER not in obs.last_ok


def test_partly_rejected_units_are_partial_not_stale():
    clock = FakeClock()
    obs = healthy_observer(clock)
    probe = obs.start_probe(ProbeKind.CONSUMER)
    for topic in ("a", "b", "c"):
        obs.on_raw_unit(f"{EVENT_PREFIX}{topic}", "x")
    obs.on_processed_unit(f"{EVENT_PREFIX}a")
    obs.on_reply(reply(probe.request_id, reply_type=hp.REPLY_SNAPSHOT, units=3))
    clock.now += 21
    obs.tick()
    assert (
        obs.last_result[ProbeKind.CONSUMER].outcome is ProbeOutcome.CONSUMER_PARTIAL
    )


def test_missing_raw_delivery_is_not_blamed_on_the_consumer():
    clock = FakeClock()
    obs = healthy_observer(clock)
    probe = obs.start_probe(ProbeKind.CONSUMER)
    obs.on_raw_unit(f"{EVENT_PREFIX}a", "x")
    obs.on_reply(reply(probe.request_id, reply_type=hp.REPLY_SNAPSHOT, units=2))
    clock.now += 21
    obs.tick()
    assert (
        obs.last_result[ProbeKind.CONSUMER].outcome
        is ProbeOutcome.DELIVERY_INCOMPLETE
    )


def test_rate_limited_snapshot_is_busy_not_no_reply():
    clock = FakeClock()
    obs = healthy_observer(clock)
    probe = obs.start_probe(ProbeKind.CONSUMER)
    obs.on_reply(
        reply(
            probe.request_id,
            reply_type=hp.REPLY_SNAPSHOT,
            result=hp.RESULT_RATE_LIMITED,
            units=0,
        )
    )
    assert obs.last_result[ProbeKind.CONSUMER].outcome is ProbeOutcome.BRIDGE_BUSY


def test_failed_snapshot_reply_is_not_success():
    clock = FakeClock()
    obs = healthy_observer(clock)
    probe = obs.start_probe(ProbeKind.CONSUMER)
    obs.on_reply(
        reply(
            probe.request_id,
            reply_type=hp.REPLY_SNAPSHOT,
            result=hp.RESULT_FAILED,
        )
    )
    assert obs.last_result[ProbeKind.CONSUMER].outcome is ProbeOutcome.BRIDGE_FAILED


def test_legacy_tombstone_and_empty_payloads_are_not_units():
    clock = FakeClock()
    obs = healthy_observer(clock)
    obs.start_probe(ProbeKind.CONSUMER)
    obs.on_raw_unit(EVENT_PREFIX, "")
    obs.on_raw_unit(EVENT_PREFIX, "{}")
    obs.on_raw_unit(f"{EVENT_PREFIX}a", "")
    obs.on_processed_unit(EVENT_PREFIX)
    assert obs.consumer.raw_messages == 0
    assert obs.consumer.processed_messages == 0
    assert obs.probes.pending.raw_topics == set()


def test_consumer_probe_only_against_a_healthy_bridge():
    clock = FakeClock()
    obs = observer(clock)
    obs.on_status(status(phase=hp.PHASE_DEGRADED).to_json(), retain=False)
    clock.now += 90
    assert obs.tick().kind is ProbeKind.STATUS


def test_settings_are_validated():
    assert HealthSettings.from_options({}) == HealthSettings()
    settings = HealthSettings.from_options(
        {
            CONF_STALE_SECONDS: 30,
            CONF_PROBE_INTERVAL_SECONDS: 60,
            CONF_PROBE_DEADLINE_SECONDS: True,
        }
    )
    assert settings == HealthSettings()
    assert HealthSettings.from_options({CONF_STALE_SECONDS: 300}).stale_seconds == 300


# ---------------------------------------------------------------------------
# Real config entry on Home Assistant's MQTT test double
# ---------------------------------------------------------------------------


async def setup_entry(hass, clock, *, freeze_consumer=False):
    entry = MockConfigEntry(domain=DOMAIN, data={CONF_NETWORK_NAME: "test"}, version=2)
    entry.add_to_hass(hass)
    ids = iter(f"req-{index}" for index in range(1000))
    real_subscribe = integration.async_subscribe

    async def subscribe(hass_arg, topic, callback, qos):
        if freeze_consumer and getattr(callback, "__name__", "") == "event_processor":
            # The entity consumer's subscription silently never fires, while
            # every other subscription (raw listener included) still works.
            async def frozen(_msg):
                return None

            callback = frozen
        return await real_subscribe(hass_arg, topic, callback, qos)

    with (
        patch.object(
            integration,
            "HealthObserver",
            partial(HealthObserver, monotonic=clock, token=lambda: next(ids)),
        ),
        patch.object(integration, "async_subscribe", side_effect=subscribe),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


def published_commands(mqtt_mock):
    return [
        json.loads(call.args[1])
        for call in mqtt_mock.async_publish.call_args_list
        if call.args[0] == "casambi/test/commands"
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_real_consumer_handles_unchanged_state_and_probe_succeeds(
    hass, mqtt_mock
):
    clock = FakeClock()
    entry = await setup_entry(hass, clock)
    obs = entry.runtime_data.health
    async_fire_mqtt_message(hass, "casambi/test/status", status().to_json())
    await hass.async_block_till_done()

    probe = obs.start_probe(ProbeKind.CONSUMER)
    payload = unit_payload("u1", "addr")
    for _ in range(2):  # the same dimmer value twice
        async_fire_mqtt_message(hass, f"{EVENT_PREFIX}addr", payload)
        await hass.async_block_till_done()
    async_fire_mqtt_message(
        hass,
        "casambi/test/health/reply",
        reply(probe.request_id, reply_type=hp.REPLY_SNAPSHOT, units=1),
    )
    await hass.async_block_till_done()

    assert obs.last_result[ProbeKind.CONSUMER].outcome is ProbeOutcome.OK
    assert obs.consumer.processed_messages == 2
    assert hass.states.get("light.addressless_dimmer") is not None
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_frozen_real_consumer_is_detected(hass, mqtt_mock):
    clock = FakeClock()
    entry = await setup_entry(hass, clock, freeze_consumer=True)
    obs = entry.runtime_data.health
    async_fire_mqtt_message(hass, "casambi/test/status", status().to_json())
    await hass.async_block_till_done()
    status_probe = obs.start_probe(ProbeKind.STATUS)
    async_fire_mqtt_message(
        hass, "casambi/test/health/reply", reply(status_probe.request_id)
    )
    await hass.async_block_till_done()
    assert obs.last_result[ProbeKind.STATUS].outcome is ProbeOutcome.OK
    probe = obs.start_probe(ProbeKind.CONSUMER)

    async_fire_mqtt_message(hass, f"{EVENT_PREFIX}addr", unit_payload("u1", "addr"))
    async_fire_mqtt_message(
        hass,
        "casambi/test/health/reply",
        reply(probe.request_id, reply_type=hp.REPLY_SNAPSHOT, units=1),
    )
    await hass.async_block_till_done()
    clock.now += 21
    obs.tick()

    assert obs.last_result[ProbeKind.CONSUMER].outcome is ProbeOutcome.CONSUMER_STALE
    assert obs.consumer.raw_messages == 1
    assert obs.consumer.processed_messages == 0
    assert hass.states.get("light.addressless_dimmer") is None
    assert obs.bridge_state() is BridgeState.HEALTHY
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_health_paths_publish_zero_control_commands(hass, mqtt_mock):
    clock = FakeClock()
    entry = await setup_entry(hass, clock)
    async_fire_mqtt_message(hass, "casambi/test/status", status().to_json())
    await hass.async_block_till_done()
    kinds = set()
    for step in range(1, 7):
        clock.now += 1000
        async_fire_mqtt_message(
            hass, "casambi/test/status", status(sequence=step + 1).to_json()
        )
        await hass.async_block_till_done()
        # Drive the real interval timer without waiting on wall time.
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=6 * step))
        await hass.async_block_till_done()
        pending = entry.runtime_data.health.probes.pending
        if pending is not None:
            kinds.add(pending.kind)
            entry.runtime_data.health.probes.cancel()

    commands = published_commands(mqtt_mock)
    assert kinds == {ProbeKind.STATUS, ProbeKind.CONSUMER}
    assert len(commands) >= 2
    assert {command["action"] for command in commands} <= {
        hp.GET_STATUS_ACTION,
        hp.PUBLISH_ENTITIES_ACTION,
    }
    assert all("request_id" in command for command in commands)
    assert not any(
        call.args[0] != "casambi/test/commands"
        for call in mqtt_mock.async_publish.call_args_list
    )
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_diagnostics_are_privacy_limited(hass, mqtt_mock):
    clock = FakeClock()
    entry = await setup_entry(hass, clock)
    async_fire_mqtt_message(hass, "casambi/test/status", status().to_json())
    async_fire_mqtt_message(hass, f"{EVENT_PREFIX}addr", unit_payload("u1", "addr"))
    await hass.async_block_till_done()
    entry.runtime_data.health.start_probe(ProbeKind.STATUS)

    diagnostics = await async_get_config_entry_diagnostics(hass, entry)
    text = json.dumps(diagnostics)

    assert diagnostics["health"]["bridge_state"] == "healthy"
    for private in ("casambi/test", "test/", INSTANCE_A, "req-0", "u1", "addr"):
        assert private not in text
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_unload_and_reload_do_not_accumulate_health_subscribers(
    hass, mqtt_mock
):
    clock = FakeClock()
    entry = await setup_entry(hass, clock)
    for _ in range(3):
        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done()
    obs = entry.runtime_data.health
    async_fire_mqtt_message(hass, f"{EVENT_PREFIX}addr", unit_payload("u1", "addr"))
    await hass.async_block_till_done()
    # One raw listener and one consumer after three reloads, not four each.
    assert obs.consumer.raw_messages == 1
    assert obs.consumer.processed_messages == 1
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_options_flow_validates_health_settings(hass, mqtt_mock):
    entry = await setup_entry(hass, FakeClock())
    flow = await hass.config_entries.options.async_init(entry.entry_id)
    schema_keys = {str(key) for key in flow["data_schema"].schema}
    assert {CONF_STALE_SECONDS, CONF_PROBE_INTERVAL_SECONDS} <= schema_keys

    with pytest.raises(InvalidData, match="health_probe_interval_seconds"):
        await hass.config_entries.options.async_configure(
            flow["flow_id"],
            {CONF_NETWORK_NAME: "test", CONF_PROBE_INTERVAL_SECONDS: 60},
        )

    flow = await hass.config_entries.options.async_init(entry.entry_id)
    result = await hass.config_entries.options.async_configure(
        flow["flow_id"],
        {CONF_NETWORK_NAME: "test", CONF_STALE_SECONDS: 300},
    )
    assert result["type"] == "create_entry"
    assert entry.options[CONF_STALE_SECONDS] == 300
    assert entry.options[CONF_PROBE_INTERVAL_SECONDS] == 300
    assert HealthSettings.from_options(entry.options).stale_seconds == 300
    await hass.async_block_till_done()
    # The options change reloaded only this entry with the new settings.
    assert entry.runtime_data.health.settings.stale_seconds == 300
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_network_rename_sets_the_entry_up_once_with_new_options(hass, mqtt_mock):
    entry = await setup_entry(hass, FakeClock())
    setups = []
    real_setup = integration.async_setup_entry

    async def counting_setup(hass_arg, entry_arg):
        setups.append(dict(entry_arg.options))
        return await real_setup(hass_arg, entry_arg)

    with patch.object(integration, "async_setup_entry", counting_setup):
        flow = await hass.config_entries.options.async_init(entry.entry_id)
        await hass.config_entries.options.async_configure(
            flow["flow_id"], {CONF_NETWORK_NAME: "renamed"}
        )
        await hass.async_block_till_done()

    assert len(setups) == 1
    assert setups[0][CONF_NETWORK_NAME] == "renamed"
    assert await hass.config_entries.async_unload(entry.entry_id)
