"""
Entry-scoped consumer supervision and self-recovery (issue #28).

Runs the real config entry on Home Assistant's MQTT test double. A fake
bridge answers GET_STATUS and correlated snapshots exactly like the real one;
"freezing" the consumer makes the entry's real ``event_processor``
subscription silently stop handling messages while every other subscription
(raw listener, status, replies) keeps working -- the shape of the 27.10.2026
incident. Monotonic time, wall time and the verification sleep are fake.
"""

import json
from contextlib import ExitStack
from functools import partial
from unittest.mock import patch

import pytest
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_mqtt_message,
)

import custom_components.casambi_mqtt as integration
from custom_components.casambi_mqtt import health_protocol as hp
from custom_components.casambi_mqtt.const import CONF_NETWORK_NAME, DOMAIN
from custom_components.casambi_mqtt.health import (
    CONF_RECOVERY_ENABLED,
    HealthObserver,
    ProbeKind,
)
from custom_components.casambi_mqtt.recovery import (
    DATA_RECOVERY,
    ConsumerState,
    RecoveryManager,
    recovery_issue_id,
    unsupported_issue_id,
)
from custom_components.casambi_mqtt.repairs import async_create_fix_flow
from homeassistant.config_entries import ConfigEntryState
from homeassistant.helpers import issue_registry as ir

INSTANCE = "a" * 32


def unit_payload(address, name, dimmer=40):
    return json.dumps(
        {
            "address": address,
            "device_id": 7,
            "is_on": dimmer > 0,
            "name": name,
            "online": True,
            "state": {"dimmer": dimmer},
            "uuid": f"uuid-{address}",
            "unit_type": {
                "id": 1,
                "manufacturer": "Casambi",
                "mode": "Dim",
                "model": "Test",
                "state_length": 1,
                "controls": [
                    {
                        "default": 0,
                        "length": 8,
                        "offset": 0,
                        "readonly": False,
                        "type": {"name": "DIMMER", "value": 0},
                    }
                ],
            },
        }
    )


def status_payload(sequence, phase=hp.PHASE_READY, error=hp.ERROR_NONE, units=2):
    return hp.BridgeStatus(
        instance_id=INSTANCE,
        sequence=sequence,
        phase=phase,
        ble=hp.LINK_CONNECTED,
        mqtt=hp.LINK_CONNECTED,
        uptime_s=1,
        snapshot=hp.SnapshotSummary(generation=1, units=units, scenes=0, failed=0),
        error=error,
        time=None,
    )


class Clock:
    def __init__(self):
        self.mono = 10_000.0
        self.wall = 2_000_000_000.0

    def monotonic(self):
        return self.mono

    def time(self):
        return self.wall

    def advance(self, seconds):
        self.mono += seconds
        self.wall += seconds


class FakeBridge:
    """Answers health requests like the real bridge; never actuates anything."""

    def __init__(self, hass, network, clock, units):
        self.hass = hass
        self.network = network
        self.clock = clock
        self.units = dict(units)
        self.sequence = 0
        self.answer = True
        self.phase = hp.PHASE_READY
        self.error = hp.ERROR_NONE
        self.commands = []

    def on_publish(self, topic, payload, *args, **kwargs):
        if topic != f"casambi/{self.network}/commands":
            return
        command = json.loads(payload)
        self.commands.append(command)
        if self.answer:
            self.hass.loop.call_soon(self._respond, command)

    def _status(self):
        self.sequence += 1
        return status_payload(
            self.sequence, self.phase, self.error, units=len(self.units)
        )

    def heartbeat(self):
        async_fire_mqtt_message(
            self.hass, f"casambi/{self.network}/status", self._status().to_json()
        )

    def _respond(self, command):
        request_id = command.get("request_id")
        reply_topic = f"casambi/{self.network}/health/reply"
        if command["action"] == hp.GET_STATUS_ACTION:
            reply = hp.HealthReply(
                hp.REPLY_STATUS, request_id, INSTANCE, hp.RESULT_OK, self._status()
            )
        elif command["action"] == hp.PUBLISH_ENTITIES_ACTION and request_id:
            for address, name in self.units.items():
                async_fire_mqtt_message(
                    self.hass,
                    f"casambi/{self.network}/events/{address}",
                    unit_payload(address, name),
                )
            reply = hp.HealthReply(
                hp.REPLY_SNAPSHOT,
                request_id,
                INSTANCE,
                hp.RESULT_OK,
                self._status(),
                units=len(self.units),
                scenes=0,
                failed=0,
                generation=1,
            )
        else:
            return
        async_fire_mqtt_message(self.hass, reply_topic, reply.to_json())


class Harness:
    """One entry, its fake bridge, a fake clock and a freezable consumer."""

    def __init__(self, hass, mqtt_mock, network="test", options=None, entry=None):
        self.hass = hass
        self.mqtt_mock = mqtt_mock
        self.clock = Clock()
        self.bridge = FakeBridge(
            hass,
            network,
            self.clock,
            {f"{network}-1": f"{network} one", f"{network}-2": f"{network} two"},
        )
        self.entry = entry or MockConfigEntry(
            domain=DOMAIN,
            data={CONF_NETWORK_NAME: network},
            options=options or {},
            version=2,
        )
        self.generation = 0
        self.frozen_generations = set()
        self.reloads = 0
        self.subscribe_calls = 0

    async def start(self, stack):
        if self.hass.config_entries.async_get_entry(self.entry.entry_id) is None:
            self.entry.add_to_hass(self.hass)
        self.manager = RecoveryManager(
            self.hass,
            self.entry.entry_id,
            wall=self.clock.time,
            monotonic=self.clock.monotonic,
            sleep=self._verify_sleep,
        )
        self.hass.data.setdefault(DOMAIN, {}).setdefault(DATA_RECOVERY, {})[
            self.entry.entry_id
        ] = self.manager
        ids = iter(f"{self.entry.entry_id[:6]}-{n}" for n in range(10_000))
        self.observer_factory = partial(
            HealthObserver, monotonic=self.clock.monotonic, token=lambda: next(ids)
        )
        assert await self.hass.config_entries.async_setup(self.entry.entry_id)
        await self.hass.async_block_till_done()

    def wrap(self, callback):
        """Count a subscription; make the entity consumer freezable."""
        self.subscribe_calls += 1
        if getattr(callback, "__name__", "") != "event_processor":
            return callback
        self.generation += 1
        generation = self.generation

        async def maybe_frozen(msg):
            if generation in self.frozen_generations:
                return None
            return await callback(msg)

        return maybe_frozen

    async def _verify_sleep(self, seconds):
        self.clock.advance(seconds)
        self.bridge.heartbeat()
        await self.hass.async_block_till_done()

    @property
    def runtime(self):
        return self.entry.runtime_data

    @property
    def coordinator(self):
        return self.runtime.health_coordinator

    async def settle(self):
        await self.hass.async_block_till_done()

    async def tick(self, seconds=0):
        self.clock.advance(seconds)
        self.bridge.heartbeat()
        await self.settle()
        self.runtime.health_monitor._on_tick()
        await self.settle()

    async def consumer_probe(self):
        """Run one correlated consumer probe to its conclusion."""
        self.coordinator.request_probe(ProbeKind.CONSUMER)
        await self.settle()
        if self.runtime.health.probes.pending is not None:
            await self.tick(21)
        return self.runtime.health.last_result[ProbeKind.CONSUMER]

    async def wait_recovery(self):
        task = self.manager.task
        if task is not None:
            await task
        await self.settle()


@pytest.fixture
def mqtt_bridge(hass, mqtt_mock):
    harnesses = []

    async def publish(*args, **kwargs):
        for harness in harnesses:
            harness.bridge.on_publish(*args, **kwargs)

    mqtt_mock.async_publish.side_effect = publish

    real_reload = hass.config_entries.async_reload

    async def counting_reload(entry_id):
        for item in harnesses:
            if item.entry.entry_id == entry_id:
                item.reloads += 1
        return await real_reload(entry_id)

    real_subscribe = integration.async_subscribe

    def harness_for(prefix):
        return next(h for h in harnesses if prefix.startswith(f"casambi/{h.bridge.network}/"))

    async def subscribe(hass_arg, topic, callback, qos):
        callback = harness_for(topic).wrap(callback)
        return await real_subscribe(hass_arg, topic, callback, qos)

    def observer(settings, event_prefix, **kwargs):
        return harness_for(event_prefix).observer_factory(settings, event_prefix)

    async def make(network="test", options=None, stack=None, entry=None):
        harness = Harness(hass, mqtt_mock, network, options, entry)
        if not harnesses:
            stack.enter_context(
                patch.object(hass.config_entries, "async_reload", counting_reload)
            )
            stack.enter_context(
                patch.object(integration, "async_subscribe", side_effect=subscribe)
            )
            stack.enter_context(patch.object(integration, "HealthObserver", observer))
        harnesses.append(harness)
        await harness.start(stack)
        return harness

    return make


async def healthy_start(harness):
    """Past the startup grace, live status, lights created and processed."""
    await harness.tick(91)
    result = await harness.consumer_probe()
    assert result.outcome.value == "ok"
    assert harness.coordinator.state is ConsumerState.HEALTHY


def light_ids(hass, network):
    return sorted(
        state.entity_id
        for state in hass.states.async_all("light")
        if state.attributes.get("friendly_name", "").startswith(network)
    )


def control_commands(harness):
    return [
        command
        for command in harness.bridge.commands
        if command["action"] not in {hp.GET_STATUS_ACTION, hp.PUBLISH_ENTITIES_ACTION}
        or "request_id" not in command
    ]


# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_frozen_consumer_is_recovered_by_reloading_only_its_entry(
    hass, mqtt_bridge
):
    with ExitStack() as stack:
        h = await mqtt_bridge(stack=stack)
        await healthy_start(h)
        before_ids = light_ids(hass, "test")
        assert len(before_ids) == 2

        h.frozen_generations.add(1)
        first = await h.consumer_probe()
        assert first.outcome.value == "consumer_stale"
        assert h.coordinator.state is ConsumerState.CONSUMER_STALE
        assert h.reloads == 0  # one proof is never enough

        # The re-check is not allowed before the confirmation interval.
        await h.tick(30)
        assert h.runtime.health.probes.pending is None
        await h.tick(60)
        assert h.runtime.health.probes.pending is not None
        await h.tick(21)
        await h.wait_recovery()

        assert h.reloads == 1
        assert h.entry.state is ConfigEntryState.LOADED
        assert h.manager.last_result == "recovered"
        assert h.coordinator.state is not ConsumerState.RECOVERY_FAILED
        # Same entity IDs, really available again (not a fresh unavailable).
        assert light_ids(hass, "test") == before_ids
        assert all(hass.states.get(entity).state != "unavailable" for entity in before_ids)
        assert control_commands(h) == []
        notifications = hass.states.async_all("persistent_notification")
        assert all("test" not in str(n.attributes) for n in notifications)
        await hass.config_entries.async_unload(h.entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_unchanged_dimmer_values_stay_healthy_without_reloads(hass, mqtt_bridge):
    with ExitStack() as stack:
        h = await mqtt_bridge(stack=stack)
        await healthy_start(h)
        for _ in range(5):
            await h.tick(300)
            assert (await h.consumer_probe()).outcome.value == "ok"
        assert h.reloads == 0
        assert h.coordinator.state is ConsumerState.HEALTHY
        await hass.config_entries.async_unload(h.entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
@pytest.mark.parametrize(
    ("scenario", "expected"),
    [
        ("broker_or_bridge_silent", ConsumerState.BRIDGE_UNAVAILABLE),
        ("ble_down", ConsumerState.BRIDGE_UNAVAILABLE),
        ("old_bridge", ConsumerState.UNSUPPORTED),
    ],
)
async def test_external_outages_never_reload(hass, mqtt_bridge, scenario, expected):
    with ExitStack() as stack:
        h = await mqtt_bridge(stack=stack)
        if scenario == "old_bridge":
            h.bridge.answer = False
            h.bridge.heartbeat = lambda: None
            await h.tick(91)
            await h.tick(25)
        else:
            await healthy_start(h)
            h.frozen_generations.add(1)
            if scenario == "broker_or_bridge_silent":
                h.bridge.answer = False
                h.bridge.heartbeat = lambda: None
            else:
                h.bridge.phase = hp.PHASE_DEGRADED
                h.bridge.error = hp.ERROR_BLE_DISCONNECTED
            for _ in range(8):
                await h.tick(100)
                h.coordinator.request_probe(ProbeKind.CONSUMER)
                await h.tick(25)
        assert h.coordinator.state is expected
        assert h.reloads == 0
        issues = ir.async_get(hass)
        has_unsupported = (
            issues.async_get_issue(DOMAIN, unsupported_issue_id(h.entry.entry_id))
            is not None
        )
        assert has_unsupported == (scenario == "old_bridge")
        await hass.config_entries.async_unload(h.entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_startup_grace_is_starting_not_a_fault(hass, mqtt_bridge):
    with ExitStack() as stack:
        h = await mqtt_bridge(stack=stack)
        await h.tick(10)
        assert h.coordinator.state is ConsumerState.STARTING
        assert h.runtime.health.probes.pending is None
        await hass.config_entries.async_unload(h.entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_one_entry_fault_never_touches_another_entry(hass, mqtt_bridge):
    with ExitStack() as stack:
        sick = await mqtt_bridge("sick", stack=stack)
        well = await mqtt_bridge("well", stack=stack)
        await healthy_start(sick)
        await healthy_start(well)
        well_subscriptions = well.subscribe_calls

        sick.frozen_generations.add(1)
        await sick.consumer_probe()
        await sick.tick(90)
        await sick.tick(21)
        await sick.wait_recovery()

        assert sick.reloads == 1
        assert well.reloads == 0
        assert well.subscribe_calls == well_subscriptions
        assert well.entry.state is ConfigEntryState.LOADED
        assert (await well.consumer_probe()).outcome.value == "ok"
        for h in (sick, well):
            await hass.config_entries.async_unload(h.entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_simultaneous_triggers_produce_one_reload(hass, mqtt_bridge):
    with ExitStack() as stack:
        h = await mqtt_bridge(stack=stack)
        await healthy_start(h)
        settings = h.coordinator.settings
        results = [h.manager.request_recovery(settings) for _ in range(3)]
        assert results == ["started", "in_progress", "in_progress"]
        await h.wait_recovery()
        assert h.reloads == 1
        await hass.config_entries.async_unload(h.entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_budget_survives_reloads_then_latches_with_one_repair_issue(
    hass, mqtt_bridge
):
    with ExitStack() as stack:
        h = await mqtt_bridge(stack=stack)
        await healthy_start(h)
        settings = h.coordinator.settings
        # Every generation stays frozen: reloads never fix it.
        h.frozen_generations.update(range(1, 20))

        assert h.manager.request_recovery(settings) == "started"
        await h.wait_recovery()
        assert h.manager.last_result == "verify_failed"
        manager_after_reload = h.hass.data[DOMAIN][DATA_RECOVERY][h.entry.entry_id]
        assert manager_after_reload is h.manager
        assert len(h.manager.recent_reloads()) == 1

        assert h.manager.request_recovery(settings) == "cooldown"
        h.clock.advance(600)
        assert h.manager.request_recovery(settings) == "started"
        await h.wait_recovery()
        assert h.reloads == 2
        assert h.manager.failed  # second unverified reload inside 30 min
        h.clock.advance(1)
        for _ in range(3):
            assert h.manager.request_recovery(settings) == "recovery_failed"
        await h.settle()
        assert h.reloads == 2

        registry = ir.async_get(hass)
        issue_ids = [
            issue_id
            for (domain, issue_id) in registry.issues
            if domain == DOMAIN and issue_id.startswith("recovery_failed")
        ]
        assert issue_ids == [recovery_issue_id(h.entry.entry_id)]
        assert h.coordinator.state is ConsumerState.RECOVERY_FAILED

        # The budget is stored, so a fresh manager (HA restart) still sees it.
        reloaded = RecoveryManager(hass, h.entry.entry_id, wall=h.clock.time)
        await reloaded.async_load()
        assert reloaded.failed
        assert len(reloaded.recent_reloads()) == 2

        # Operator acknowledgement through the repair flow.
        flow = await async_create_fix_flow(
            hass, recovery_issue_id(h.entry.entry_id), {"entry_id": h.entry.entry_id}
        )
        flow.hass = hass
        await flow.async_step_confirm({})
        assert not h.manager.failed
        assert registry.async_get_issue(DOMAIN, recovery_issue_id(h.entry.entry_id)) is None
        await hass.config_entries.async_unload(h.entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_fresh_unavailable_light_fails_verification(hass, mqtt_bridge):
    with ExitStack() as stack:
        h = await mqtt_bridge(stack=stack)
        await healthy_start(h)
        # After the reload the bridge no longer publishes one of the lights:
        # the consumer probe passes, but that target stays unavailable.
        del h.bridge.units["test-2"]
        assert h.manager.request_recovery(h.coordinator.settings) == "started"
        await h.wait_recovery()
        assert h.manager.last_result == "verify_failed"
        assert ProbeKind.CONSUMER in h.runtime.health.last_ok
        await hass.config_entries.async_unload(h.entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_recovery_disabled_by_option_never_reloads(hass, mqtt_bridge):
    with ExitStack() as stack:
        h = await mqtt_bridge(options={CONF_RECOVERY_ENABLED: False}, stack=stack)
        await healthy_start(h)
        h.frozen_generations.add(1)
        for _ in range(4):
            await h.consumer_probe()
            await h.tick(100)
        assert h.reloads == 0
        await hass.config_entries.async_unload(h.entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_handlers_do_not_accumulate_over_reload_rounds(hass, mqtt_bridge):
    with ExitStack() as stack:
        h = await mqtt_bridge(stack=stack)
        await healthy_start(h)
        for _ in range(3):
            assert await hass.config_entries.async_reload(h.entry.entry_id)
            await h.settle()
        obs = h.runtime.health
        before = obs.consumer.processed_messages
        async_fire_mqtt_message(hass, "casambi/test/events/test-1", unit_payload("test-1", "test one"))
        await h.settle()
        assert obs.consumer.processed_messages == before + 1
        assert obs.consumer.raw_messages == 1
        # Coordinators of unloaded generations no longer listen to the manager.
        assert len(h.manager.listeners) == 1
        await hass.config_entries.async_unload(h.entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_health_sensor_and_diagnostics_are_private(hass, mqtt_bridge):
    from custom_components.casambi_mqtt.diagnostics import (
        async_get_config_entry_diagnostics,
    )

    with ExitStack() as stack:
        h = await mqtt_bridge("secretnet", stack=stack)
        await healthy_start(h)
        from homeassistant.helpers import entity_registry as er

        entity_id = er.async_get(hass).async_get_entity_id(
            "sensor", DOMAIN, f"{h.entry.entry_id}_casambi_mqtt_health"
        )
        sensor = hass.states.get(entity_id)
        assert sensor is not None
        assert sensor.state == "healthy"
        text = json.dumps(await async_get_config_entry_diagnostics(hass, h.entry))
        text += json.dumps(dict(sensor.attributes))
        for private in ("secretnet", INSTANCE, "uuid-"):
            assert private not in text
        await hass.config_entries.async_unload(h.entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_disabled_light_updates_are_handled_without_errors(
    hass, mqtt_bridge, caplog
):
    """
    Lifecycle bug found while investigating #28: a user-disabled light is never
    added to Home Assistant (no entity_id), and every later update for it raised
    NoEntitySpecifiedError inside the consumer callback.
    """
    from homeassistant.helpers import entity_registry as er

    with ExitStack() as stack:
        entry = MockConfigEntry(
            domain=DOMAIN, data={CONF_NETWORK_NAME: "test"}, version=2
        )
        entry.add_to_hass(hass)
        er.async_get(hass).async_get_or_create(
            "light",
            DOMAIN,
            f"{entry.entry_id}_casambi_mqtt_light_test-1",
            config_entry=entry,
            disabled_by=er.RegistryEntryDisabler.USER,
        )
        h = await mqtt_bridge(stack=stack, entry=entry)
        for _ in range(3):
            async_fire_mqtt_message(
                hass, "casambi/test/events/test-1", unit_payload("test-1", "test one")
            )
            await h.settle()
        assert "NoEntitySpecifiedError" not in caplog.text
        assert "Task exception was never retrieved" not in caplog.text
        assert h.runtime.health.consumer.processed_messages == 3
        await hass.config_entries.async_unload(h.entry.entry_id)
