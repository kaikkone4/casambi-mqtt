"""Published health diagnostics must stay fresh even without state transitions."""

from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest
from homeassistant.helpers import entity_registry as er

from custom_components.casambi_mqtt.const import DOMAIN
from tests.test_recovery import healthy_start, mqtt_bridge  # noqa: F401


def health_state(hass, entry):
    entity_id = er.async_get(hass).async_get_entity_id(
        "sensor", DOMAIN, f"{entry.entry_id}_casambi_mqtt_health"
    )
    return hass.states.get(entity_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_healthy_probes_publish_distinct_success_timestamps(hass, mqtt_bridge):
    first = datetime(2026, 11, 1, tzinfo=timezone.utc)
    with ExitStack() as stack:
        now = stack.enter_context(
            patch(
                "custom_components.casambi_mqtt.recovery.dt_util.utcnow",
                return_value=first,
            )
        )
        h = await mqtt_bridge(stack=stack)
        await healthy_start(h)
        before = health_state(hass, h.entry)
        assert before.state == "healthy"
        assert before.attributes["last_consumer_ok"] == first.isoformat(
            timespec="seconds"
        )

        second = first + timedelta(seconds=60)
        now.return_value = second
        await h.consumer_probe()
        after = health_state(hass, h.entry)
        assert after.state == before.state
        assert after.attributes["reason"] == before.attributes["reason"]
        assert after.attributes["last_consumer_ok"] == second.isoformat(
            timespec="seconds"
        )
        await hass.config_entries.async_unload(h.entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_reload_count_ages_out_without_state_change(hass, mqtt_bridge):
    with ExitStack() as stack:
        h = await mqtt_bridge(stack=stack)
        # Seed a charged reload just inside the rolling window. Only the wall
        # clock advances: the existing healthy verdict and probe time stay put.
        wall = stack.enter_context(patch.object(h.manager, "_wall", return_value=1800))
        h.manager.reloads = [1]
        await healthy_start(h)
        before = health_state(hass, h.entry)
        assert before.state == "healthy"
        assert before.attributes["reloads_last_30_min"] == 1

        wall.return_value = 1801
        h.coordinator.async_update()
        await h.settle()
        after = health_state(hass, h.entry)
        assert after.state == before.state
        assert after.attributes["reason"] == before.attributes["reason"]
        assert (
            after.attributes["last_consumer_ok"]
            == before.attributes["last_consumer_ok"]
        )
        assert after.attributes["reloads_last_30_min"] == 0
        await hass.config_entries.async_unload(h.entry.entry_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("expected_lingering_timers", [True])
async def test_unchanged_diagnostics_do_not_notify_sensor(hass, mqtt_bridge):
    with ExitStack() as stack:
        h = await mqtt_bridge(stack=stack)
        await healthy_start(h)
        before = health_state(hass, h.entry)
        with patch.object(h.coordinator, "listeners", [listener := Mock()]):
            for _ in range(20):
                h.coordinator.async_update()
            listener.assert_not_called()
        assert health_state(hass, h.entry) is before
        await hass.config_entries.async_unload(h.entry.entry_id)
