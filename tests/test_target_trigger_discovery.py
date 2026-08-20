"""Regression tests for Home Assistant's target-based trigger discovery.

Home Assistant 2026.8's automation editor resolves "Add trigger -> Device"
through the ``get_triggers_for_target`` websocket command, not through
``device_automation/trigger/list``. That command expands the target to entity
IDs and matches only modern trigger descriptions, so a device carrying nothing
but legacy device automations offers the editor no triggers at all.

These tests drive that exact command.
"""

import json
from contextlib import asynccontextmanager
from itertools import count
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

import custom_components.casambi_mqtt as integration
from custom_components.casambi_mqtt.const import CONF_NETWORK_NAME, DOMAIN
from custom_components.casambi_mqtt.switch_events import DEFAULT_SWITCH_BUTTONS
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.setup import async_setup_component

NETWORK = "koti"
SWITCH_TOPIC = f"casambi/{NETWORK}/switch_events"
UNIT_ID = 35


@pytest.fixture(autouse=True)
def distinct_switch_events():
    """Give every published event its own duplicate-suppression window."""
    ticks = count()
    with patch(
        "custom_components.casambi_mqtt.runtime_data.monotonic",
        side_effect=lambda: float(next(ticks)),
    ):
        yield


@asynccontextmanager
async def _capture_subscriptions(subscriptions):
    async def subscribe(_hass, topic, callback, _qos):
        subscriptions[topic] = callback
        return lambda: None

    with patch.object(integration, "async_subscribe", side_effect=subscribe):
        yield


async def _setup_entry(hass):
    entry = MockConfigEntry(domain=DOMAIN, data={CONF_NETWORK_NAME: NETWORK})
    entry.add_to_hass(hass)
    subscriptions = {}
    async with _capture_subscriptions(subscriptions):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
    return entry, subscriptions


async def _reload_entry(hass, entry, subscriptions):
    async with _capture_subscriptions(subscriptions):
        assert await hass.config_entries.async_reload(entry.entry_id)
        await hass.async_block_till_done()


async def _publish(hass, subscriptions, button, event):
    await subscriptions[SWITCH_TOPIC](
        SimpleNamespace(
            topic=SWITCH_TOPIC,
            payload=json.dumps(
                {"unit_id": UNIT_ID, "button": button, "event": event}
            ),
            retain=False,
        )
    )
    await hass.async_block_till_done()


def _switch_device(hass, entry):
    return dr.async_get(hass).async_get_device(
        identifiers={(DOMAIN, f"{entry.entry_id}:switch:{UNIT_ID}")}
    )


def _button_entity_id(hass, button):
    """Find one button's event entity by its registry unique ID."""
    registry = er.async_get(hass)
    return next(
        entry.entity_id
        for entry in registry.entities.values()
        if entry.domain == "event"
        and entry.unique_id.endswith(f"casambi_mqtt_switch_{UNIT_ID}_button_{button}")
    )


async def _triggers_for_target(hass, hass_ws_client, device_id):
    """Ask exactly what the automation editor asks."""
    client = await hass_ws_client(hass)
    await client.send_json_auto_id(
        {
            "type": "get_triggers_for_target",
            "target": {"device_id": device_id},
            "expand_group": True,
        }
    )
    message = await client.receive_json()
    assert message["success"], message.get("error")
    return set(message["result"])


@pytest.mark.asyncio
async def test_editor_offers_triggers_for_a_discovered_switch(hass, hass_ws_client):
    """The automation editor's target picker must not be a dead end."""
    assert await async_setup_component(hass, "homeassistant", {})
    entry, subscriptions = await _setup_entry(hass)
    await _publish(hass, subscriptions, 4, "PRESS")

    device = _switch_device(hass, entry)
    assert device is not None

    assert "event.received" in await _triggers_for_target(
        hass, hass_ws_client, device.id
    )


@pytest.mark.asyncio
async def test_every_standard_button_becomes_a_read_only_event_entity(hass):
    entry, subscriptions = await _setup_entry(hass)
    await _publish(hass, subscriptions, 4, "PRESS")

    event_ids = hass.states.async_entity_ids("event")
    assert len(event_ids) == len(DEFAULT_SWITCH_BUTTONS)
    for entity_id in event_ids:
        state = hass.states.get(entity_id)
        assert state.attributes["device_class"] == "button"
        assert state.attributes["event_types"] == [
            "press",
            "release",
            "hold",
            "release_after_hold",
        ]

    # The switch stays input only: every entity it owns is an event entity.
    device_id = _switch_device(hass, entry).id
    owned = er.async_entries_for_device(er.async_get(hass), device_id)
    assert owned
    assert {entry.domain for entry in owned} == {"event"}


@pytest.mark.asyncio
async def test_event_entity_reports_each_button_phase(hass):
    _, subscriptions = await _setup_entry(hass)
    await _publish(hass, subscriptions, 4, "PRESS")

    entity_id = _button_entity_id(hass, 4)
    assert hass.states.get(entity_id).attributes["event_type"] == "press"

    await _publish(hass, subscriptions, 4, "RELEASE_AFTER_HOLD")
    assert hass.states.get(entity_id).attributes["event_type"] == "release_after_hold"

    await _publish(hass, subscriptions, 4, "HOLD")
    assert hass.states.get(entity_id).attributes["event_type"] == "hold"

    # a different button must not report another button's phase
    other = _button_entity_id(hass, 1)
    assert hass.states.get(other).attributes["event_type"] is None


@pytest.mark.asyncio
async def test_observed_button_outside_the_defaults_gets_an_entity(hass):
    _, subscriptions = await _setup_entry(hass)
    await _publish(hass, subscriptions, 9, "PRESS")

    assert len(hass.states.async_entity_ids("event")) == len(DEFAULT_SWITCH_BUTTONS) + 1
    assert hass.states.get(_button_entity_id(hass, 9)).attributes["event_type"] == (
        "press"
    )


@pytest.mark.asyncio
async def test_known_switch_works_before_it_is_ever_pressed(hass, hass_ws_client):
    """A switch restored from the registry must not need an MQTT event first.

    This is the state a 0.2.4/0.2.5 user restarts into: the device exists in
    the registry, the persisted button store holds nothing for it.
    """
    assert await async_setup_component(hass, "homeassistant", {})
    entry, subscriptions = await _setup_entry(hass)
    await _publish(hass, subscriptions, 4, "PRESS")
    device_id = _switch_device(hass, entry).id

    entry.runtime_data.switch_store = None
    await _reload_entry(hass, entry, subscriptions)

    assert len(hass.states.async_entity_ids("event")) == len(DEFAULT_SWITCH_BUTTONS)
    assert "event.received" in await _triggers_for_target(
        hass, hass_ws_client, device_id
    )


@pytest.mark.asyncio
async def test_entities_survive_a_config_entry_reload(hass):
    entry, subscriptions = await _setup_entry(hass)
    await _publish(hass, subscriptions, 4, "PRESS")
    before = set(hass.states.async_entity_ids("event"))

    await _reload_entry(hass, entry, subscriptions)

    assert set(hass.states.async_entity_ids("event")) == before
    await _publish(hass, subscriptions, 4, "HOLD")
    assert hass.states.get(_button_entity_id(hass, 4)).attributes["event_type"] == (
        "hold"
    )
