"""Regression tests for Home Assistant's real device-automation discovery path.

These tests deliberately avoid calling ``device_trigger.async_get_triggers``
directly. They drive the same code Home Assistant's automation editor drives:
``device_automation/trigger/list`` over the websocket API, and a real
``automation`` config entry attaching the trigger.
"""

import json
from contextlib import asynccontextmanager
from itertools import count
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_mock_service,
)

import custom_components.casambi_mqtt as integration
from custom_components.casambi_mqtt.const import CONF_NETWORK_NAME, DOMAIN
from custom_components.casambi_mqtt.switch_events import (
    DEFAULT_SWITCH_BUTTONS,
    SUPPORTED_SWITCH_EVENTS,
)
from homeassistant.setup import async_setup_component
from homeassistant.helpers import device_registry as dr

NETWORK = "koti"
SWITCH_TOPIC = f"casambi/{NETWORK}/switch_events"
UNIT_ID = 35
EVENT_TYPES = tuple(event.lower() for event in SUPPORTED_SWITCH_EVENTS)


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


async def _publish(hass, subscriptions, button, event, *, retain=False):
    await subscriptions[SWITCH_TOPIC](
        SimpleNamespace(
            topic=SWITCH_TOPIC,
            payload=json.dumps(
                {"unit_id": UNIT_ID, "button": button, "event": event}
            ),
            retain=retain,
        )
    )
    await hass.async_block_till_done()


def _switch_device(hass, entry):
    return dr.async_get(hass).async_get_device(
        identifiers={(DOMAIN, f"{entry.entry_id}:switch:{UNIT_ID}")}
    )


async def _list_triggers(hass, hass_ws_client, device_id):
    """List triggers exactly like the automation editor does."""
    client = await hass_ws_client(hass)
    await client.send_json_auto_id(
        {"type": "device_automation/trigger/list", "device_id": device_id}
    )
    message = await client.receive_json()
    assert message["success"], message.get("error")
    return message["result"]


def _expected(device_id, buttons):
    return [
        {
            "platform": "device",
            "domain": DOMAIN,
            "device_id": device_id,
            "type": event_type,
            "subtype": button,
            "metadata": {},
        }
        for button in sorted(buttons)
        for event_type in EVENT_TYPES
    ]


@pytest.mark.asyncio
async def test_known_switch_lists_triggers_without_any_observed_button(
    hass, hass_ws_client
):
    """A switch restored from the registry must not be a dead end in the UI.

    This is the v0.2.4 -> v0.2.5 upgrade state: the device exists, but the
    persisted button store has no record for it yet.
    """
    assert await async_setup_component(hass, "device_automation", {})
    entry, subscriptions = await _setup_entry(hass)
    await _publish(hass, subscriptions, 4, "PRESS")
    device = _switch_device(hass, entry)
    assert device is not None

    # Wipe every trace of the observation, keeping only the registered device.
    await _reload_entry(hass, entry, subscriptions)
    entry.runtime_data.switch_units.clear()

    assert await _list_triggers(hass, hass_ws_client, device.id) == _expected(
        device.id, DEFAULT_SWITCH_BUTTONS
    )


@pytest.mark.asyncio
async def test_observed_button_outside_the_defaults_is_added(hass, hass_ws_client):
    assert await async_setup_component(hass, "device_automation", {})
    entry, subscriptions = await _setup_entry(hass)
    await _publish(hass, subscriptions, 7, "PRESS")
    device = _switch_device(hass, entry)

    assert await _list_triggers(hass, hass_ws_client, device.id) == _expected(
        device.id, {*DEFAULT_SWITCH_BUTTONS, 7}
    )


@pytest.mark.asyncio
async def test_removed_device_is_recreated_and_keeps_listing_triggers(
    hass, hass_ws_client
):
    """A stale runtime device id must never permanently kill enumeration."""
    assert await async_setup_component(hass, "device_automation", {})
    entry, subscriptions = await _setup_entry(hass)
    await _publish(hass, subscriptions, 4, "PRESS")
    registry = dr.async_get(hass)
    registry.async_remove_device(_switch_device(hass, entry).id)
    await hass.async_block_till_done()
    assert _switch_device(hass, entry) is None

    await _publish(hass, subscriptions, 4, "RELEASE")

    device = _switch_device(hass, entry)
    assert device is not None
    assert entry.runtime_data.switch_units[UNIT_ID].device_id == device.id
    assert await _list_triggers(hass, hass_ws_client, device.id) == _expected(
        device.id, DEFAULT_SWITCH_BUTTONS
    )


@pytest.mark.asyncio
async def test_device_trigger_runs_a_real_automation(hass):
    entry, subscriptions = await _setup_entry(hass)
    await _publish(hass, subscriptions, 4, "PRESS")
    device = _switch_device(hass, entry)
    calls = async_mock_service(hass, "test", "automation")

    assert await async_setup_component(
        hass,
        "automation",
        {
            "automation": {
                "trigger": {
                    "platform": "device",
                    "domain": DOMAIN,
                    "device_id": device.id,
                    "type": "press",
                    "subtype": 4,
                },
                "action": {
                    "service": "test.automation",
                    "data": {"description": "{{ trigger.description }}"},
                },
            }
        },
    )
    await hass.async_block_till_done()

    await _publish(hass, subscriptions, 4, "RELEASE")
    assert len(calls) == 0

    await _publish(hass, subscriptions, 4, "PRESS")
    assert len(calls) == 1
    assert calls[0].data["description"] == "Casambi switch button 4 press"


@pytest.mark.asyncio
async def test_attached_trigger_survives_a_config_entry_reload(hass):
    """Reloading the entry must not silently orphan running automations."""
    entry, subscriptions = await _setup_entry(hass)
    await _publish(hass, subscriptions, 4, "PRESS")
    device = _switch_device(hass, entry)
    calls = async_mock_service(hass, "test", "automation")

    assert await async_setup_component(
        hass,
        "automation",
        {
            "automation": {
                "trigger": {
                    "platform": "device",
                    "domain": DOMAIN,
                    "device_id": device.id,
                    "type": "press",
                    "subtype": 4,
                },
                "action": {"service": "test.automation"},
            }
        },
    )
    await hass.async_block_till_done()

    await _reload_entry(hass, entry, subscriptions)

    await _publish(hass, subscriptions, 4, "PRESS")
    assert len(calls) == 1
