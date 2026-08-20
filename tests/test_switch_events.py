import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

import custom_components.casambi_mqtt as integration
from custom_components.casambi_mqtt import device_trigger
from custom_components.casambi_mqtt.const import CONF_NETWORK_NAME, DOMAIN
from homeassistant.components.device_automation import InvalidDeviceAutomationConfig
from homeassistant.helpers import device_registry as dr, entity_registry as er


async def _setup_entry(hass, network_name="test"):
    entry = MockConfigEntry(domain=DOMAIN, data={CONF_NETWORK_NAME: network_name})
    entry.add_to_hass(hass)
    subscriptions = []

    async def subscribe(_hass, topic, callback, qos):
        subscriptions.append((topic, callback, qos))
        return lambda: None

    with (
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
        patch.object(integration, "async_subscribe", side_effect=subscribe),
    ):
        assert await integration.async_setup_entry(hass, entry)

    return entry, subscriptions


@pytest.mark.asyncio
async def test_valid_switch_event_discovers_device_and_triggers(hass):
    entry, subscriptions = await _setup_entry(hass)

    assert [item[0] for item in subscriptions] == [
        "casambi/test/events/#",
        "casambi/test/scenes/#",
        "casambi/test/switch_events",
    ]
    assert subscriptions[2][2] == 1

    await subscriptions[2][1](
        SimpleNamespace(
            topic="casambi/test/switch_events",
            payload=json.dumps({"unit_id": 7, "button": 2, "event": "PRESS"}),
        )
    )

    registry = dr.async_get(hass)
    device = registry.async_get_device(identifiers={(DOMAIN, f"{entry.entry_id}:switch:7")})
    assert device is not None
    assert device.name == "Casambi switch 7"
    assert device.manufacturer == "Casambi"
    assert device.model == "Switch"
    assert er.async_entries_for_config_entry(er.async_get(hass), entry.entry_id) == []

    assert await device_trigger.async_get_triggers(hass, device.id) == [
        {
            "platform": "device",
            "domain": DOMAIN,
            "device_id": device.id,
            "type": event_type,
            "subtype": 2,
        }
        for event_type in (
            "press",
            "release",
            "hold",
            "release_after_hold",
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        "not-json",
        "[]",
        json.dumps({"unit_id": 1, "button": 2}),
        json.dumps({"unit_id": 1, "button": 2, "event": "PRESS", "raw": "secret"}),
        json.dumps({"unit_id": True, "button": 2, "event": "PRESS"}),
        json.dumps({"unit_id": 256, "button": 2, "event": "PRESS"}),
        json.dumps({"unit_id": 1, "button": -1, "event": "PRESS"}),
        json.dumps({"unit_id": 1, "button": 2, "event": "DOUBLE_PRESS"}),
    ],
)
async def test_invalid_switch_event_schema_fails_closed_without_logging(
    hass, caplog, payload
):
    entry, subscriptions = await _setup_entry(hass)

    await subscriptions[2][1](
        SimpleNamespace(topic="casambi/test/switch_events", payload=payload)
    )

    assert entry.runtime_data.switch_units == {}
    registry = dr.async_get(hass)
    assert dr.async_entries_for_config_entry(registry, entry.entry_id) == []
    assert payload not in caplog.text


@pytest.mark.asyncio
async def test_attached_trigger_fires_only_for_matching_event(hass):
    entry, subscriptions = await _setup_entry(hass)
    processor = subscriptions[2][1]
    await processor(
        SimpleNamespace(
            topic="casambi/test/switch_events",
            payload=json.dumps({"unit_id": 7, "button": 2, "event": "RELEASE"}),
        )
    )
    device_id = entry.runtime_data.switch_units[7].device_id
    action = AsyncMock()
    remove = await device_trigger.async_attach_trigger(
        hass,
        {
            "platform": "device",
            "domain": DOMAIN,
            "device_id": device_id,
            "type": "press",
            "subtype": 2,
        },
        action,
        {
            "trigger_data": {"id": "switch-press", "idx": "0", "alias": None},
            "domain": DOMAIN,
            "name": "automation",
            "home_assistant_start": False,
            "variables": {},
        },
    )

    await processor(
        SimpleNamespace(
            topic="casambi/test/switch_events",
            payload=json.dumps({"unit_id": 7, "button": 3, "event": "PRESS"}),
        )
    )
    await processor(
        SimpleNamespace(
            topic="casambi/test/switch_events",
            payload=json.dumps({"unit_id": 7, "button": 2, "event": "PRESS"}),
        )
    )
    await hass.async_block_till_done()

    action.assert_awaited_once_with(
        {
            "trigger": {
                "id": "switch-press",
                "idx": "0",
                "alias": None,
                "platform": "device",
                "domain": DOMAIN,
                "device_id": device_id,
                "type": "press",
                "subtype": 2,
                "description": "Casambi switch button 2 press",
            }
        }
    )

    remove()
    await processor(
        SimpleNamespace(
            topic="casambi/test/switch_events",
            payload=json.dumps({"unit_id": 7, "button": 2, "event": "HOLD"}),
        )
    )
    action.assert_awaited_once()


@pytest.mark.asyncio
async def test_trigger_attaches_after_reload_before_switch_is_rediscovered(hass):
    entry, subscriptions = await _setup_entry(hass)
    await subscriptions[2][1](
        SimpleNamespace(
            topic="casambi/test/switch_events",
            payload=json.dumps({"unit_id": 7, "button": 2, "event": "RELEASE"}),
        )
    )
    device_id = entry.runtime_data.switch_units[7].device_id

    reloaded_subscriptions = []

    async def subscribe(_hass, topic, callback, qos):
        reloaded_subscriptions.append((topic, callback, qos))
        return lambda: None

    with (
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
        patch.object(integration, "async_subscribe", side_effect=subscribe),
    ):
        assert await integration.async_setup_entry(hass, entry)

    action = AsyncMock()
    await device_trigger.async_attach_trigger(
        hass,
        {
            "platform": "device",
            "domain": DOMAIN,
            "device_id": device_id,
            "type": "press",
            "subtype": 2,
        },
        action,
        {},
    )
    await reloaded_subscriptions[2][1](
        SimpleNamespace(
            topic="casambi/test/switch_events",
            payload=json.dumps({"unit_id": 7, "button": 2, "event": "PRESS"}),
        )
    )
    await hass.async_block_till_done()

    action.assert_awaited_once()
    assert action.await_args.args[0]["trigger"] == {
        "platform": "device",
        "domain": DOMAIN,
        "device_id": device_id,
        "type": "press",
        "subtype": 2,
        "description": "Casambi switch button 2 press",
    }


@pytest.mark.asyncio
async def test_duplicate_is_collapsed_without_swallowing_press_release(hass):
    entry, subscriptions = await _setup_entry(hass)
    processor = subscriptions[2][1]
    with patch(
        "custom_components.casambi_mqtt.runtime_data.monotonic",
        side_effect=(0.0, 0.01, 0.1, 0.11, 0.4),
    ):
        await processor(
            SimpleNamespace(
                topic="casambi/test/switch_events",
                payload=json.dumps({"unit_id": 7, "button": 2, "event": "HOLD"}),
            )
        )
        device_id = entry.runtime_data.switch_units[7].device_id
        press_action = AsyncMock()
        release_action = AsyncMock()
        for event_type, action in (
            ("press", press_action),
            ("release", release_action),
        ):
            await device_trigger.async_attach_trigger(
                hass,
                {
                    "platform": "device",
                    "domain": DOMAIN,
                    "device_id": device_id,
                    "type": event_type,
                    "subtype": 2,
                },
                action,
                {},
            )
        for event_type in ("PRESS", "PRESS", "RELEASE", "PRESS"):
            await processor(
                SimpleNamespace(
                    topic="casambi/test/switch_events",
                    payload=json.dumps(
                        {"unit_id": 7, "button": 2, "event": event_type}
                    ),
                )
            )
        await hass.async_block_till_done()

    assert press_action.await_count == 2
    release_action.assert_awaited_once()


@pytest.mark.asyncio
async def test_entry_unload_removes_all_runtime_subscriptions(hass):
    entry = MockConfigEntry(domain=DOMAIN, data={CONF_NETWORK_NAME: "test"})
    entry.add_to_hass(hass)
    unsubscribers = []

    async def subscribe(_hass, _topic, _callback, _qos):
        unsubscribe = Mock()
        unsubscribers.append(unsubscribe)
        return unsubscribe

    with (
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
        patch.object(integration, "async_subscribe", side_effect=subscribe),
    ):
        assert await integration.async_setup_entry(hass, entry)

    assert len(unsubscribers) == 3
    await entry._async_process_on_unload(hass)
    for unsubscribe in unsubscribers:
        unsubscribe.assert_called_once_with()


@pytest.mark.asyncio
async def test_retained_switch_event_fails_closed(hass):
    entry, subscriptions = await _setup_entry(hass)

    await subscriptions[2][1](
        SimpleNamespace(
            topic="casambi/test/switch_events",
            payload=json.dumps({"unit_id": 7, "button": 2, "event": "PRESS"}),
            retain=True,
        )
    )

    assert entry.runtime_data.switch_units == {}
    assert dr.async_entries_for_config_entry(dr.async_get(hass), entry.entry_id) == []


@pytest.mark.asyncio
async def test_unknown_device_trigger_fails_closed(hass):
    await _setup_entry(hass)
    config = {
        "platform": "device",
        "domain": DOMAIN,
        "device_id": "unknown-device",
        "type": "press",
        "subtype": 2,
    }

    with pytest.raises(InvalidDeviceAutomationConfig):
        await device_trigger.async_validate_trigger_config(hass, config)

    with pytest.raises(InvalidDeviceAutomationConfig):
        await device_trigger.async_attach_trigger(hass, config, AsyncMock(), {})
