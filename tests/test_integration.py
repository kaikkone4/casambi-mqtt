import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

import custom_components.casambi_mqtt as integration
from custom_components.casambi_mqtt.const import CONF_NETWORK_NAME, DOMAIN
from homeassistant.helpers import entity_registry as er


def unit_payload(
    uuid: str, address: str = "", control_name: str = "DIMMER"
) -> str:
    return json.dumps(
        {
            "address": address,
            "device_id": 7,
            "is_on": False,
            "name": "Addressless dimmer",
            "online": True,
            "state": {"dimmer": 0},
            "uuid": uuid,
            "unit_type": {
                "id": 1,
                "manufacturer": "Casambi",
                "mode": "Dim" if control_name == "DIMMER" else "Switch",
                "model": "Test",
                "state_length": 1,
                "controls": [
                    {
                        "default": 0,
                        "length": 8,
                        "offset": 0,
                        "readonly": False,
                        "type": {"name": control_name, "value": 0},
                    }
                ],
            },
        }
    )


@pytest.mark.asyncio
async def test_addressless_topic_requires_matching_uuid_and_ignores_tombstone(hass):
    entry = MockConfigEntry(domain=DOMAIN, data={CONF_NETWORK_NAME: "test"})
    entry.add_to_hass(hass)
    subscriptions = []

    async def subscribe(_hass, topic, callback, _qos):
        subscriptions.append((topic, callback))
        return lambda: None

    with (
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
        patch.object(integration, "async_subscribe", side_effect=subscribe),
    ):
        assert await integration.async_setup_entry(hass, entry)

    event_processor = subscriptions[0][1]
    add_entities = Mock()
    entry.runtime_data.light_add_entities = add_entities

    await event_processor(
        SimpleNamespace(topic="casambi/test/events/uuid/unit-uuid", payload="")
    )
    await event_processor(
        SimpleNamespace(
            topic="casambi/test/events/", payload=unit_payload("unit-uuid")
        )
    )
    await event_processor(
        SimpleNamespace(
            topic="casambi/test/events/uuid/wrong", payload=unit_payload("unit-uuid")
        )
    )
    assert add_entities.call_count == 0

    await event_processor(
        SimpleNamespace(
            topic="casambi/test/events/uuid/unit-uuid", payload=unit_payload("unit-uuid")
        )
    )
    assert add_entities.call_count == 1
    assert entry.runtime_data.lights[
        "casambi/test/events/uuid/unit-uuid"
    ].unique_id == f"{entry.entry_id}_casambi_mqtt_light_uuid_unit-uuid"


@pytest.mark.asyncio
async def test_non_light_unit_is_ignored_without_invalid_payload_warning(hass, caplog):
    entry = MockConfigEntry(domain=DOMAIN, data={CONF_NETWORK_NAME: "test"})
    entry.add_to_hass(hass)
    subscriptions = []

    async def subscribe(_hass, topic, callback, _qos):
        subscriptions.append((topic, callback))
        return lambda: None

    with (
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
        patch.object(integration, "async_subscribe", side_effect=subscribe),
    ):
        assert await integration.async_setup_entry(hass, entry)

    entry.runtime_data.light_add_entities = Mock()
    await subscriptions[0][1](
        SimpleNamespace(
            topic="casambi/test/events/switch-address",
            payload=unit_payload("switch-uuid", "switch-address", "SWITCH"),
        )
    )

    assert entry.runtime_data.light_add_entities.call_count == 0
    assert "Invalid Casambi unit payload" not in caplog.text


@pytest.mark.asyncio
async def test_malformed_unit_type_warns_without_crashing(hass, caplog):
    entry = MockConfigEntry(domain=DOMAIN, data={CONF_NETWORK_NAME: "test"})
    entry.add_to_hass(hass)
    subscriptions = []

    async def subscribe(_hass, topic, callback, _qos):
        subscriptions.append((topic, callback))
        return lambda: None

    with (
        patch.object(hass.config_entries, "async_forward_entry_setups", AsyncMock()),
        patch.object(integration, "async_subscribe", side_effect=subscribe),
    ):
        assert await integration.async_setup_entry(hass, entry)

    malformed = json.loads(unit_payload("unit-uuid", "address"))
    malformed["unit_type"] = None
    with pytest.warns(RuntimeWarning, match="unit_type"):
        await subscriptions[0][1](
            SimpleNamespace(
                topic="casambi/test/events/address", payload=json.dumps(malformed)
            )
        )

    assert "Invalid Casambi unit payload" in caplog.text


def test_legacy_registry_cleanup_is_scoped_to_current_entry(hass):
    first = MockConfigEntry(domain=DOMAIN, data={CONF_NETWORK_NAME: "first"})
    second = MockConfigEntry(domain=DOMAIN, data={CONF_NETWORK_NAME: "second"})
    first.add_to_hass(hass)
    second.add_to_hass(hass)
    registry = er.async_get(hass)
    first_legacy = registry.async_get_or_create(
        "light",
        DOMAIN,
        integration.LEGACY_ADDRESSLESS_LIGHT_UNIQUE_ID,
        config_entry=first,
    )
    second_legacy = registry.async_get_or_create(
        "light",
        DOMAIN,
        "casambi_mqtt_light_other_legacy",
        config_entry=second,
    )

    integration._remove_legacy_addressless_light(hass, second)
    assert registry.async_get(first_legacy.entity_id) is not None

    integration._remove_legacy_addressless_light(hass, first)

    assert registry.async_get(first_legacy.entity_id) is None
    assert registry.async_get(second_legacy.entity_id) is not None
