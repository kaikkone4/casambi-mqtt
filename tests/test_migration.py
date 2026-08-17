import pytest
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

import custom_components.casambi_mqtt as integration
from custom_components.casambi_mqtt.const import (
    CONF_NETWORK_NAME,
    DEFAULT_NETWORK_NAME,
    DOMAIN,
    configured_network_name,
)


def v1_entry(network_name: str = "test") -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        version=1,
        data={CONF_NETWORK_NAME: network_name},
    )


def test_empty_legacy_option_normalizes_to_default_network():
    assert (
        configured_network_name(
            {CONF_NETWORK_NAME: ""}, {CONF_NETWORK_NAME: ""}
        )
        == DEFAULT_NETWORK_NAME
    )


@pytest.mark.asyncio
async def test_migration_scopes_known_entities_and_removes_ambiguous_light(hass):
    entry = v1_entry()
    entry.add_to_hass(hass)
    registry = er.async_get(hass)
    light = registry.async_get_or_create(
        "light",
        DOMAIN,
        "casambi_mqtt_light_address-a",
        suggested_object_id="renamed_light",
        config_entry=entry,
    )
    scene = registry.async_get_or_create(
        "scene", DOMAIN, "casambi_mqtt_scene_1", config_entry=entry
    )
    button = registry.async_get_or_create(
        "button", DOMAIN, "casambi_mqtt_reload_entities", config_entry=entry
    )
    ambiguous = registry.async_get_or_create(
        "light",
        DOMAIN,
        integration.LEGACY_ADDRESSLESS_LIGHT_UNIQUE_ID,
        config_entry=entry,
    )

    assert await integration.async_migrate_entry(hass, entry)

    assert entry.version == integration.CONFIG_ENTRY_VERSION
    assert entry.unique_id == "test"
    assert registry.async_get(light.entity_id).unique_id == (
        f"{entry.entry_id}_casambi_mqtt_light_address-a"
    )
    assert registry.async_get(light.entity_id).entity_id == light.entity_id
    assert registry.async_get(scene.entity_id).unique_id == (
        f"{entry.entry_id}_casambi_mqtt_scene_1"
    )
    assert registry.async_get(button.entity_id).unique_id == (
        f"{entry.entry_id}_casambi_mqtt_reload_entities"
    )
    assert registry.async_get(ambiguous.entity_id) is None


@pytest.mark.asyncio
async def test_migration_refuses_collision_without_mutating_any_registry_entry(hass):
    entry = v1_entry()
    entry.add_to_hass(hass)
    registry = er.async_get(hass)
    legacy = registry.async_get_or_create(
        "light", DOMAIN, "casambi_mqtt_light_address-a", config_entry=entry
    )
    conflict = registry.async_get_or_create(
        "light",
        DOMAIN,
        f"{entry.entry_id}_casambi_mqtt_light_address-a",
    )

    assert not await integration.async_migrate_entry(hass, entry)

    assert entry.version == 1
    assert (
        registry.async_get(legacy.entity_id).unique_id == "casambi_mqtt_light_address-a"
    )
    assert registry.async_get(conflict.entity_id).unique_id == (
        f"{entry.entry_id}_casambi_mqtt_light_address-a"
    )


@pytest.mark.asyncio
async def test_migration_ignores_another_config_entry_and_is_retry_safe(hass):
    entry = v1_entry("first")
    other = v1_entry("second")
    entry.add_to_hass(hass)
    other.add_to_hass(hass)
    registry = er.async_get(hass)
    ours = registry.async_get_or_create(
        "light",
        DOMAIN,
        f"{entry.entry_id}_casambi_mqtt_light_address-a",
        config_entry=entry,
    )
    theirs = registry.async_get_or_create(
        "light", DOMAIN, "casambi_mqtt_light_address-a", config_entry=other
    )

    assert await integration.async_migrate_entry(hass, entry)

    assert registry.async_get(ours.entity_id).unique_id == (
        f"{entry.entry_id}_casambi_mqtt_light_address-a"
    )
    assert (
        registry.async_get(theirs.entity_id).unique_id == "casambi_mqtt_light_address-a"
    )


@pytest.mark.asyncio
async def test_migration_refuses_duplicate_effective_network_name(hass):
    entry = v1_entry("shared")
    other = v1_entry("shared")
    entry.add_to_hass(hass)
    other.add_to_hass(hass)

    assert not await integration.async_migrate_entry(hass, entry)
    assert entry.version == 1
    assert entry.unique_id is None
