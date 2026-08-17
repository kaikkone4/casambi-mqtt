import json
from unittest.mock import AsyncMock, patch

import pytest

from custom_components.casambi_mqtt.entities.entities import (
    Unit,
    UnitControl,
    UnitControlType,
    UnitState,
    UnitType,
)
from custom_components.casambi_mqtt.light import CasambiMqttLight
from homeassistant.components.light import ATTR_BRIGHTNESS


def addressless_unit(uuid: str = "unit-uuid", address: str = "") -> Unit:
    return Unit(
        address=address,
        device_id=7,
        is_on=False,
        name="Addressless dimmer",
        online=True,
        state=UnitState(dimmer=0),
        uuid=uuid,
        unit_type=UnitType(
            id=1,
            manufacturer="Casambi",
            mode="Dim",
            model="Test",
            state_length=1,
            controls=[
                UnitControl(
                    default=0,
                    length=8,
                    offset=0,
                    readonly=False,
                    type=UnitControlType(name="DIMMER", value=0),
                )
            ],
        ),
    )


@pytest.mark.asyncio
async def test_addressless_light_has_uuid_identity_and_preserves_control_address(hass):
    light = CasambiMqttLight(
        hass,
        "casambi/test/events/uuid/unit-uuid",
        "test",
        "entry-id",
        addressless_unit(),
    )

    assert light.unique_id == "entry-id_casambi_mqtt_light_uuid_unit-uuid"

    with patch(
        "custom_components.casambi_mqtt.light.mqtt.async_publish",
        new_callable=AsyncMock,
    ) as publish:
        await light.async_turn_on(**{ATTR_BRIGHTNESS: 42})
        await light.async_turn_off()

    first_payload = json.loads(publish.call_args_list[0].args[2])
    second_payload = json.loads(publish.call_args_list[1].args[2])
    assert publish.call_args_list[0].args[1] == "casambi/test/commands"
    assert first_payload == {
        "action": "SET_LEVEL",
        "address": "",
        "unit_uuid": "unit-uuid",
        "value": 42,
    }
    assert second_payload == {
        "action": "SET_LEVEL",
        "address": "",
        "unit_uuid": "unit-uuid",
        "value": 0,
    }


def test_light_treats_missing_dimmer_as_off(hass):
    unit = addressless_unit()
    unit.state = UnitState(dimmer=None)

    light = CasambiMqttLight(
        hass, "casambi/test/events/uuid/unit-uuid", "test", "entry-id", unit
    )

    assert light.is_on is False
    assert light.brightness == 0


@pytest.mark.asyncio
async def test_addressed_light_refreshes_command_uuid_after_unit_replacement(hass):
    light = CasambiMqttLight(
        hass,
        "casambi/test/events/real-address",
        "test",
        "entry-id",
        addressless_unit("old-uuid", "real-address"),
    )
    replacement = addressless_unit("new-uuid", "real-address")
    with patch.object(light, "async_write_ha_state"):
        light.update_entity(replacement)

    with patch(
        "custom_components.casambi_mqtt.light.mqtt.async_publish",
        new_callable=AsyncMock,
    ) as publish:
        await light.async_turn_off()

    assert json.loads(publish.call_args.args[2])["unit_uuid"] == "new-uuid"
