"""Notification template tests use synthetic protocol data only."""

from pathlib import Path

from jinja2.sandbox import SandboxedEnvironment
import yaml
import pytest

PATH = Path(__file__).resolve().parents[1] / "haos_casambi/recovery-notification.yaml"
ENV = SandboxedEnvironment()


@pytest.mark.parametrize(
    "patch,expected",
    [
        ({"attempts": 0}, True),
        ({"attempts": 4}, False),
        ({"attempts": -1}, False),
        ({"attempts": True}, False),
        ({"attempts": "3"}, False),
        ({"protocol_version": True}, False),
        ({"protocol_version": "1"}, False),
        ({"incident_id": None}, False),
        ({"incident_id": ""}, False),
        ({"incident_id": "x" * 65}, False),
        ({"state": "ready"}, False),
        ({"state": "armed", "attempts": 0}, True),
        ({"state": "armed", "attempts": 3}, False),
    ],
)
def test_only_fixed_bounded_protocol_is_accepted(patch, expected):
    config = yaml.safe_load(PATH.read_text())
    payload = {
        "protocol_version": 1,
        "incident_id": "synthetic",
        "state": "failed",
        "attempts": 3,
    } | patch
    rendered = (
        ENV.from_string(config["conditions"][0]["value_template"])
        .render(trigger={"payload_json": payload})
        .strip()
    )
    assert rendered == str(expected)


@pytest.mark.parametrize("payload", [None, [], "invalid", {}])
def test_non_objects_and_missing_fields_fail_closed(payload):
    config = yaml.safe_load(PATH.read_text())
    assert (
        ENV.from_string(config["conditions"][0]["value_template"])
        .render(trigger={"payload_json": payload})
        .strip()
        == "False"
    )


def test_manual_rearm_dismisses_only_fixed_notification():
    config = yaml.safe_load(PATH.read_text())
    dismiss = config["actions"][0]["choose"][1]["sequence"][0]
    assert dismiss == {
        "action": "persistent_notification.dismiss",
        "data": {"notification_id": "casambi_bridge_recovery_NETWORK_NAME"},
    }


def test_failed_three_is_a_fixed_persistent_notification():
    config = yaml.safe_load(PATH.read_text())
    payload = {
        "protocol_version": 1,
        "incident_id": "synthetic",
        "state": "failed",
        "attempts": 3,
    }
    assert (
        ENV.from_string(config["conditions"][0]["value_template"])
        .render(trigger={"payload_json": payload})
        .strip()
        == "True"
    )
    create = config["actions"][0]["choose"][0]["sequence"][0]
    assert create["action"] == "persistent_notification.create"
    assert create["data"]["notification_id"] == "casambi_bridge_recovery_NETWORK_NAME"
