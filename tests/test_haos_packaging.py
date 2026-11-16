"""Package contracts; real container smoke checks run in HAOS CI."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "haos_casambi"


def test_manifest_autostarts_with_internal_liveness_and_no_privileged_defaults():
    config = yaml.safe_load((APP / "config.yaml").read_text())
    assert config["arch"] == ["aarch64"]
    assert config["boot"] == "auto"
    assert config["version"] == "0.2.10-haos.2"
    assert config["watchdog"] == "http://[HOST]:[PORT:8099]/supervisor-health"
    assert config["timeout"] == 30
    assert not any(config.get("ports", {}).values())
    assert config["startup"] == "application"
    assert config["host_dbus"] is True
    assert config["host_network"] is False
    assert config["init"] is True
    assert config.get("apparmor", True) is True
    for key in (
        "privileged",
        "devices",
        "full_access",
        "docker_api",
        "hassio_api",
        "homeassistant_api",
        "host_pid",
        "map",
    ):
        assert not config.get(key)
    assert config["options"] == {
        "mode": "disabled",
        "env": "",
        "recovery_reset_generation": 0,
    }
    assert config["schema"]["mode"] == "list(disabled|preflight|bridge)"
    assert config["schema"]["env"] == "password"
    assert config["schema"]["recovery_reset_generation"] == "int(0,1000000)"
    assert "image" not in config  # No unconfigured registry credentials needed.


def test_repository_descriptor_and_root_level_app_are_discoverable():
    config = yaml.safe_load((ROOT / "repository.yaml").read_text())
    assert config["name"]
    assert config["url"] == "https://github.com/kaikkone4/casambi-mqtt"
    assert APP.parent == ROOT


def test_app_build_pins_and_verifies_existing_bridge_source():
    docker = (APP / "Dockerfile").read_text()
    assert "e5e3fd661740479489c7ed797f5707a031feaa5f" in docker
    assert "d63f26c655465eda33e57f02b297abcf65f91b814897a9504aff625057196724" in docker
    assert "sha256sum -c" in docker
    for path in (
        "const.py",
        "health_protocol.py",
        "entities",
        "switch_decoder.py",
        "server.py",
        "THIRD_PARTY_LICENSES",
        "requirements-server.txt",
    ):
        assert path in docker
    assert "COPY run.py" in docker
    assert "bridge_supervisor.py" in docker
    assert 'ENTRYPOINT ["python", "/app/bridge_supervisor.py"]' in docker
    assert "__init__.py" not in docker


def test_existing_generic_image_includes_shared_const_module():
    assert (
        "custom_components/casambi_mqtt/const.py" in (ROOT / "Dockerfile").read_text()
    )


def test_docs_explain_private_restore_and_real_bridge_control_boundary():
    text = (APP / "DOCS.md").read_text()
    for marker in (
        "preflight",
        "bridge",
        "/data/casambi-bt-store",
        ".cachever",
        "Telegram",
        "MQTT",
        "BlueZ",
        "manual",
    ):
        assert marker in text


def test_ci_exercises_real_isolated_container_without_publish_permissions():
    workflow = (ROOT / ".github/workflows/haos.yml").read_text()
    smoke = (ROOT / "tests/haos_container_smoke.py").read_text()
    executable = workflow + smoke
    assert "python tests/haos_container_smoke.py" in workflow
    for marker in (
        "docker build",
        "docker run",
        "--network none",
        "--entrypoint python",
        "import server",
        "supervisor-health",
        '"exec"',
        "latched",
        "test_haos_supervisor.py",
        "test_haos_entrypoint.py",
    ):
        assert marker in executable
    assert "packages: write" not in workflow
    assert "docker push" not in workflow
