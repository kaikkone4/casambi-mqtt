"""Launcher tests: no hardware, broker, HA stubs or recovered credentials."""

import builtins
import importlib.util
import json
import os
import shutil
import signal
import subprocess
import time
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = ROOT / "haos_casambi" / "run.py"


def load_launcher():
    assert LAUNCHER.is_file(), "HAOS launcher is not implemented"
    spec = importlib.util.spec_from_file_location("haos_launcher", LAUNCHER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("options", [None, {}, {"mode": "disabled"}])
def test_disabled_has_no_bridge_imports_or_side_effects(
    tmp_path, monkeypatch, capsys, options
):
    if options is not None:
        (tmp_path / "options.json").write_text(json.dumps(options))
    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        assert not name.startswith(
            (
                "dbus",
                "bleak",
                "CasambiBt",
                "aiomqtt",
                "dotenv",
                "server",
                "custom_components",
            )
        )
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    module = load_launcher()
    before = set(tmp_path.iterdir())
    cwd = Path.cwd()
    monkeypatch.setattr(os, "chdir", lambda *args: pytest.fail("disabled changed cwd"))
    monkeypatch.setattr(os, "execv", lambda *args: pytest.fail("disabled exec"))
    monkeypatch.setattr(
        os, "umask", lambda *args: pytest.fail("disabled changed umask")
    )
    assert module.main(data_dir=tmp_path) == 0
    assert Path.cwd() == cwd
    assert set(tmp_path.iterdir()) == before
    assert capsys.readouterr().out == "Casambi bridge disabled\n"


@pytest.mark.parametrize(
    "raw",
    [
        '{"mode":"secret-mode"}',
        '{"mode":null}',
        "[]",
        "{broken",
        '{"enabled":true}',
        '{"mode":"disabled","enabled":false}',
    ],
)
def test_invalid_options_fail_closed(tmp_path, capsys, raw):
    (tmp_path / "options.json").write_text(raw)
    assert load_launcher().main(data_dir=tmp_path) == 78
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "Invalid launcher configuration\n"


@pytest.mark.parametrize("result", ["ready", "busy", "error", "malformed"])
def test_preflight_only_reads_adapter_properties(tmp_path, monkeypatch, capsys, result):
    from dbus_fast import Message, Variant
    from dbus_fast.constants import BusType, MessageType
    import dbus_fast.aio
    from types import SimpleNamespace

    (tmp_path / "options.json").write_text(
        json.dumps({"mode": "preflight", "env": "BAD='PRIVATE"})
    )
    (tmp_path / ".env").write_text("THIS MUST NOT BE READ")
    calls = []

    class FakeBus:
        def __init__(self, *, bus_type):
            assert bus_type == BusType.SYSTEM
            calls.append("new")

        async def connect(self):
            calls.append("connect")
            return self

        async def call(self, message):
            assert isinstance(message, Message)
            assert message.destination == "org.bluez"
            assert message.path == "/org/bluez/hci0"
            assert message.interface == "org.freedesktop.DBus.Properties"
            assert message.member == "GetAll"
            assert message.signature == "s"
            assert message.body == ["org.bluez.Adapter1"]
            calls.append("GetAll")
            if result == "error":
                raise RuntimeError("SECRET DBUS DETAILS")
            properties = {
                "Powered": Variant("b", True),
                "Discovering": Variant("b", result == "busy"),
                "Address": Variant("s", "PRIVATE ADDRESS"),
            }
            if result == "malformed":
                properties.pop("Powered")
            return SimpleNamespace(
                message_type=MessageType.METHOD_RETURN, body=[properties]
            )

        def disconnect(self):
            calls.append("disconnect")

    monkeypatch.setattr(dbus_fast.aio, "MessageBus", FakeBus)
    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        assert not name.startswith(
            ("CasambiBt", "bleak", "aiomqtt", "dotenv", "server", "custom_components")
        )
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    assert load_launcher().main(data_dir=tmp_path) == (
        79 if result in ("error", "malformed") else 0
    )
    assert calls == ["new", "connect", "GetAll", "disconnect"]
    captured = capsys.readouterr()
    if result in ("error", "malformed"):
        assert captured.err == "Bluetooth preflight failed\n"
        assert captured.out == ""
    else:
        assert (
            captured.out
            == f"Bluetooth preflight: Powered=true Discovering={str(result == 'busy').lower()}\n"
        )
        assert captured.err == ""


CONFIG = {
    "MQTT_BROKER": "broker.example.invalid",
    "MQTT_PORT": "1883",
    "MQTT_USERNAME": "original-user",
    "MQTT_PASSWORD": "private-mqtt-password",
    "CASAMBI_NETWORK_ADDRESS": "AA:BB:CC:DD:EE:FF",
    "CASAMBI_NETWORK_PASSWORD": "private-network-password",
    "CASAMBI_NETWORK_NAME": "original namespace",
}


def write_bridge_config(data_dir, values):
    (data_dir / "options.json").write_text('{"mode":"bridge"}')

    def quoted(value):
        return value.replace("\\", "\\\\").replace("'", "\\'")

    (data_dir / ".env").write_text(
        "".join(f"{key}='{quoted(value)}'\n" for key, value in values.items())
    )


@pytest.mark.parametrize("missing", list(CONFIG) + [".env"])
def test_missing_bridge_configuration_fails_before_import_or_exec(
    tmp_path, monkeypatch, capsys, missing
):
    values = dict(CONFIG)
    values.pop(missing, None)
    write_bridge_config(tmp_path, values)
    if missing == ".env":
        (tmp_path / ".env").unlink()
    # Inherited values must not silently replace recovered identities.
    for key, value in CONFIG.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(
        os, "execv", lambda *args: pytest.fail("invalid configuration exec")
    )
    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        assert not name.startswith(
            ("CasambiBt", "bleak", "aiomqtt", "server", "custom_components")
        )
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    assert load_launcher().main(data_dir=tmp_path, server_path=ROOT / "server.py") == 78
    assert capsys.readouterr().err == "Invalid bridge configuration\n"


@pytest.fixture
def bridge_process(tmp_path):
    app, data = tmp_path / "app", tmp_path / "data"
    shared = app / "custom_components/casambi_mqtt"
    shared.mkdir(parents=True)
    data.mkdir()
    # Real shared module; namespace packages, no HA __init__ or stubs.
    shutil.copyfile(
        ROOT / "custom_components/casambi_mqtt/const.py", shared / "const.py"
    )
    values = CONFIG | {
        "MQTT_PASSWORD": "literal ${HOME} $abc # quote' double\" slash\\ newline\nää $(touch SHOULD_NOT_EXIST)",
        "CASAMBI_NETWORK_PASSWORD": "another # $secret \\ ' \" value",
        "LOG_LEVEL": "WARNING",
        "HEALTH_HEARTBEAT_SECONDS": "15",
        "HEALTH_RECORD_PATH": str(data / "health.json"),
        "UNTRUSTED_EXTRA": "must-not-reach-child",
        "PYTHONPATH": "/must/not/be/loaded",
    }
    write_bridge_config(data, values)
    store = data / "casambi-bt-store"
    store.mkdir()
    (store / ".cachever").write_bytes(b"2")
    (store / "recovered.pickle").write_bytes(
        b"opaque cache; never deserialize in launcher\x00\xff"
    )
    (store / "nested").mkdir()
    (store / "nested/item").write_bytes(b"persistent")
    server = app / "server.py"
    server.write_text(
        """import json, os, sys
from pathlib import Path
from dotenv import load_dotenv
from CasambiBt._cache import CACHE_PATH_DEFAULT
from custom_components.casambi_mqtt.const import is_valid_network_name
assert is_valid_network_name(os.environ["CASAMBI_NETWORK_NAME"])
assert load_dotenv() is False
result = {
    "pid": os.getpid(), "cwd": os.getcwd(), "argv": sys.argv,
    "cache_path": str(CACHE_PATH_DEFAULT),
    "environment": {key: os.environ.get(key) for key in """
        + repr(list(values) + ["PYTHON_DOTENV_DISABLED"])
        + """},
}
Path("result.json").write_text(json.dumps(result))
"""
    )
    code = (
        f"import runpy; m=runpy.run_path({str(LAUNCHER)!r}); "
        f"raise SystemExit(m['main'](data_dir=m['Path']({str(data)!r}), "
        f"server_path=m['Path']({str(server)!r})))"
    )
    environment = dict(os.environ)
    for key in values:
        environment.pop(key, None)
    environment["HEALTH_HEARTBEAT_SECONDS"] = "invalid inherited value"
    environment["LOG_LEVEL"] = "invalid inherited value"
    return data, server, values, [sys.executable, "-c", code], environment


@pytest.mark.parametrize("exit_code", [0, 75, 76, 78, 79])
def test_exec_preserves_environment_cache_and_exit_codes(bridge_process, exit_code):
    data, server, values, command, environment = bridge_process
    with server.open("a") as stream:
        stream.write(f"raise SystemExit({exit_code})\n")
    store = data / "casambi-bt-store"
    before = {
        p.relative_to(store): p.read_bytes() for p in store.rglob("*") if p.is_file()
    }
    for _ in range(2):
        process = subprocess.Popen(
            command,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == exit_code
        assert stdout == stderr == ""
        result = json.loads((data / "result.json").read_text())
        assert result["pid"] == process.pid
        assert result["cwd"] == str(data)
        assert result["cache_path"] == str(store)
        assert result["argv"] == [str(server)]
        expected = {
            key: (None if key in ("UNTRUSTED_EXTRA", "PYTHONPATH") else value)
            for key, value in values.items()
        }
        assert result["environment"] == expected | {"PYTHON_DOTENV_DISABLED": "1"}
        assert not (data / "SHOULD_NOT_EXIST").exists()
        assert (data / ".env").stat().st_mode & 0o777 == 0o600
        assert (data / "result.json").stat().st_mode & 0o777 == 0o600
        assert store.stat().st_mode & 0o777 == 0o700
        for path in store.rglob("*"):
            assert path.stat().st_mode & 0o777 == (0o700 if path.is_dir() else 0o600)
        assert {
            p.relative_to(store): p.read_bytes()
            for p in store.rglob("*")
            if p.is_file()
        } == before


@pytest.mark.parametrize("fallback", ["absent", "empty"])
def test_options_env_empty_or_absent_keeps_file_fallback(bridge_process, fallback):
    data, _, _, command, environment = bridge_process
    if fallback == "empty":
        (data / "options.json").write_text(json.dumps({"mode": "bridge", "env": ""}))
    result = subprocess.run(
        command, env=environment, capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0
    assert result.stdout == result.stderr == ""
    assert (data / "result.json").exists()


def test_options_env_preserves_special_characters_without_env_file(bridge_process):
    data, server, values, command, environment = bridge_process
    text = (data / ".env").read_text()
    (data / ".env").unlink()
    (data / "options.json").write_text(json.dumps({"mode": "bridge", "env": text}))
    result = subprocess.run(
        command, env=environment, capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0
    assert result.stdout == result.stderr == ""
    observed = json.loads((data / "result.json").read_text())
    assert observed["argv"] == [str(server)]
    for key, value in values.items():
        assert observed["environment"][key] == (
            None if key in ("PYTHONPATH", "UNTRUSTED_EXTRA") else value
        )
    assert not (data / ".env").exists()
    assert (data / "options.json").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "text", ["MQTT_PASSWORD='SECRET'", "BAD='unclosed SECRET", 42, None]
)
def test_invalid_provided_env_never_falls_back_to_valid_file(bridge_process, text):
    data, _, _, command, environment = bridge_process
    (data / "options.json").write_text(json.dumps({"mode": "bridge", "env": text}))
    result = subprocess.run(
        command, env=environment, capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 78
    assert result.stdout == ""
    assert result.stderr == "Invalid bridge configuration\n"
    assert not (data / "result.json").exists()


@pytest.mark.parametrize(
    "options",
    [
        {"mode": "disabled", "env": "BAD='PRIVATE"},
        {"env": "BAD='PRIVATE"},
    ],
)
def test_disabled_ignores_pasted_credentials(tmp_path, capsys, options):
    (tmp_path / "options.json").write_text(json.dumps(options))
    assert load_launcher().main(data_dir=tmp_path) == 0
    captured = capsys.readouterr()
    assert captured.out == "Casambi bridge disabled\n"
    assert captured.err == ""


def test_sigterm_reaches_execed_server(bridge_process):
    data, server, _, command, environment = bridge_process
    original = server.read_text()
    server.write_text(
        "import signal, time\nsignal.signal(signal.SIGTERM, lambda *_: exit(0))\n"
        + original
        + "while True: time.sleep(0.05)\n"
    )
    process = subprocess.Popen(
        command,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while (
            not (data / "result.json").exists()
            and process.poll() is None
            and time.monotonic() < deadline
        ):
            time.sleep(0.02)
        assert (data / "result.json").exists(), "execed server never became ready"
        assert json.loads((data / "result.json").read_text())["pid"] == process.pid
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == 0
        assert stdout == stderr == ""
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()


@pytest.mark.parametrize(
    "key,value",
    [
        ("MQTT_BROKER", ""),
        ("MQTT_PORT", "bad"),
        ("MQTT_PORT", "0"),
        ("MQTT_PORT", "65536"),
        ("CASAMBI_NETWORK_ADDRESS", "not-a-mac"),
        ("CASAMBI_NETWORK_NAME", ""),
        ("CASAMBI_NETWORK_NAME", "a/b"),
        ("CASAMBI_NETWORK_NAME", "a+"),
        ("CASAMBI_NETWORK_NAME", "#"),
        ("MQTT_PASSWORD", ""),
        ("CASAMBI_NETWORK_PASSWORD", ""),
        ("LOG_LEVEL", "SECRET"),
        ("HEALTH_HEARTBEAT_SECONDS", "4"),
        ("HEALTH_HEARTBEAT_SECONDS", "301"),
        ("HEALTH_HEARTBEAT_SECONDS", "bad"),
        ("MQTT_USERNAME", "nul\x00value"),
    ],
)
def test_invalid_bridge_fields_fail_closed(tmp_path, monkeypatch, capsys, key, value):
    write_bridge_config(tmp_path, CONFIG | {key: value})
    monkeypatch.setattr(
        os, "execv", lambda *args: pytest.fail("invalid configuration exec")
    )
    assert load_launcher().main(data_dir=tmp_path, server_path=ROOT / "server.py") == 78
    captured = capsys.readouterr()
    assert captured.err == "Invalid bridge configuration\n"
    assert captured.out == ""


def test_malformed_dotenv_has_only_fixed_error(tmp_path, capsys):
    write_bridge_config(tmp_path, CONFIG)
    with (tmp_path / ".env").open("a") as stream:
        stream.write("BAD='unclosed SECRET\n")
    assert load_launcher().main(data_dir=tmp_path, server_path=ROOT / "server.py") == 78
    assert capsys.readouterr().err == "Invalid bridge configuration\n"
