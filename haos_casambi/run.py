"""Opt-in HAOS entrypoint. Never import the bridge before validation."""
# Standalone CLI; delayed third-party imports keep disabled mode inert.
# Fixed CLI messages intentionally never interpolate errors or credentials.
# ruff: noqa: INP001, PLC0415, T201

import asyncio
import importlib.util
import json
import os
import re
import sys
from io import StringIO
from pathlib import Path
from typing import Any

MIN_PORT = 1
MAX_PORT = 65535
MIN_HEARTBEAT = 5
MAX_HEARTBEAT = 300
MAX_RESET_GENERATION = 1000000
REQUIRED_ENV = (
    "MQTT_BROKER",
    "MQTT_PORT",
    "MQTT_USERNAME",
    "MQTT_PASSWORD",
    "CASAMBI_NETWORK_ADDRESS",
    "CASAMBI_NETWORK_PASSWORD",
    "CASAMBI_NETWORK_NAME",
)
OPTIONAL_ENV = ("LOG_LEVEL", "HEALTH_HEARTBEAT_SECONDS", "HEALTH_RECORD_PATH")


async def preflight() -> None:
    """Read hci0 properties; no discovery, setters, BLE or MQTT imports."""
    from dbus_fast import Message
    from dbus_fast.aio import MessageBus
    from dbus_fast.constants import BusType, MessageType

    bus = MessageBus(bus_type=BusType.SYSTEM)
    try:
        await asyncio.wait_for(bus.connect(), timeout=10)
        reply = await asyncio.wait_for(
            bus.call(
                Message(
                    destination="org.bluez",
                    path="/org/bluez/hci0",
                    interface="org.freedesktop.DBus.Properties",
                    member="GetAll",
                    signature="s",
                    body=["org.bluez.Adapter1"],
                )
            ),
            timeout=10,
        )
        if reply.message_type != MessageType.METHOD_RETURN:
            raise ValueError
        properties = reply.body[0]
        powered = properties["Powered"].value
        discovering = properties["Discovering"].value
        if type(powered) is not bool or type(discovering) is not bool:
            raise ValueError
        print(
            f"Bluetooth preflight: Powered={str(powered).lower()} "
            f"Discovering={str(discovering).lower()}"
        )
    finally:
        bus.disconnect()


def bridge_environment(
    data_dir: Path, server_path: Path, text: str = ""
) -> dict[str, str]:
    """Parse trusted recovered settings, without interpolation or shell eval."""
    from dotenv import dotenv_values
    from dotenv.parser import parse_stream

    if not isinstance(text, str):
        raise TypeError
    if not text:
        text = (data_dir / ".env").read_text(encoding="utf-8")
    # dotenv_values otherwise only warns and skips malformed lines.
    if any(binding.error for binding in parse_stream(StringIO(text))):
        raise ValueError
    parsed = dotenv_values(stream=StringIO(text), interpolate=False)
    selected = {
        key: parsed[key] for key in REQUIRED_ENV + OPTIONAL_ENV if key in parsed
    }
    if any(value is None or "\x00" in value for value in selected.values()):
        raise ValueError
    values = {key: value for key, value in selected.items() if value is not None}
    if any(not values.get(key) for key in REQUIRED_ENV):
        raise ValueError
    if not MIN_PORT <= int(values["MQTT_PORT"]) <= MAX_PORT:
        raise ValueError
    if not re.fullmatch(
        r"(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}", values["CASAMBI_NETWORK_ADDRESS"]
    ):
        raise ValueError
    # Load only the shared module, not the checkout's HA integration __init__.
    spec = importlib.util.spec_from_file_location(
        "casambi_shared_const",
        server_path.parent / "custom_components/casambi_mqtt/const.py",
    )
    if spec is None or spec.loader is None:
        raise ValueError
    const = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(const)
    if not const.is_valid_network_name(values["CASAMBI_NETWORK_NAME"]):
        raise ValueError
    if values.get("LOG_LEVEL", "INFO") not in (
        "CRITICAL",
        "FATAL",
        "ERROR",
        "WARNING",
        "WARN",
        "INFO",
        "DEBUG",
        "NOTSET",
    ):
        raise ValueError
    if (
        not MIN_HEARTBEAT
        <= int(values.get("HEALTH_HEARTBEAT_SECONDS", "30"))
        <= MAX_HEARTBEAT
    ):
        raise ValueError
    return values


def _read_options(options_path: Path) -> dict[str, Any]:
    options = json.loads(options_path.read_text()) if options_path.exists() else {}
    if not isinstance(options, dict) or set(options) - {
        "mode",
        "env",
        "recovery_reset_generation",
    }:
        raise ValueError
    generation = options.get("recovery_reset_generation", 0)
    if type(generation) is not int or not 0 <= generation <= MAX_RESET_GENERATION:
        raise ValueError
    if options.get("mode", "disabled") not in ("disabled", "preflight", "bridge"):
        raise ValueError
    return options


def main(
    *, data_dir: Path = Path("/data"), server_path: Path = Path("/app/server.py")
) -> int:
    """Dispatch the explicitly selected mode; exec preserves bridge signals."""
    options_path = data_dir / "options.json"
    try:
        options = _read_options(options_path)
    except (OSError, ValueError):
        print("Invalid launcher configuration", file=sys.stderr)
        return 78
    mode = options.get("mode", "disabled")
    if mode == "disabled":
        print("Casambi bridge disabled")
        return 0
    if mode == "preflight":
        try:
            asyncio.run(preflight())
        except Exception:  # noqa: BLE001 — D-Bus errors may carry private identifiers.
            print("Bluetooth preflight failed", file=sys.stderr)
            return 79
        return 0
    try:
        values = bridge_environment(data_dir, server_path, options.get("env", ""))
    except Exception:  # noqa: BLE001 — never expose parser errors or credential values.
        print("Invalid bridge configuration", file=sys.stderr)
        return 78
    try:
        server_path = server_path.resolve()
        os.umask(0o077)
        options_path.chmod(0o600)
        if (data_dir / ".env").exists():
            (data_dir / ".env").chmod(0o600)
        store = data_dir / "casambi-bt-store"
        if store.exists():
            store.chmod(0o700)
            for directory, dirs, files in os.walk(store):
                for name in dirs:
                    (Path(directory) / name).chmod(0o700)
                for name in files:
                    (Path(directory) / name).chmod(0o600)
        # Prevent server load_dotenv() bypassing validation or interpolation policy.
        for key in REQUIRED_ENV + OPTIONAL_ENV:
            os.environ.pop(key, None)
        os.environ.update(values)
        os.environ["PYTHON_DOTENV_DISABLED"] = "1"
        os.chdir(data_dir)
        # Fixed interpreter/script path, no shell and no credentials in argv.
        os.execv(sys.executable, [sys.executable, str(server_path)])  # noqa: S606
    except OSError:
        print("Bridge launch failed", file=sys.stderr)
        return 79


if __name__ == "__main__":
    raise SystemExit(main())
