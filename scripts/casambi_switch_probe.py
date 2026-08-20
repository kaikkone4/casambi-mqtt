#!/usr/bin/env python3
# Copyright (c) 2026 kaikkone4
"""
Listen briefly for Casambi switch events without controlling the network.

Runtime contract:
- Requires casambi-bt-revamped==0.4.2.dev6 (import name: CasambiBt).
- Reads the network password only from CASAMBI_PASSWORD.
- Uses CASAMBI_ADDR when set; otherwise requires discovery to find exactly one network.
- Writes only ``SWITCH_EVENT`` plus whitelisted JSON fields to stdout.

Connecting/authenticating necessarily performs the protocol handshake, but this probe
never calls control, pairing, reset, configuration, or raw-send APIs.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

REQUIRED_DISTRIBUTION = "casambi-bt-revamped"
REQUIRED_VERSION = "0.4.2.dev6"
LISTEN_SECONDS = 90
ADDRESS_HEX_LENGTH = 12
MAX_EVENT_NUMBER = 255
ALLOWED_EVENTS = frozenset(
    {
        "button_press",
        "button_release",
        "button_hold",
        "button_release_after_hold",
        "input_event",
    }
)


class ConfigurationError(RuntimeError):
    """The environment does not provide a safe, usable configuration."""


class ProbeError(RuntimeError):
    """The probe cannot run without exposing sensitive diagnostics."""


def read_environment() -> tuple[str, str | None]:
    """Read the deliberately small environment contract."""
    password = os.environ.get("CASAMBI_PASSWORD")
    if not password:
        raise ConfigurationError
    address = os.environ.get("CASAMBI_ADDR") or None
    return password, address


def sanitize_switch_event(event_data: Mapping[str, Any]) -> dict[str, int | str]:
    """Return only documented semantic fields with bounded primitive values."""
    record: dict[str, int | str] = {}

    event = event_data.get("event")
    if isinstance(event, str) and event in ALLOWED_EVENTS:
        record["event"] = event

    for field in ("button", "unit_id"):
        value = event_data.get(field)
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and 0 <= value <= MAX_EVENT_NUMBER
        ):
            record[field] = value

    return record


def emit_switch_event(event_data: Mapping[str, Any]) -> None:
    """Emit one machine-readable record, omitting all diagnostic/raw fields."""
    record = sanitize_switch_event(event_data)
    if "event" not in record:
        return
    payload = json.dumps(record, sort_keys=True, separators=(",", ":"))
    sys.stdout.write(f"SWITCH_EVENT {payload}\n")
    sys.stdout.flush()


async def listen(
    casa: Any,
    target: Any,
    password: str,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Connect, listen for exactly the fixed window, and always clean up."""
    handler = emit_switch_event
    casa.registerSwitchEventHandler(handler)
    try:
        await casa.connect(target, password)
        await sleep(LISTEN_SECONDS)
    finally:
        try:
            casa.unregisterSwitchEventHandler(handler)
        finally:
            await casa.disconnect()


def normalize_address(address: str) -> str:
    """Return a colon-delimited uppercase Casambi network address."""
    compact = address.translate(str.maketrans("", "", ":-."))
    if len(compact) != ADDRESS_HEX_LENGTH or not all(
        character in "0123456789abcdefABCDEF" for character in compact
    ):
        raise ProbeError
    return ":".join(
        compact[index : index + 2] for index in range(0, ADDRESS_HEX_LENGTH, 2)
    ).upper()


async def resolve_target(
    address: str | None,
    discover: Callable[[], Awaitable[list[Any]]],
) -> Any:
    """Use the supplied address or a single unambiguous discovered endpoint."""
    if address is not None:
        return normalize_address(address)
    devices = await discover()
    if len(devices) != 1:
        raise ProbeError
    return devices[0]


async def async_main() -> None:
    """Validate the dependency and run the read-only event listener."""
    try:
        installed_version = version(REQUIRED_DISTRIBUTION)
    except PackageNotFoundError as exc:
        raise ProbeError from exc
    if installed_version != REQUIRED_VERSION:
        raise ProbeError

    password, address = read_environment()

    # Import only after logging is disabled in main; the dependency contains
    # address/raw-packet diagnostics that this probe must never expose.
    from CasambiBt import Casambi, discover  # noqa: PLC0415

    target = await resolve_target(address, discover)
    await listen(Casambi(), target, password)


def main() -> int:
    """CLI entrypoint with deliberately non-diagnostic, secret-safe failures."""
    logging.disable(logging.CRITICAL)
    try:
        asyncio.run(async_main())
    except ConfigurationError:
        sys.stderr.write("casambi switch probe: configuration error\n")
        return 2
    except KeyboardInterrupt:
        return 130
    # Every dependency failure is intentionally collapsed to a fixed message:
    # Bluetooth/library exceptions can contain the private network address.
    except Exception:  # noqa: BLE001
        sys.stderr.write("casambi switch probe: failed\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
