# Casambi MQTT Bridge — HAOS app

Use this profile when the bridge shares a Home Assistant OS host. The bridge
runtime and MQTT/entity identities are the same as the independent Linux
installation; only process supervision differs. See
[deployment modes](../docs/deployment-modes.md).

The app uses host BlueZ through D-Bus. Leave HA's native Bluetooth entry disabled
if deliberately disabled; ESPHome proxies are independent. The app never invokes
systemd, restarts global BlueZ, reboots HA/the host, or needs host/HA API access.

## First start and modes

The app supports automatic boot. Keep `mode: disabled` until private settings are
saved and checked. A small outer supervisor stays alive in every mode, so
Supervisor Watchdog cannot turn an intentional stop/latch into a restart storm.

- `disabled`: no bridge child, Bluetooth or broker operations. Only the private
  controller-liveness HTTP listener runs.
- `preflight`: reads hci0 Powered/Discovering once; no discovery, power changes,
  Casambi connection or MQTT command subscription. The outer supervisor remains
  alive afterward. This proves D-Bus access, NOT actual Casambi connectivity.
- `bridge`: validates the trusted settings and launches normal Casambi/MQTT
  control. This mode is NOT read-only. Preserve the existing broker, network name,
  topics/payloads and HA config entry/entity identities.

The one-shot `run.py` helper remains separately testable; its exit semantics are
not the lifecycle of the persistent Docker entrypoint.

## Private configuration restore

Keep original recovered files. Never put credentials in Telegram, GitHub issues,
screenshots or build logs. Generate a complete options document locally:

```sh
python3 -c 'import json,pathlib,sys; print(json.dumps({"mode":"disabled","env":pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"),"recovery_reset_generation":0}))' /path/to/recovered.env
```

Paste the **WHOLE output**, replacing the whole HA app YAML-options document.
JSON is valid YAML. Do not copy an escaped JSON substring into `env: |`: JSON
backslash escapes such as `\"` must be decoded as part of the complete document,
not become password characters. If editing manually, use a literal `env: |` block
and preserve the original .env quoting, values and special characters.

The app parses without shell sourcing or interpolation. `/data/.env` is supported
when `env` is absent/empty. Explicit MQTT_BROKER, MQTT_PORT, MQTT_USERNAME,
MQTT_PASSWORD, CASAMBI_NETWORK_ADDRESS, CASAMBI_NETWORK_PASSWORD and
CASAMBI_NETWORK_NAME are required. Save without starting, then validate before
selecting normal bridge mode. Do not recreate the existing HA integration.

The trusted library cache persists under `/data/casambi-bt-store`. Preserve the
whole cache including `.cachever`; it contains pickle, so never import an
untrusted cache. There is no upload webserver or broad host-file mount.

## Three-replacement recovery budget

The HAOS supervisor owns one fixed bridge child. An original launch may be
followed by at most **three automatic replacement launches**. Attempts are charged
and durably saved BEFORE spawning. The budget/latch survives container,
Supervisor and host restarts; ordinary Stop/Start never silently clears it.

A new bridge process after a clean app restart also consumes a replacement
reservation. This conservative rule cannot mistake a watchdog-triggered SIGTERM
for a human reset. Rearm explicitly after planned maintenance when needed.

Retryable child exits or confirmed process hangs receive bounded recovery.
Configuration/Casambi authentication exit78 and definite MQTT authentication
rejections stop recovery instead of consuming a futile restart loop. Broker or
network transport outages alone are not reasons to restart a healthy process.

The child health record is process-bound and phase-aware. It lives in the
container's RAM-backed `/dev/shm/casambi-health.json`, not an SD heartbeat file.
Only meaningful budget/incident changes persist in `/data/recovery/state.json`.
Old boot/PID/reused-process records must not make a new child look healthy.

A terminal latch leaves the supervisor alive but **no bridge child**. Its internal
`/supervisor-health` endpoint means controller liveness, NOT Casambi readiness.
HTTP200 during a terminal latch is intentional: otherwise native Supervisor
Watchdog could repeatedly restart the app around the retry limit. A stale
controller loop fails that liveness check; a restarted controller still respects
the persisted bridge budget. The port is not published to the host network.

## Failure notification and explicit rearm

The supervisor publishes a fixed-schema retained failure record to
`casambi/<network>/recovery`; it does not overwrite bridge `/status` or accept
arbitrary MQTT recovery commands. Install
[the notification automation example](recovery-notification.yaml), replacing
`NETWORK_NAME` consistently with the existing MQTT namespace. It creates one
persistent HA notification with a stable ID. QoS/replay/app/HA restarts do not
create a collection of duplicate notifications.

If the broker uses restrictive ACLs, permit the app to publish this exact
network's recovery topic and HA to subscribe to it. No additional command-topic
permissions or wildcard host access are needed.

If the broker or its credentials are unavailable, immediate delivery through
that same broker is impossible. The app retains the incident/outbox and replays
when transport/authentication works again; inspect app logs in the meantime.

After correcting the cause, increase `recovery_reset_generation` explicitly
(for example, `0` to `1`), save and restart the app. A repeated value does not rearm.
This initial policy has **manual-only rearm**, not a hidden reset after brief
`ready`. Explicit rearm publishes `armed` with attempts0 and dismisses the old HA
notification; a new failure creates a new incident. Corrupt/unwritable recovery
state fails closed and requires validated backup/operator repair, not guessing
or deleting the Casambi cache.

## Health ownership and limits

- App supervision owns process/child liveness, confirmed disconnects, startup and
  persistent restart limits.
- The existing HA integration keeps its independent broker-delivery/consumer
  health and scoped entry-reload policy. Do not replace that budget with the
  app's three-replacement budget.
- A cached BLE-connected flag or snapshot is NOT proof of a physical radio round
  trip. casambi-bt0.3.2 exposes no non-actuating RF ping. Fresh raw local `ready`
  also must not be mistaken for successful publication or HA consumption.

## Build and tests

The self-contained app context fetches one reviewed bridge commit and verifies
its archive SHA256. There is no second maintained bridge implementation and no
registry login is required. CI builds the real image, checks standalone imports,
controller liveness/inert modes/config latch and a clearly synthetic exit75 child
(original+three replacements, then no further launch after container restart).
Network/radio are disabled in those tests. They do not replace live D-Bus/BLE/HA
acceptance. No fault injection should toggle real lights or reset the shared host.
