# Casambi MQTT Bridge — experimental HAOS app

This app uses the host BlueZ service over D-Bus. Leave HA's native Bluetooth
entry disabled if it was deliberately disabled; ESPHome proxies are independent.
No host Bluetooth-service restart, host recovery hooks, extra capabilities or
Home Assistant restart are provided or required by this package.

## Safe first start

Boot is **manual_only** during this experiment; start is always explicit.
The default `mode: disabled` exits successfully without network or Bluetooth
calls. Set `mode: preflight` to read hci0 Powered and Discovering properties only.
It does not scan, change power, connect to Casambi or subscribe to MQTT.
A successful preflight is NOT proof of actual Casambi connectivity.
The app stops after printing the result; this is intentional, not a crash.

## Private configuration restore

Keep the original recovered files and image. Never paste credentials into
Telegram, GitHub issues, screenshots or build logs. In the app's own Configuration
YAML editor, the password-type `env` option may contain the recovered .env text.
Generate JSON (valid YAML) **locally on the laptop with the recovered file**:

```sh
python3 -c 'import json,pathlib,sys; print(json.dumps({"mode":"disabled","env":pathlib.Path(sys.argv[1]).read_text()}))' "$HOME/casambi-rescue/recovered/bridge.env"
```

Paste that output ONLY into this app's HA Configuration editor, then save.
It contains passwords; do not send it in chat. The script reads the original file
without modifying it. No interpolation, shell sourcing or password-in-argv is
performed by the app. A trusted `/data/.env` file is also supported when `env`
is absent/empty. Explicit MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD,
CASAMBI_NETWORK_ADDRESS, CASAMBI_NETWORK_PASSWORD and CASAMBI_NETWORK_NAME are
required; keep their original values and network namespace (for example `koti`).
Do not recreate the HA integration or entities.

The library's cache is persistent at `/data/casambi-bt-store`. If restoring an
existing trusted cache, preserve the whole directory including `.cachever`.
It contains pickle files: never import an untrusted cache. This package has no
web upload endpoint or broad host-file mount. With an empty cache the library
has its normal online bootstrap path, but actual success still depends on the
network credentials, network availability and Bluetooth connection.

## Normal operation is not read-only

Only explicitly selecting `mode: bridge` and starting the app starts normal
operation: it connects to Casambi, publishes state and accepts normal MQTT
control commands. It is NOT a read-only test mode. Do not send light or scene
commands solely to test the migration. Preserve the existing broker, network
name, topic/payload contracts and HA config entry/entity identities.

The original bridge retains its bounded startup/cleanup protections and exit
codes75/76/78/79. This experimental app has no automatic restart loop, Supervisor
watchdog or host BlueZ reset. Automatic boot/recovery is a separate acceptance
gate after reliable live operation is demonstrated; this is not yet an
unattended-production reliability claim.

## Build and verification

The app builds natively once from a small app-local context; it downloads the
existing bridge at an exact commit and verifies the archive SHA256. The bridge
source is not maintained as a second copy. No container-registry login or image
publication is needed. CI builds this exact Dockerfile and runs actual container
smoke checks with network disabled; those checks do not substitute for a real
Pi5 D-Bus/BLE test. Persistent /data is part of the HA app backup.
