# Casambi MQTT for Home Assistant

A local-push Home Assistant integration for Casambi networks. A separate Bluetooth bridge publishes Casambi unit and scene state to MQTT; Home Assistant consumes those topics and sends control commands back through MQTT. This lets the Bluetooth bridge run on a different Linux host from Home Assistant.

> This project uses the unofficial [casambi-bt](https://github.com/lkempf/casambi-bt) library. It currently supports dimmable lights and scenes.

## Release notes

### 0.2.9

- Hardens the Bluetooth bridge against silent Casambi BLE disconnects: it exits for supervisor restart instead of remaining active without updates.
- Bounds and fairly schedules MQTT state publication, preserves the newest state during transient publish failures, and keeps retry diagnostics privacy-safe and rate-limited.
- The public MQTT contract and `casambi-bt==0.3.2` remain unchanged. This is reliability hardening, not a claim about the earlier incident's root cause.

### 0.2.8

- Fixes PTM/Casambi switch-event invocation frame decoding while retaining `casambi-bt==0.3.2` and the public sanitized MQTT contract.
- Button identities are intentionally numeric and configuration-specific; see **Button mapping in Home Assistant** below before creating automations.
- Switch events remain input-only: the bridge adds no event control path.

### 0.2.7

- Fixes the Home Assistant automation editor's **Kohteen mukaan** target-picker path by exposing each Casambi switch button as a read-only event entity; the device remains input-only, with no service, control entity, or actions.

### 0.2.6

- Fixes device-trigger enumeration and reload persistence on Home Assistant 2026.8.2.
- Known Casambi switches expose default buttons 1–4 plus any observed extra buttons.
- Switch devices heal after Home Assistant device-registry removal and recreation.

### 0.2.5

- Persists sanitized observed unit/button pairs across Home Assistant reloads so input-only device triggers remain available.
- A physical switch has device triggers, not conditions, actions, or control entities.

### 0.2.4

- Bridge v0.2.3+ publishes sanitized physical switch events to MQTT.
- HACS v0.2.4 subscribes to those events and discovers switch devices only after a physical event.
- The Home Assistant device triggers are input-only; no control entity or action is added for Casambi switches.

### 0.2.3

- The bridge now publishes sanitized switch callbacks to the bridge-only MQTT topic `casambi/<network>/switch_events` as `{"unit_id":<0-255>,"button":<0-255>,"event":"<type>"}`.
- Events use QoS 1, are not retained, and identical `(unit_id, button, event)` callbacks are deduplicated within 250 ms.
- This adds no Home Assistant automation integration or device-control path; consumers subscribe to the MQTT event topic directly.

## Architecture and requirements

You need all of the following:

- A configured MQTT broker and Home Assistant's [MQTT integration](https://www.home-assistant.io/integrations/mqtt/)
- Home Assistant 2025.2.4 or newer
- A Linux Bluetooth host for the bridge (the host needs Bluetooth access and the relevant Casambi network credentials)

The Home Assistant custom component and Bluetooth bridge are separate deployments. Installing this repository in HACS does **not** install or start the bridge.

## Install the Home Assistant integration with HACS

This repository is currently distributed as a **HACS custom repository**.

1. In Home Assistant, open **HACS → Integrations**.
2. Open the three-dot menu → **Custom repositories**.
3. Add `https://github.com/kaikkone4/casambi-mqtt` with category **Integration**.
4. Search for **Casambi MQTT** and install the latest release.
5. Restart Home Assistant.
6. Go to **Settings → Devices & services → Add integration**, then add **Casambi MQTT**.
7. Enter the MQTT network name used by the bridge. It must be one literal MQTT topic level (no `/`, `+`, or `#`).

For upgrades that include an entity migration, keep the existing config entry and restart Home Assistant. Do not delete and recreate the integration: the migration preserves existing entity IDs and user customizations where safely possible.

## Install the Bluetooth bridge from source

A maintained Docker image is not published for this release. Use the latest GitHub Release tag in place of `<release-tag>` on the Bluetooth host instead.

```bash
git clone --branch <release-tag> --depth 1 https://github.com/kaikkone4/casambi-mqtt.git
cd casambi-mqtt
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-server.txt
cp .env.example .env
```

Set the required MQTT and Casambi values in the local `.env` file. Keep it private: do not commit it or paste its contents into issues. Then start the bridge:

```bash
python server.py
```

For production, run the bridge under a service manager such as systemd and deploy only reviewed release tags. Do not point a production bridge at an unreviewed branch.

The bridge logs available Casambi networks when it cannot find a configured network. It publishes state below `casambi/<network>/events/` and `casambi/<network>/scenes/`; commands are accepted at `casambi/<network>/commands`.

### Switch event MQTT contract

While the normal bridge has both Casambi and MQTT connected, it also translates
supported `casambi-bt==0.3.2` switch callbacks to this event-only MQTT contract:

- **Topic:** `casambi/<network>/switch_events`, where `<network>` is the same
  validated `CASAMBI_NETWORK_NAME` topic level used by the existing bridge topics.
- **Payload:** compact UTF-8 JSON with exactly `{"unit_id":<0-255>,"button":<0-255>,"event":"<type>"}`.
  The supported event types are `PRESS`, `RELEASE`, `HOLD`, and
  `RELEASE_AFTER_HOLD`; malformed and unknown callback values are dropped.
- **Delivery:** QoS 1 with `retain=false`. Switch events are not state and are
  never retained by the bridge.

The bridge does not include a unit name, Bluetooth address, network identifier,
raw callback payload, or credentials in this message. Identical sanitized
`(unit_id, button, event)` callback bursts within 250 ms are collapsed using a
monotonic clock. A `PRESS` and `RELEASE` are distinct tuples, so the real
press/release sequence is preserved. The handler is unregistered whenever the
MQTT session reconnects or the bridge shuts down. This topic does not control a
device and does not create or trigger a Home Assistant entity, scene, or action.

### Button mapping in Home Assistant

`button` is the numeric event identity configured by Casambi. It is **not** a
universal physical rocker label: depending on the switch model and its Casambi
configuration, a four-button/2-rocker switch can report non-sequential values
or values such as `6` and `8`. Treat each switch's mapping as device-specific.

Before creating an automation, open that switch's page in the Home Assistant
Casambi MQTT integration and press one physical button at a time. Confirm which
button event/entity activates, then bind the automation to that observed numeric
button. Do not assume that the same physical position uses the same number on a
different Casambi switch or after its Casambi configuration changes.

For long press behavior, observe the switch's own event sequence before using
it for dimming or stateful automation: configurations can differ between
`HOLD`, `RELEASE_AFTER_HOLD`, and `PRESS`/`RELEASE` sequences.

### Bounded switch-event probe

The bridge has an explicit 90-second, read-only diagnostic mode for switch
events. Run it from the bridge's normal working directory **only while the
service is stopped**:

```bash
sudo systemctl stop casambi-mqtt.service && ./.venv/bin/python server.py switch-event-probe
```

The mode loads the same `.env` variables and constructs the same
`casambi-bt==0.3.2` `Casambi()` connection as the normal bridge. Consequently,
when it is run from the service's working directory, it uses that deployment's
existing `casambi-bt-store` cache/session. It does not start an MQTT client or
invoke control, configuration, pairing, or reset operations. Each supported
event is emitted as one JSON object containing only `event`, `button`, and
`unit_id`; all failures use a fixed, non-diagnostic message.

### Addressless Casambi units

Some Casambi units do not expose a usable Bluetooth address. For those units, the bridge uses their stable UUID only for MQTT and Home Assistant identity (`events/uuid/<uuid>`). The original Casambi address is retained in the control path. Do not manually create retained payloads on the legacy bare `events/` topic.

## Development

### Bridge tests

```bash
python -m pip install -r requirements-server.txt -r requirements-dev.txt
pytest -q tests/test_server.py
```

### Home Assistant component tests

The component test environment is intentionally separate from the bridge dependencies because their MQTT dependency versions conflict.

```bash
uv venv --python 3.13 .venv-ha-test
. .venv-ha-test/bin/activate
python -m pip install -r requirements-test.txt
pytest -q tests/test_light.py tests/test_integration.py tests/test_migration.py -o asyncio_mode=auto
```

CI runs bridge tests, component tests, Ruff, Hassfest, and HACS validation on every pull request.

## Safety notes

- Do not use test or troubleshooting commands that actuate Casambi units unless you explicitly intend to control them.
- A safe state-only bridge refresh is the MQTT command `{"action":"PUBLISH_ENTITIES"}`.
- Do not commit `.env`, Casambi Bluetooth state, credentials, tokens, or hardware-local data.

## License

MIT. See [LICENSE](LICENSE).
