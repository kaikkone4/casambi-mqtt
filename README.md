# Casambi MQTT for Home Assistant

A local-push Home Assistant integration for Casambi networks. A separate Bluetooth bridge publishes Casambi unit and scene state to MQTT; Home Assistant consumes those topics and sends control commands back through MQTT. This lets the Bluetooth bridge run on a different Linux host from Home Assistant.

> This project uses the unofficial [casambi-bt](https://github.com/lkempf/casambi-bt) library. It currently supports dimmable lights and scenes.

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

A maintained Docker image is not published for this release. Use the tagged source release on the Bluetooth host instead.

```bash
git clone --branch v0.2.1 --depth 1 https://github.com/kaikkone4/casambi-mqtt.git
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

For production, run the bridge under a service manager such as systemd and deploy only signed-off Git tags. Do not point a production bridge at an unreviewed branch.

The bridge logs available Casambi networks when it cannot find a configured network. It publishes state below `casambi/<network>/events/` and `casambi/<network>/scenes/`; commands are accepted at `casambi/<network>/commands`.

### Addressless Casambi units

Some Casambi units do not expose a usable Bluetooth address. For those units, the bridge uses their stable UUID only for MQTT and Home Assistant identity (`events/uuid/<uuid>`). The original Casambi address is retained in the control path. Do not manually create retained payloads on the legacy bare `events/` topic.

## Development

### Bridge tests

```bash
python -m pip install -r requirements.txt -r requirements-dev.txt
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
