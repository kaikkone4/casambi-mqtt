# Two deployment profiles, one bridge

Choose where the bridge runs. Do not run both profiles for the same Casambi
network/MQTT namespace: competing BLE connections and duplicate command consumers
are not a redundancy mechanism. Stop the old bridge before activating its
replacement, and preserve the existing network name and HA entity identities.

## Independent Linux host

Use the existing standalone installation and systemd services on a dedicated
Linux/Raspberry Pi host. See [the recovery runbook](pi-recovery.md) for its existing
root-owned recovery policy and explicitly enabled host-level actions.

This profile remains available when Home Assistant OS is elsewhere or the bridge
needs a separately located Bluetooth adapter. No HAOS supervisor is required.
The standalone executor is unchanged by the HAOS app implementation.

## Shared Home Assistant OS host

Install the [HAOS app](../haos_casambi/DOCS.md). It packages the same checksum-pinned
bridge source plus an app-only child-process supervisor. It uses host D-Bus, not
HA's native Bluetooth scanner or an ESPHome proxy.

- Autostart and native Supervisor Watchdog concern the outer controller.
- The controller allows the original launch plus at most three automatic
  replacement launches, then persists a terminal latch and publishes an MQTT
  failure notification. HA displays one persistent notification per network.
- Container/Supervisor/host restarts do not reset that budget. Explicitly increase
  `recovery_reset_generation` after correcting the cause to rearm it.
- No root recovery scripts, BlueZ reset, Core restart or host reboot are invoked.
  The app gets no HA/Supervisor API token, host-network access or broad privileges.
- A terminally latched outer controller intentionally stays alive without a bridge
  child. HTTP liveness is not MQTT delivery, HA consumer health or physical RF proof.

The integration's existing scoped consumer-reload recovery remains independent.
Do not copy the standalone root/systemd executor into the shared HAOS host.

## Compatibility and maintenance

Both profiles retain the same MQTT topics/payloads, network namespace and HA
integration/entity IDs. No second bridge-code fork is maintained. Deployment
updates and recovery wrappers can evolve independently without giving a shared
home-automation host the dedicated appliance's reboot permissions.

Tests of the three-replacement limit use clearly synthetic subprocess/container
children with no real BLE or light commands. A live app update must separately
verify actual broker/consumer health and preserve disabled HA Bluetooth entries,
ESPHome proxies, automation targets and entity identities. A configured autostart
setting is not proof of a physical reboot test.
