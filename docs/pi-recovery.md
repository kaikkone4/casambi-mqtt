# Bounded BLE/BlueZ recovery on the bridge host

This change keeps a Bluetooth discovery failure (such as the
`org.bluez.Error.InProgress` loop of 27.10.2026, which reached
`NRestarts=2614`) from turning into hours of restarts. It tries the smallest
fix first and makes failure visible instead of looping. Nothing here has been
installed on a real host. Do the device smoke test and any production deploy
only with separate permission, from a published release tag, never with a
blind `git pull`.

What this fixes is the **loop**. It does not claim to have found what left
BlueZ in that state on 27.10.2026, and it is not a permanent fix for that
root cause.

## Layers, smallest first

1. **Inside the bridge** (`server.py`, unprivileged):
   - Discovery and connect each have their own deadline: 30 s and 90 s.
   - They are retried with backoff of 1, 2, 4, 8 and 16 s, capped at 30 s, plus up to 20 % jitter. That is six attempts in all.
   - Exactly one scan or connect runs at a time. A timed-out attempt is cancelled and awaited, then the BLE state is released before the next attempt.
   - Failures are classified by the stable D-Bus error name (`org.bluez.Error.InProgress` → `ble_discovery_in_progress`), never by the human-readable text.
   - A rejected Casambi login or an invalid setting is `configuration`. It is not retried, and the bridge exits with **78**.
   - Exhausted retries exit after a bounded (10 s) MQTT publish of `phase=offline` with that category, so Home Assistant can see the failure even though no normal session ever existed. Exit **76** means a BLE startup failure (`ble_discovery_in_progress`, `ble_discovery_failed`, `ble_connect_failed`); exit **79** means network not found or unclassified, which a Bluetooth reset would not fix.
   - A cancelled BLE call gets 10 s to unwind. bleak's scanner stop is a D-Bus call without a timeout, the same call that failed on 27.10.2026. If it ignores cancellation, the bridge does not stay alive and stuck: it reports the failure and leaves at once with exit 76.
   - A lost BLE link still exits with **75**, as before, also when its BLE disconnect hangs and the process has to leave hard. Only a failure before the first successful connect can be 76.
   - An MQTT outage is retried inside the process. It never exits for it and never touches Bluetooth.
2. **systemd** (`deploy/systemd/casambi-mqtt.service`):
   - `Restart=on-failure`, `RestartSec=10`, `RestartPreventExitStatus=78`.
   - `StartLimitBurst=5` within `StartLimitIntervalSec=600`. The old 10 s window never limited anything. Now about five failing processes (roughly 5–6 minutes) put the unit in `failed` instead of looping for hours.
3. **Root-owned supervisor** (`scripts/bridge_recovery.py`, run by
   `casambi-mqtt-recovery.service` from `OnFailure=` and a 1-minute timer):
   - It counts failures from what **systemd** reports for every single exit. The bridge unit's `ExecStopPost=+…bridge_recovery.py record-exit` runs as root after each exit, including every automatic restart and the final exit when systemd gives up. It receives systemd's own `$EXIT_CODE`/`$EXIT_STATUS` and appends them to a small root-owned exit log under its own brief lock. Each supervisor run consumes that log.
     - This is trusted data. Nothing is lost between timer runs, and nothing is counted twice.
     - `systemctl show` alone cannot provide this: systemd clears `ExecMainStatus` whenever it restarts the process.
   - A bridge process that is alive but has not progressed out of `discovering`/`connecting` for 10 minutes is treated as a repeated BLE fault by itself, because it has missed many retry cycles and will not exit on its own. With the bounded unwind above this should not happen; it is a second line of defence.
   - It resets Bluetooth only after **two** BLE startup failures (exit 76) within 10 minutes, and only while the unit's current main process is not `ready`.
   - The sequence is fixed:
     1. stop the bridge;
     2. wait (bounded) until its process is gone;
     3. restart `bluetooth.service`;
     4. run `reset-failed` on the bridge, only if it hit its start limit;
     5. start the bridge;
     6. verify that a **new** bridge process reports `ready` within 3 minutes.
   - Budget: at most **one reset per 10 minutes and two per 30 minutes**. It is charged before the first action and stored in root-owned state, so bridge restarts never refill it.
   - When the budget is spent, or any step fails, it latches `recovery_failed`, logs one `crit` line to the journal, and does nothing more.
   - The latch clears only after the supervisor **itself** has seen the unit's main process `ready` for an hour, measured from its own observations rather than any time the record claims, or by `bridge_recovery.py reset`.
   - If systemd gave up on the bridge for another reason (not a configuration error, exit 78), it does one plain `reset-failed` and `start` per 30 minutes, without touching Bluetooth. This covers, for example, lights that were switched off at the wall for a while.
   - Budgets use the kernel's boot-scoped `CLOCK_BOOTTIME`, so an NTP clock step cannot refill them.
   - After a reset whose verification fails, the next reset is still held back by the 10-minute limit. In practice the supervisor then latches `recovery_failed` on the next repeated failure instead of trying again. This is deliberately conservative.
   - It never touches any other unit, never reboots, and never runs a shell.
   - MQTT errors, configuration errors, `network_not_found`, a BLE drop after the bridge had been ready, and Home Assistant consumer problems cause **zero** Bluetooth resets.

A host **reboot is not implemented** as a recovery step. An opt-in reboot step
would need its own approval and evidence that it works. Leave any existing
maintenance reboot you already run unchanged.

## Local health record

The bridge atomically replaces `HEALTH_RECORD_PATH` (in the unit:
`/run/casambi-mqtt/health.json`) on every phase change and heartbeat. It
writes a temporary file and then renames it; it never appends.

The record contains only fixed enums, integers and random IDs:

- process identity: instance ID, PID, kernel start time and boot ID;
- state: phase, error category, link states;
- timing: `ever_ready`, `ready_since` and `updated`, on the system-wide
  `CLOCK_MONOTONIC`.

The supervisor parses it as **untrusted**, advisory input:

- It opens the file without following symlinks and without blocking, and
  only accepts a regular file of limited size.
- A record from another boot is ignored.
- A record counts only if its PID is the unit's current `MainPID` (from
  systemd) and the process start time matches. An old file, or one that
  names some other process, can never make the bridge look healthy.
- Failure counting never relies on the record; it uses systemd exit
  statuses. The record only adds the "stuck" signal, the `ready` check and
  the MQTT reconnect count shown by `status`. A compromised bridge user could
  still claim `ready`, or exit with status 76 on purpose. Neither escalates
  beyond the budget of two Bluetooth resets per 30 minutes.

## Supervisor settings

These are environment variables in the root-owned unit; out-of-range values
refuse to start (exit 78).

| Variable | Default | Range |
|---|---|---|
| `RECOVERY_BRIDGE_UNIT` | `casambi-mqtt.service` | plain `*.service` name, not `bluetooth.service` |
| `RECOVERY_HEALTH_RECORD` | `/run/casambi-mqtt/health.json` | absolute path |
| `RECOVERY_STATE_DIR` | `/var/lib/casambi-mqtt-recovery` | absolute path |
| `RECOVERY_FAILURES_TO_RESET` | 2 | 2–10 |
| `RECOVERY_FAILURE_WINDOW_SECONDS` | 600 | 60–86400 |
| `RECOVERY_MAX_RESETS_SHORT` / `_SHORT_WINDOW_SECONDS` | 1 / 600 | 1–5 / 300–86400 |
| `RECOVERY_MAX_RESETS_LONG` / `_LONG_WINDOW_SECONDS` | 2 / 1800 | 1–10 / 600–604800 |
| `RECOVERY_STOP_TIMEOUT_SECONDS` | 60 | 5–600 |
| `RECOVERY_VERIFY_SECONDS` | 180 | 30–1800 |
| `RECOVERY_HEALTHY_RESET_SECONDS` | 3600 | 600–604800 |

## Install (on the bridge host, with permission)

Replace the placeholders with your own values. Do not put host names, IPs or
credentials into the repository.

```bash
# 0. Back up what runs today (host-local, never into Git).
sudo install -d -m 0700 /root/casambi-mqtt-backup
sudo cp -a /etc/systemd/system/casambi-mqtt.service /root/casambi-mqtt-backup/ 2>/dev/null || true
sudo cp -a <install-dir>/.env <install-dir>/casambi-bt-store /root/casambi-mqtt-backup/ 2>/dev/null || true

# 1. Check out the reviewed release tag in <install-dir> as the bridge user.

# 2. Install the supervisor root-owned, outside the bridge checkout.
sudo install -d -o root -g root -m 0755 /usr/local/libexec/casambi-mqtt
sudo install -o root -g root -m 0755 scripts/bridge_recovery.py /usr/local/libexec/casambi-mqtt/

# 3. Render and install the units.
sed -e 's|@INSTALL_DIR@|<install-dir>|g' -e 's|@BRIDGE_USER@|<user>|g' \
    -e 's|@BRIDGE_GROUP@|<group>|g' deploy/systemd/casambi-mqtt.service \
  | sudo tee /etc/systemd/system/casambi-mqtt.service >/dev/null
sudo install -m 0644 deploy/systemd/casambi-mqtt-recovery.service \
  deploy/systemd/casambi-mqtt-recovery.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now casambi-mqtt-recovery.timer
sudo systemctl restart casambi-mqtt.service
```

Inspect: `journalctl -u casambi-mqtt -u casambi-mqtt-recovery`, and
`sudo /usr/bin/python3 -I /usr/local/libexec/casambi-mqtt/bridge_recovery.py status`.
After a `recovery_failed`, fix the cause, then acknowledge with
`sudo env RECOVERY_STATE_DIR=/var/lib/casambi-mqtt-recovery /usr/bin/python3 -I /usr/local/libexec/casambi-mqtt/bridge_recovery.py reset`.

## Rollback

```bash
sudo systemctl disable --now casambi-mqtt-recovery.timer
sudo rm /etc/systemd/system/casambi-mqtt-recovery.{service,timer}
sudo cp -a /root/casambi-mqtt-backup/casambi-mqtt.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl restart casambi-mqtt.service
```

Then check out the previous release tag. The state in
`/var/lib/casambi-mqtt-recovery` can be removed afterwards.
