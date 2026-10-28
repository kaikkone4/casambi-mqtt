# Bridge health protocol, version 1

This protocol lets Home Assistant tell four things apart that a stale light
state alone cannot distinguish:

1. the bridge **process** is alive,
2. its **BLE/Casambi** link is believed connected,
3. **MQTT** carries its messages both ways, and
4. this Home Assistant config entry's **own unit consumer** still handles
   unit messages.

Every message is **non-actuating**. Nothing in this protocol turns on, dims,
or switches a Casambi unit or scene, and no consumer of it may treat an old
`last_updated`/`last_reported` timestamp on a light as evidence of a fault:
unchanged payloads legitimately leave those unchanged.

The existing light, scene, switch-event and command topics, payloads and
entity identities are unchanged.

## Topics

| Topic | Direction | QoS | Retained | Purpose |
|---|---|---|---|---|
| `casambi/<network>/status` | bridge → HA | 1 | yes | heartbeat, phase changes, last will |
| `casambi/<network>/health/reply` | bridge → HA | 1 | no | correlated probe replies |
| `casambi/<network>/commands` | HA → bridge | 1 | no | existing topic; adds `GET_STATUS` and an optional `request_id` |

### Minimum broker ACLs

- Bridge user: **write** `casambi/<network>/status` and
  `casambi/<network>/health/reply` (in addition to its existing
  `events/#`, `scenes/#`, `switch_events` write and `commands` read).
- Home Assistant user: **read** `casambi/<network>/status` and
  `casambi/<network>/health/reply` (in addition to its existing
  `events/#`, `scenes/#`, `switch_events` read and `commands` write).

## Status message

Published retained on `status`: immediately when an MQTT session starts
(`phase=starting`), after the baseline snapshot (`ready` or `degraded`), every
`HEALTH_HEARTBEAT_SECONDS` (default **30**, valid 5–300) from an independent
task, and once more, bounded to 2 s, when the session ends on purpose.

```json
{"protocol_version":1,"instance_id":"<32 lowercase hex>","sequence":17,
 "phase":"ready","ble":"connected","mqtt":"connected","uptime_s":3600,
 "snapshot":{"generation":3,"units":48,"scenes":8,"failed":0,"age_s":900},
 "error":"none","time":"2026-10-27T19:00:00Z"}
```

- `instance_id`: random per bridge **process**. It survives MQTT reconnects
  but never a process restart, so a new process cannot inherit a previous
  instance's `ready`.
- `sequence`: increases by one for every status (and every reply) of the
  instance. It is `null` only in the last will.
- `phase`: `starting`, `discovering`, `connecting`, `ready`, `degraded`,
  or `offline`. `discovering`/`connecting` happen before an MQTT session
  exists; they are listed for the local startup record of a later change and
  are not published over MQTT today.
- `ble`, `mqtt`: `connected`, `disconnected` or `unknown`, as the bridge
  believes them.
- `uptime_s`, `snapshot.age_s`: monotonic-clock durations.
- `snapshot`: counters of the last **correlated** snapshot (generation 0 means
  none yet). The empty legacy `events/` tombstone is never counted as a unit.
- `error`: one fixed category: `none`, `ble_disconnected`,
  `ble_discovery_in_progress`, `ble_discovery_failed`, `ble_connect_failed`,
  `network_not_found`, `configuration`, `mqtt_error`,
  `snapshot_publish_failed`, `state_publish_failed`, `connection_lost`,
  `shutdown`, `unknown`. Never free text.
- `time`: UTC, **diagnostic only**. No consumer uses it for freshness.

`degraded` means the bridge is up but: its last snapshot had a failed publish
(`snapshot_publish_failed`), live state publishing is in a failure streak
(`state_publish_failed`), or the BLE link was just lost (`ble_disconnected`,
published right before the process exits for a supervisor restart).

### Last will and shutdown

The MQTT client registers a retained last will on `status` with
`phase=offline`, `error=connection_lost`, `sequence=null`. A clean stop
(SIGINT, or SIGTERM from systemd, which the bridge now maps to the same
cancellation) publishes `phase=offline`, `error=shutdown` before
disconnecting.

## Requests

`request_id`: 1–64 characters from `A–Z a–z 0–9 . _ : -`. Anything else,
a missing ID on `GET_STATUS`, or an extra key is rejected with a fixed log
line that never echoes the request.

### `GET_STATUS`

```json
{"action":"GET_STATUS","request_id":"ha-1a2b3c4d5e6f7a8b"}
```

Answered from bridge memory only; it never touches a Casambi unit. Replies are
coalesced to at most one per second.

### Correlated snapshot

```json
{"action":"PUBLISH_ENTITIES","request_id":"ha-1a2b3c4d5e6f7a8b"}
```

The bridge publishes the same retained snapshot as the legacy command, and
**only after every publish has been confirmed by the broker** (aiomqtt awaits
the QoS 1 PUBACK) it replies with the confirmed counts. A partial failure
replies `result=failed`; it is never acknowledged as success. Correlated
snapshots are limited to one per 30 s; excess requests get a snapshot-type
reply with `result=rate_limited` and zero counters. A reply that cannot be
published is logged with a fixed line and never ends the MQTT session. The legacy `{"action":"PUBLISH_ENTITIES"}` without a
`request_id` is unchanged and not rate limited.

### Reply

```json
{"protocol_version":1,"type":"status","request_id":"ha-…","instance_id":"…",
 "result":"ok","status":{…status object…}}
```

`type=snapshot` replies add `units`, `scenes`, `failed`, `generation`.
`protocol_version` must be the JSON integer `1` (not `1.0` or `true`).
Status and snapshot replies are coalesced in separate one-second windows.
`result` is `ok`, `failed`, `rate_limited` or `busy`.

## What Home Assistant concludes

Home Assistant uses the **local monotonic receipt time** of each message.

- A **retained** status is history (it is replayed on every subscribe and
  reconnect) and never makes the bridge healthy.
- A status from an instance that has already been replaced, or a
  non-increasing `sequence` of the current instance, is ignored. The last will
  has no sequence and never lowers the highest one seen, so after an `offline`
  only a newer heartbeat -- the same process after an MQTT reconnect, or a new
  process -- is live again.
- `healthy` needs a live `ready` status received within the stale limit
  (default **120 s**). Within the startup grace (default **90 s**) without
  live status the bridge is `starting`, not failed.
- Without one **live** v1 status since setup the bridge is `unknown`, even
  if a retained one exists (it may be left over from a rolled-back bridge).
  If `GET_STATUS` then goes unanswered it is `unsupported`: an older bridge,
  or one that is down — the two cannot be told apart without a live message.
  Neither is ever a fault to recover from.
- A **status probe** succeeds only on a reply carrying the pending
  `request_id`, the live `instance_id`, before the deadline (default **20 s**).
  At most one probe is in flight; each kind runs at most every 5 minutes
  (status default 5 min, consumer default 15 min).
- A **consumer probe** (correlated snapshot) succeeds only if, **by the
  deadline**, in addition to an `ok` reply:
  - at least as many distinct unit topics reached Home Assistant during the
    probe as the bridge confirmed publishing, and
  - the entry's **real unit callback** handled **every** unit message that
    the separate raw listener saw during the probe, counted per topic.
  Handling of unrelated ambient updates therefore cannot certify a snapshot
  whose own messages were not handled. An unchanged dimmer value counts as
  handled. Nothing after the deadline counts. Outcomes:
  - full raw delivery with **nothing** handled: `consumer_stale`;
  - some handled but not all: `consumer_partial` (degraded);
  - missing raw delivery: `delivery_incomplete`, not blamed on the consumer;
  - a snapshot with zero units: `no_units`, which proves nothing;
  - a bridge instance change while the probe is pending: `superseded`, which
    is inconclusive. Evidence from one bridge process never certifies another.

What remains uncorrelated: unit payloads carry no snapshot identifier
(changing them would break older consumers), so the per-topic coverage
above, bounded by the probe window, is the binding.

## What this does not prove

The bridge reads unit state from casambi-bt's in-memory `casa.units` cache,
and casambi-bt 0.3.2 offers no non-actuating ping. A `ready` status and a
successful snapshot therefore prove the process, its MQTT session, and the
bridge's own belief that BLE is connected — **not** a fresh physical radio
round trip. A silent BLE failure that casambi-bt does not report is outside
what this protocol can detect.

## Compatibility, release and rollback

- New bridge + old Home Assistant component: the old component does not
  subscribe to `status`/`health/reply`; lights, scenes, switches and the
  reload button work as before.
- Old bridge + new component: the old bridge logs and ignores `GET_STATUS`
  and rejects a `PUBLISH_ENTITIES` with `request_id` by schema; the component
  reports `unsupported` and does nothing else.
- Rolling back the bridge leaves a retained v1 status on the broker. It is
  only history (never healthy). To remove it, publish an empty retained
  message to `casambi/<network>/status`.
