"""
Config-entry-scoped consumer supervision and self-recovery (issue #28).

Two objects with deliberately different lifetimes:

* ``RecoveryManager`` -- one per config entry, kept in ``hass.data`` and in a
  ``Store``. It holds the reload lock, the in-progress flag, the reload
  budget and the ``recovery_failed`` latch, so all of them survive the
  entry's own unload/reload (and Home Assistant restarts, for the budget).
* ``HealthCoordinator`` -- recreated with every setup of the entry. It turns
  the HealthObserver's evidence into one state and asks the manager for a
  recovery only on correlated proof that the broker/bridge path works while
  this entry's consumer does not.

Only ``hass.config_entries.async_reload(entry_id)`` of the affected entry is
ever used: no Home Assistant restart, no other entry, no Bluetooth or bridge
action, and never a light or scene command.
"""

from __future__ import annotations

import asyncio
import time
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from homeassistant.components import persistent_notification
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import STATE_UNAVAILABLE
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from .const import DOMAIN, LOGGER
from .health import BridgeState, ProbeKind, ProbeOutcome

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from homeassistant.config_entries import ConfigEntry

    from .health import HealthObserver, HealthSettings, ProbeResult

RECOVERY_STORE_VERSION = 1
RECOVERY_WINDOW_SECONDS = 1800
MAX_RELOADS_PER_WINDOW = 2
VERIFY_POLL_SECONDS = 5
VERIFY_PROBE_SPACING_SECONDS = 30
DATA_RECOVERY = "recovery_managers"


class ConsumerState(StrEnum):
    STARTING = "starting"
    HEALTHY = "healthy"
    BRIDGE_UNAVAILABLE = "bridge_unavailable"
    CONSUMER_STALE = "consumer_stale"
    RECOVERING = "recovering"
    RECOVERY_FAILED = "recovery_failed"
    UNSUPPORTED = "unsupported"
    DEGRADED = "degraded"


def recovery_issue_id(entry_id: str) -> str:
    return f"recovery_failed_{entry_id}"


def unsupported_issue_id(entry_id: str) -> str:
    return f"bridge_unsupported_{entry_id}"


class RecoveryManager:
    """Reload lock, budget and latch for one entry; outlives its reloads."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry_id: str,
        *,
        wall: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.hass = hass
        self.entry_id = entry_id
        self._wall = wall
        self._monotonic = monotonic
        self._sleep = sleep
        self._store: Store[dict[str, Any]] = Store(
            hass, RECOVERY_STORE_VERSION, f"{DOMAIN}.{entry_id}.recovery"
        )
        self.lock = asyncio.Lock()
        self.in_progress = False
        self.reloads: list[float] = []
        self.failed = False
        self.reason = "none"
        self.last_result = "none"
        self.last_recovery: str | None = None
        self.stale_streak = 0
        self.last_stale_at: float | None = None
        self.listeners: list[Callable[[], None]] = []
        self.task: asyncio.Task[None] | None = None

    async def async_load(self) -> None:
        data = await self._store.async_load()
        if not isinstance(data, dict):
            return
        reloads = data.get("reloads")
        if isinstance(reloads, list) and all(
            isinstance(item, int | float) and not isinstance(item, bool)
            for item in reloads
        ):
            self.reloads = [float(item) for item in reloads]
        if isinstance(data.get("failed"), bool):
            self.failed = data["failed"]
        for key in ("reason", "last_result"):
            if isinstance(data.get(key), str):
                setattr(self, key, data[key][:64])
        if isinstance(data.get("last_recovery"), str):
            self.last_recovery = data["last_recovery"][:40]
        if self.failed:
            # Repair issues are not persistent: restore the acknowledgement
            # path together with the latch after a Home Assistant restart.
            self._create_failed_issue()

    async def _async_save(self) -> None:
        await self._store.async_save(
            {
                "reloads": self.reloads,
                "failed": self.failed,
                "reason": self.reason,
                "last_result": self.last_result,
                "last_recovery": self.last_recovery,
            }
        )

    @callback
    def _notify(self) -> None:
        for listener in list(self.listeners):
            listener()

    def recent_reloads(self) -> list[float]:
        # A timestamp in the future (clock stepped back) stays counted, so
        # a clock change can only make the budget stricter.
        now = self._wall()
        return [
            value for value in self.reloads if now - value < RECOVERY_WINDOW_SECONDS
        ]

    # -- stale streak --------------------------------------------------------

    def record_stale(self, confirm_seconds: float, max_gap: float) -> bool:
        """
        Count a stale proof; return True once two consecutive ones agree.

        The second proof must come at least ``confirm_seconds`` and at most
        ``max_gap`` after the first; an older first proof starts over.
        """
        now = self._monotonic()
        if (
            self.last_stale_at is None
            or self.stale_streak == 0
            or now - self.last_stale_at > max_gap
        ):
            self.stale_streak = 1
            self.last_stale_at = now
            return False
        if now - self.last_stale_at < confirm_seconds:
            return False
        self.clear_streak()
        return True

    def clear_streak(self) -> None:
        self.stale_streak = 0
        self.last_stale_at = None

    # -- recovery --------------------------------------------------------------

    @callback
    def request_recovery(self, settings: HealthSettings) -> str:
        """
        Start at most one reload of this entry, within the budget.

        Synchronous on purpose: ``in_progress`` is set before this returns,
        so a callback failure, a deadline timer and a reconnect that all
        report the same fault in one loop iteration produce one reload.
        """
        if self.in_progress:
            return "in_progress"
        if self.failed:
            return "recovery_failed"
        recent = self.recent_reloads()
        if len(recent) >= MAX_RELOADS_PER_WINDOW:
            self.hass.async_create_task(self._async_latch("budget_exhausted"))
            return "budget_exhausted"
        if recent and self._wall() - max(recent) < settings.recovery_cooldown_seconds:
            return "cooldown"
        self.in_progress = True
        self.last_result = "recovering"
        self._notify()
        # A Home Assistant-level task, not an entry task: the reload unloads
        # this entry, which cancels the entry's own tasks.
        self.task = self.hass.async_create_background_task(
            self._async_recover(settings), name=f"{DOMAIN} consumer recovery"
        )
        return "started"

    async def _async_latch(self, reason: str) -> None:
        self.failed = True
        self.reason = reason
        self.last_result = "recovery_failed"
        await self._async_save()
        LOGGER.error("Casambi MQTT consumer recovery stopped: %s", reason)
        self._create_failed_issue()
        self._notify()

    @callback
    def _create_failed_issue(self) -> None:
        ir.async_create_issue(
            self.hass,
            DOMAIN,
            recovery_issue_id(self.entry_id),
            is_fixable=True,
            is_persistent=False,
            severity=ir.IssueSeverity.ERROR,
            translation_key="recovery_failed",
            data={"entry_id": self.entry_id},
        )

    async def _async_recover(self, settings: HealthSettings) -> None:
        async with self.lock:
            try:
                # Charge the budget before acting: a crash halfway through can
                # never hand out a free extra reload.
                self.reloads = [*self.recent_reloads(), self._wall()]
                # The proofs that led here are used up: the reloaded entry
                # must prove a fault twice again before another reload.
                self.clear_streak()
                await self._async_save()
                targets = self._current_targets()
                LOGGER.warning("Casambi MQTT consumer is stale; reloading this entry")
                if not await self.hass.config_entries.async_reload(self.entry_id):
                    await self._async_latch("reload_failed")
                    return
                if await self._async_verify(targets, settings):
                    await self._async_succeeded()
                    return
                self.last_result = "verify_failed"
                if len(self.recent_reloads()) >= MAX_RELOADS_PER_WINDOW:
                    await self._async_latch("verify_failed")
                else:
                    await self._async_save()
                    LOGGER.warning("Casambi MQTT entry reload did not verify healthy")
            except Exception:  # noqa: BLE001  # Any failure must latch, never loop.
                LOGGER.exception("Casambi MQTT consumer recovery failed")
                await self._async_latch("reload_error")
            finally:
                self.in_progress = False
                self.task = None
                self._notify()

    def _entry(self) -> ConfigEntry | None:
        return self.hass.config_entries.async_get_entry(self.entry_id)

    def _current_targets(self) -> list[str]:
        """Light entity IDs this entry represents right now (never a fixed list)."""
        entry = self._entry()
        runtime = getattr(entry, "runtime_data", None)
        lights = getattr(runtime, "lights", {}) or {}
        # A user-disabled light is never added (hass stays None) and never
        # has a state, so it can be neither a target nor a blocker.
        return sorted(
            light.entity_id
            for light in lights.values()
            if getattr(light, "entity_id", None)
            and getattr(light, "hass", None) is not None
        )

    async def _async_verify(self, targets: list[str], settings: HealthSettings) -> bool:
        """
        Require real recovery, not a reload return value.

        The entry must be loaded, a NEW consumer probe must succeed on the
        reloaded entry, and every previously present light entity must exist
        again with a state other than ``unavailable``.
        """
        # Never shorter than the reloaded entry's own startup grace plus one
        # probe, or every reload could fail verification by construction.
        deadline = self._monotonic() + max(
            settings.recovery_verify_seconds,
            settings.startup_grace_seconds + settings.probe_deadline_seconds + 60,
        )
        last_probe_at: float | None = None
        while self._monotonic() < deadline:
            entry = self._entry()
            runtime = getattr(entry, "runtime_data", None)
            coordinator: HealthCoordinator | None = getattr(
                runtime, "health_coordinator", None
            )
            if (
                entry is not None
                and entry.state is ConfigEntryState.LOADED
                and coordinator is not None
            ):
                observer = coordinator.observer
                if ProbeKind.CONSUMER in observer.last_ok and self._targets_available(
                    targets
                ):
                    return True
                now = self._monotonic()
                if (
                    observer.probes.pending is None
                    and observer.bridge_state() is BridgeState.HEALTHY
                    and observer.consumer_ready()
                    and (
                        last_probe_at is None
                        or now - last_probe_at >= VERIFY_PROBE_SPACING_SECONDS
                    )
                ):
                    last_probe_at = now
                    coordinator.request_probe(ProbeKind.CONSUMER)
            await self._sleep(VERIFY_POLL_SECONDS)
        return False

    def _targets_available(self, targets: list[str]) -> bool:
        for entity_id in targets:
            state = self.hass.states.get(entity_id)
            if state is None or state.state == STATE_UNAVAILABLE:
                return False
        return True

    async def _async_succeeded(self) -> None:
        self.clear_streak()
        self.last_result = "recovered"
        self.last_recovery = dt_util.utcnow().isoformat(timespec="seconds")
        await self._async_save()
        LOGGER.warning("Casambi MQTT consumer recovered after an entry reload")
        self._announce_recovery()

    @callback
    def _announce_recovery(self) -> None:
        persistent_notification.async_create(
            self.hass,
            "Casambi MQTT entity updates are being processed again after an "
            "automatic reload of the integration entry.",
            title="Casambi MQTT recovered",
            notification_id=f"{DOMAIN}_{self.entry_id}_recovered",
        )

    async def async_mark_healthy(self) -> None:
        """End a stale streak and the latch after a verified healthy probe."""
        self.clear_streak()
        if not self.failed:
            return
        self.failed = False
        self.reason = "none"
        self.last_result = "recovered"
        self.last_recovery = dt_util.utcnow().isoformat(timespec="seconds")
        await self._async_save()
        ir.async_delete_issue(self.hass, DOMAIN, recovery_issue_id(self.entry_id))
        self._announce_recovery()
        self._notify()

    async def async_acknowledge(self) -> None:
        """Operator acknowledgement from the repair flow: allow recovery again."""
        self.failed = False
        self.reason = "none"
        self.last_result = "acknowledged"
        self.clear_streak()
        await self._async_save()
        ir.async_delete_issue(self.hass, DOMAIN, recovery_issue_id(self.entry_id))
        self._notify()

    async def async_remove(self) -> None:
        if self.task is not None:
            self.task.cancel()
        await self._store.async_remove()
        ir.async_delete_issue(self.hass, DOMAIN, recovery_issue_id(self.entry_id))
        ir.async_delete_issue(self.hass, DOMAIN, unsupported_issue_id(self.entry_id))


async def async_get_recovery_manager(
    hass: HomeAssistant, entry_id: str
) -> RecoveryManager:
    managers: dict[str, RecoveryManager] = hass.data.setdefault(DOMAIN, {}).setdefault(
        DATA_RECOVERY, {}
    )
    manager = managers.get(entry_id)
    if manager is None:
        manager = RecoveryManager(hass, entry_id)
        await manager.async_load()
        managers[entry_id] = manager
    return manager


def get_recovery_manager(hass: HomeAssistant, entry_id: str) -> RecoveryManager | None:
    return hass.data.get(DOMAIN, {}).get(DATA_RECOVERY, {}).get(entry_id)


async def async_remove_recovery_manager(hass: HomeAssistant, entry_id: str) -> None:
    manager = hass.data.get(DOMAIN, {}).get(DATA_RECOVERY, {}).pop(entry_id, None)
    if manager is None:
        manager = RecoveryManager(hass, entry_id)
    await manager.async_remove()


class HealthCoordinator:
    """One entry's state machine on top of its observer and recovery manager."""

    def __init__(  # noqa: PLR0913
        self,
        hass: HomeAssistant,
        entry_id: str,
        observer: HealthObserver,
        manager: RecoveryManager,
        settings: HealthSettings,
        request_probe: Callable[[ProbeKind], None],
    ) -> None:
        self.hass = hass
        self.entry_id = entry_id
        self.observer = observer
        self.manager = manager
        self.settings = settings
        self._request_probe = request_probe
        self.listeners: list[Callable[[], None]] = []
        self.last_consumer_ok: str | None = None
        self.state = ConsumerState.STARTING
        self.reason = "startup"
        # None: the first update always syncs the issue, so a reload after a
        # bridge upgrade removes a warning left by the previous setup.
        self._unsupported_issue: bool | None = None
        self.last_consumer: ProbeResult | None = None
        observer.listeners.append(self._on_probe_result)
        manager.listeners.append(self.async_update)

    @callback
    def async_shutdown(self) -> None:
        with_suppressed = self.manager.listeners
        if self.async_update in with_suppressed:
            with_suppressed.remove(self.async_update)

    def request_probe(self, kind: ProbeKind) -> None:
        self._request_probe(kind)

    def _compute(self) -> tuple[ConsumerState, str]:  # noqa: PLR0911
        if self.manager.in_progress:
            return ConsumerState.RECOVERING, "reloading_entry"
        if self.manager.failed:
            return ConsumerState.RECOVERY_FAILED, self.manager.reason
        bridge = self.observer.bridge_state()
        if bridge is BridgeState.STARTING:
            return ConsumerState.STARTING, "startup"
        if bridge is BridgeState.UNSUPPORTED:
            return ConsumerState.UNSUPPORTED, "bridge_without_health_protocol"
        if bridge is BridgeState.UNKNOWN:
            return ConsumerState.STARTING, "waiting_for_bridge_status"
        if bridge in {BridgeState.UNAVAILABLE, BridgeState.DEGRADED}:
            status = self.observer.bridge.status
            return ConsumerState.BRIDGE_UNAVAILABLE, (
                bridge.value
                if status is None or status.error == "none"
                else status.error
            )
        result = self.last_consumer
        if result is None:
            return ConsumerState.STARTING, "waiting_for_consumer_probe"
        outcome = result.outcome
        if outcome is ProbeOutcome.OK:
            return ConsumerState.HEALTHY, outcome.value
        if outcome is ProbeOutcome.CONSUMER_STALE:
            return ConsumerState.CONSUMER_STALE, outcome.value
        if outcome in {
            ProbeOutcome.NO_REPLY,
            ProbeOutcome.BRIDGE_FAILED,
            ProbeOutcome.DELIVERY_INCOMPLETE,
        }:
            return ConsumerState.BRIDGE_UNAVAILABLE, outcome.value
        return ConsumerState.DEGRADED, outcome.value

    @callback
    def async_update(self) -> None:
        state, reason = self._compute()
        self._update_unsupported_issue(state)
        if (state, reason) == (self.state, self.reason):
            return
        self.state, self.reason = state, reason
        for listener in list(self.listeners):
            listener()

    @callback
    def _update_unsupported_issue(self, state: ConsumerState) -> None:
        unsupported = state is ConsumerState.UNSUPPORTED
        if unsupported is self._unsupported_issue:
            return
        self._unsupported_issue = unsupported
        if unsupported:
            ir.async_create_issue(
                self.hass,
                DOMAIN,
                unsupported_issue_id(self.entry_id),
                is_fixable=False,
                is_persistent=False,
                severity=ir.IssueSeverity.WARNING,
                translation_key="bridge_unsupported",
            )
        else:
            ir.async_delete_issue(
                self.hass, DOMAIN, unsupported_issue_id(self.entry_id)
            )

    @callback
    def _on_probe_result(self, result: ProbeResult) -> None:
        if result.kind is ProbeKind.CONSUMER:
            if result.outcome is not ProbeOutcome.BRIDGE_BUSY:
                # "busy" says nothing about the consumer: keep the last verdict.
                self.last_consumer = result
            if result.outcome is ProbeOutcome.OK:
                self.last_consumer_ok = dt_util.utcnow().isoformat(timespec="seconds")
                if self.manager.stale_streak or self.manager.failed:
                    self.hass.async_create_task(self.manager.async_mark_healthy())
            elif result.outcome is ProbeOutcome.CONSUMER_STALE:
                self._on_consumer_stale()
            elif result.outcome is not ProbeOutcome.BRIDGE_BUSY:
                # Anything else is not a correlated proof of a stuck consumer
                # (missing delivery, partial handling, no units): it never
                # counts towards a reload and breaks a stale streak.
                self.manager.clear_streak()
        self.async_update()

    @callback
    def _on_consumer_stale(self) -> None:
        if not self.settings.recovery_enabled:
            return
        if self.observer.bridge_state() is not BridgeState.HEALTHY:
            # Only a working broker/bridge path can blame the consumer.
            self.manager.clear_streak()
            return
        confirm = self.settings.stale_confirm_seconds
        max_gap = (
            3 * confirm
            + self.settings.probe_deadline_seconds
            + self.settings.consumer_probe_interval_seconds
        )
        if self.manager.record_stale(confirm, max_gap):
            self.manager.request_recovery(self.settings)
            return
        # First proof: re-check with a fresh non-actuating sync, no sooner
        # than the confirmation interval, before anything is reloaded.
        self.observer.schedule(ProbeKind.CONSUMER, self.settings.stale_confirm_seconds)

    def diagnostics(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "reason": self.reason,
            "recovery_enabled": self.settings.recovery_enabled,
            "stale_streak": self.manager.stale_streak,
            "recovery_in_progress": self.manager.in_progress,
            "recovery_failed": self.manager.failed,
            "reloads_last_30_min": len(self.manager.recent_reloads()),
            "last_recovery_result": self.manager.last_result,
            "last_recovery": self.manager.last_recovery,
            "last_consumer_ok": self.last_consumer_ok,
        }
