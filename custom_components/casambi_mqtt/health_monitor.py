"""Home Assistant wiring for the per-entry HealthObserver."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.event import async_track_time_interval

from . import health_protocol as hp
from .const import LOGGER, MQTT_TOPIC_PREFIX
from .health import TICK_SECONDS, HealthObserver, PendingProbe, ProbeKind

if TYPE_CHECKING:
    from collections.abc import Callable

    from homeassistant.components.mqtt import ReceiveMessage
    from homeassistant.config_entries import ConfigEntry


class HealthMonitor:
    """Feed one entry's observer from MQTT and send the probes it asks for."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        network_name: str,
        observer: HealthObserver,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.observer = observer
        self.status_topic = hp.status_topic(MQTT_TOPIC_PREFIX, network_name)
        self.reply_topic = hp.reply_topic(MQTT_TOPIC_PREFIX, network_name)
        self.command_topic = f"{MQTT_TOPIC_PREFIX}/{network_name}/commands"

    async def async_subscribe(
        self,
        subscribe: Callable,
    ) -> list[Callable[[], None]]:
        """Subscribe status, reply and the raw delivery listener."""
        unsubscribers = []
        try:
            unsubscribers.append(
                await subscribe(self.hass, self.status_topic, self._on_status, 1)
            )
            unsubscribers.append(
                await subscribe(self.hass, self.reply_topic, self._on_reply, 1)
            )
            unsubscribers.append(
                await subscribe(
                    self.hass,
                    f"{self.observer.event_prefix}#",
                    self._on_raw_unit,
                    1,
                )
            )
        except Exception:
            for unsubscribe in unsubscribers:
                unsubscribe()
            raise
        unsubscribers.append(
            async_track_time_interval(
                self.hass,
                self._on_tick,
                timedelta(seconds=TICK_SECONDS),
                name="casambi_mqtt health",
                cancel_on_shutdown=True,
            )
        )
        return unsubscribers

    @callback
    def _on_status(self, msg: ReceiveMessage) -> None:
        if msg.topic == self.status_topic:
            self.observer.on_status(msg.payload, retain=bool(msg.retain))

    @callback
    def _on_reply(self, msg: ReceiveMessage) -> None:
        if msg.topic == self.reply_topic and not msg.retain:
            self.observer.on_reply(msg.payload)

    @callback
    def _on_raw_unit(self, msg: ReceiveMessage) -> None:
        self.observer.on_raw_unit(msg.topic, msg.payload)

    @callback
    def _on_tick(self, _now: object = None) -> None:
        probe = self.observer.tick()
        if probe is not None:
            self.entry.async_create_background_task(
                self.hass, self._async_send(probe), "casambi_mqtt health probe"
            )

    async def _async_send(self, probe: PendingProbe) -> None:
        payload = (
            hp.get_status_command(probe.request_id)
            if probe.kind is ProbeKind.STATUS
            else hp.snapshot_command(probe.request_id)
        )
        try:
            await mqtt.async_publish(self.hass, self.command_topic, payload, qos=1)
        except HomeAssistantError:
            # The probe then simply times out as no_reply; never retry here.
            LOGGER.debug("Casambi health probe could not be published")
