# Basana
#
# Copyright 2022 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, List, Optional
from urllib.parse import urljoin
import abc
import asyncio
import datetime
import json
import logging
import time

import aiohttp

from . import client, config
from basana.core import dispatcher, logs, websockets as core_ws
from basana.core.config import get_config_value


logger = logging.getLogger(__name__)


class Channel(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def alias(self) -> str:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def stream(self) -> str:
        raise NotImplementedError()

    async def resolve_stream_name(self, api_client: client.APIClient):
        pass

    def keep_alive_period(self, config_overrides: dict = {}) -> Optional[datetime.timedelta]:
        return None

    async def keep_alive(self, api_client: client.APIClient):  # pragma: no cover
        pass


class PublicChannel(Channel):
    def __init__(self, name: str):
        self._name = name

    @property
    def alias(self) -> str:
        return self._name

    @property
    def stream(self) -> str:
        return self._name


class WebSocketClient(core_ws.WebSocketClient):
    def __init__(
            self, dispatcher: dispatcher.EventDispatcher, api_client: client.APIClient,
            session: Optional[aiohttp.ClientSession] = None, config_overrides: dict = {}
    ):
        url = urljoin(
            get_config_value(config.DEFAULTS, "api.websockets.base_url", overrides=config_overrides),
            "/stream"
        )
        super().__init__(
            url, session=session, config_overrides=config_overrides,
            heartbeat=get_config_value(config.DEFAULTS, "api.websockets.heartbeat", overrides=config_overrides)
        )
        self._dispatcher = dispatcher
        self._cli = api_client
        self._alias_to_channel: Dict[str, Channel] = {}
        self._stream_to_channel: Dict[str, Channel] = {}
        self._next_keep_alive: Dict[str, datetime.datetime] = {}
        self._next_msg_id = int(time.time() * 1000)

    def set_channel_event_source_ex(self, channel: Channel, event_source: core_ws.ChannelEventSource):
        assert channel.alias not in self._alias_to_channel, "channel already registered"
        super().set_channel_event_source(channel.alias, event_source)
        self._alias_to_channel[channel.alias] = channel

    def get_channel_event_source_ex(self, channel: Channel) -> Optional[core_ws.ChannelEventSource]:
        return super().get_channel_event_source(channel.alias)

    async def subscribe_to_channels(self, channel_aliases: List[str], ws_cli: aiohttp.ClientWebSocketResponse):
        logger.debug(logs.StructuredMessage("Subscribing", src=self, channels=channel_aliases))

        # Give a chance for dynamic channels to resolve the stream name.
        channels: List[Channel] = [self._alias_to_channel[alias] for alias in channel_aliases]
        await asyncio.gather(*[
            channel.resolve_stream_name(self._cli) for channel in channels
        ])
        self._stream_to_channel.update({
            channel.stream: channel for channel in channels
        })

        msg_id = self._get_next_msg_id()
        await ws_cli.send_str(json.dumps({
            "id": msg_id,
            "method": "SUBSCRIBE",
            "params": [channel.stream for channel in channels]
        }))

        # Schedule keep alives.
        for channel in channels:
            self._schedule_keep_alive(channel)

    async def handle_message(self, message: dict) -> bool:
        coro = None

        # A response to a message we sent.
        if {"result", "id"} <= set(message.keys()):
            coro = self._on_response(message)
        # A message associated to a channel.
        elif stream := message.get("stream"):
            channel = self._stream_to_channel.get(stream)
            assert channel, f"{stream} could not be mapped to a channel instance"
            # Resubscribe to the channel if the listen key expired.
            if message.get("data", {}).get("e") == "listenKeyExpired":
                logger.debug(logs.StructuredMessage(
                    "License key expired. Scheduling re-subscription", alias=channel.alias
                ))
                self.schedule_resubscription([channel.alias])
            # Get the event source for the channel alias.
            if event_source := self.get_channel_event_source(channel.alias):
                coro = event_source.push_from_message(message)

        ret = False
        if coro:
            await coro
            ret = True
        return ret

    async def _on_response(self, message: dict):
        if message["result"] is not None:
            await self.on_error(message)

    def _get_next_msg_id(self) -> int:
        ret = self._next_msg_id
        self._next_msg_id += 1
        return ret

    def _keep_alive_channel(self, channel: Channel) -> dispatcher.SchedulerJob:
        async def scheduler_job():
            if self._next_keep_alive[channel.alias] <= self._dispatcher.now():
                logger.debug(logs.StructuredMessage("Channel keep alive", alias=channel.alias))
                try:
                    await channel.keep_alive(self._cli)
                finally:
                    self._schedule_keep_alive(channel)
        return scheduler_job

    def _schedule_keep_alive(self, channel: Channel):
        period = channel.keep_alive_period(self._config_overrides)
        if period:
            schedule_dt = self._dispatcher.now() + period
            logger.debug(logs.StructuredMessage("Scheduling keep alive", when=schedule_dt, alias=channel.alias))
            self._next_keep_alive[channel.alias] = schedule_dt
            self._dispatcher.schedule(schedule_dt, self._keep_alive_channel(channel))
