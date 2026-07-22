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

from . import client, config, helpers
from basana.core import dispatcher, logs, websockets as core_ws
from basana.core.config import get_config_value


logger = logging.getLogger(__name__)


class Channel(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def stream(self) -> str:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def websocket_protocol(self) -> str:
        raise NotImplementedError()


class WSStreamChannel(Channel):
    @property
    def websocket_protocol(self) -> str:
        return "ws_stream"

    @property
    @abc.abstractmethod
    def alias(self) -> str:
        raise NotImplementedError()

    async def resolve_stream_name(self, api_client: client.APIClient):
        pass

    def keep_alive_period(self, config_overrides: dict = {}) -> Optional[datetime.timedelta]:
        return None

    async def keep_alive(self, api_client: client.APIClient):  # pragma: no cover
        pass


class WSAPIChannel(Channel):
    def __init__(self, stream: str):
        self._stream = stream

    @property
    def stream(self) -> str:
        return self._stream

    @property
    def websocket_protocol(self) -> str:
        return "ws_api"


class PublicStreamChannel(WSStreamChannel):
    def __init__(self, name: str):
        self._name = name

    @property
    def alias(self) -> str:
        return self._name

    @property
    def stream(self) -> str:
        return self._name


class WebSocketStreamClient(core_ws.WebSocketClient):
    def __init__(
            self, dispatcher: dispatcher.EventDispatcher, api_client: client.APIClient,
            session: Optional[aiohttp.ClientSession] = None, config_overrides: dict = {}
    ):
        url = urljoin(
            get_config_value(config.DEFAULTS, "api.ws_stream.base_url", overrides=config_overrides),
            "/stream"
        )
        super().__init__(
            url, session=session, config_overrides=config_overrides,
            heartbeat=get_config_value(config.DEFAULTS, "api.ws_stream.heartbeat", overrides=config_overrides)
        )
        self._dispatcher = dispatcher
        self._cli = api_client
        self._alias_to_channel: Dict[str, WSStreamChannel] = {}
        self._stream_to_channel: Dict[str, WSStreamChannel] = {}
        self._next_keep_alive: Dict[str, datetime.datetime] = {}
        self._next_msg_id = int(time.time() * 1000)

    def set_channel_event_source_ex(self, channel: WSStreamChannel, event_source: core_ws.ChannelEventSource):
        assert channel.alias not in self._alias_to_channel, "channel already registered"
        super().set_channel_event_source(channel.alias, event_source)
        self._alias_to_channel[channel.alias] = channel

    def get_channel_event_source_ex(self, channel: WSStreamChannel) -> Optional[core_ws.ChannelEventSource]:
        return super().get_channel_event_source(channel.alias)

    async def subscribe_to_channels(self, channel_aliases: List[str], ws_cli: aiohttp.ClientWebSocketResponse):
        logger.debug(logs.StructuredMessage("Subscribing", src=self, channels=channel_aliases))

        # Give a chance for dynamic channels to resolve the stream name.
        channels: List[WSStreamChannel] = [self._alias_to_channel[alias] for alias in channel_aliases]
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
        # An event
        elif {"stream", "data"} <= set(message.keys()):
            stream = message["stream"]
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
                coro = event_source.push_from_message(message["data"])

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

    def _keep_alive_channel(self, channel: WSStreamChannel) -> dispatcher.SchedulerJob:
        async def scheduler_job():
            if self._next_keep_alive[channel.alias] <= self._dispatcher.now():
                logger.debug(logs.StructuredMessage("Channel keep alive", alias=channel.alias))
                try:
                    await channel.keep_alive(self._cli)
                finally:
                    self._schedule_keep_alive(channel)
        return scheduler_job

    def _schedule_keep_alive(self, channel: WSStreamChannel):
        period = channel.keep_alive_period(self._config_overrides)
        if period:
            schedule_dt = self._dispatcher.now() + period
            logger.debug(logs.StructuredMessage("Scheduling keep alive", when=schedule_dt, alias=channel.alias))
            self._next_keep_alive[channel.alias] = schedule_dt
            self._dispatcher.schedule(schedule_dt, self._keep_alive_channel(channel))


class WebSocketAPIClient(core_ws.WebSocketClient):
    def __init__(
            self, dispatcher: dispatcher.EventDispatcher, api_client: client.APIClient,
            session: Optional[aiohttp.ClientSession] = None, config_overrides: dict = {}
    ):
        url = urljoin(
            get_config_value(config.DEFAULTS, "api.ws_api.base_url", overrides=config_overrides),
            "/ws-api/v3"
        )
        super().__init__(
            url,
            session=session, config_overrides=config_overrides,
            heartbeat=get_config_value(config.DEFAULTS, "api.ws_api.heartbeat", overrides=config_overrides)
        )
        self._dispatcher = dispatcher
        self._cli = api_client
        self._stream_to_channel: Dict[str, WSAPIChannel] = {}
        self._msg_id_to_channel: Dict[int, str] = {}
        self._subscription_id_to_channel: Dict[int, WSAPIChannel] = {}
        self._next_msg_id = int(time.time() * 1000)

    def set_channel_event_source_ex(self, channel: WSAPIChannel, event_source: core_ws.ChannelEventSource):
        assert channel.stream not in self._stream_to_channel, "channel already registered"
        super().set_channel_event_source(channel.stream, event_source)
        self._stream_to_channel[channel.stream] = channel

    def get_channel_event_source_ex(self, channel: WSAPIChannel) -> Optional[core_ws.ChannelEventSource]:
        return super().get_channel_event_source(channel.stream)

    async def subscribe_to_channels(self, channels: List[str], ws_cli: aiohttp.ClientWebSocketResponse):
        logger.debug(logs.StructuredMessage("Subscribing", src=self, channels=channels))

        ws_api_channels: List[WSAPIChannel] = [self._stream_to_channel[alias] for alias in channels]
        for channel in ws_api_channels:
            msg_id = self._get_next_msg_id()
            self._msg_id_to_channel[msg_id] = channel.stream
            await ws_cli.send_str(json.dumps({
                "id": msg_id,
                "method": "userDataStream.subscribe.signature",
                "params": self._get_signature_subscription_params(),
            }))

    async def handle_message(self, message: dict) -> bool:
        coro = None

        # A response to a message we sent.
        if {"status", "id"} <= set(message.keys()):
            coro = self._on_response(message)
        # An event
        elif {"event", "subscriptionId"} <= set(message.keys()):
            subscription_id = message["subscriptionId"]
            channel = self._subscription_id_to_channel.get(subscription_id)
            if channel and (event_source := self.get_channel_event_source(channel.stream)):
                coro = event_source.push_from_message(message["event"])

        ret = False
        if coro:
            await coro
            ret = True
        return ret

    async def _on_response(self, message: dict):
        if message["status"] != 200:
            await self.on_error(message)
            return

        # Subscription response.
        if channel_alias := self._msg_id_to_channel.pop(message["id"], None):
            subscription_id = message["result"]["subscriptionId"]
            self._subscription_id_to_channel[subscription_id] = self._stream_to_channel[channel_alias]

    def _get_next_msg_id(self) -> int:
        ret = self._next_msg_id
        self._next_msg_id += 1
        return ret

    def _get_signature_subscription_params(self) -> dict:
        api_key, api_secret = self._cli.get_api_credentials()
        params: dict = {
            "apiKey": api_key,
            "timestamp": int(round(time.time() * 1000)),
        }
        params["signature"] = helpers.get_signature(api_secret, qs_params=params)
        return params
