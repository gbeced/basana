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

from typing import cast, Dict, List, Optional
from urllib.parse import urljoin
import abc
import asyncio
import json
import logging
import time

import aiohttp

from . import client, config, helpers as binance_helpers
from basana.core import logs, websockets as core_ws
from basana.core.config import get_config_value
import basana as bs


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


class PublicChannel(Channel):
    def __init__(self, name: str):
        self._name = name

    @property
    def alias(self) -> str:
        return self._name

    @property
    def stream(self) -> str:
        return self._name


class SpotUserDataChannel(Channel):
    def __init__(self):
        self._listen_key = None

    @property
    def alias(self) -> str:
        return "spot_user_data"

    @property
    def stream(self) -> str:
        assert self._listen_key, "resolve_name not called"
        return self._listen_key

    async def resolve_stream_name(self, api_client: client.APIClient):
        self._listen_key = (await api_client.spot_account.create_listen_key())["listenKey"]


class CrossMarginUserDataChannel(Channel):
    def __init__(self):
        self._listen_key = None

    @property
    def alias(self) -> str:
        return "cross_margin_user_data"

    @property
    def stream(self) -> str:
        assert self._listen_key, "resolve_name not called"
        return self._listen_key

    async def resolve_stream_name(self, api_client: client.APIClient):
        self._listen_key = (await api_client.cross_margin_account.create_listen_key())["listenKey"]


class IsolatedMarginUserDataChannel(Channel):
    def __init__(self, pair: bs.Pair):
        self._pair = pair
        self._listen_key = None

    @property
    def alias(self) -> str:
        symbol = binance_helpers.pair_to_order_book_symbol(self._pair)
        return f"isolated_margin_user_data_{symbol}"

    @property
    def stream(self) -> str:
        assert self._listen_key, "resolve_name not called"
        return self._listen_key

    async def resolve_stream_name(self, api_client: client.APIClient):
        symbol = binance_helpers.pair_to_order_book_symbol(self._pair)
        self._listen_key = (await api_client.isolated_margin_account.create_listen_key(symbol))["listenKey"]


class WebSocketClient(core_ws.WebSocketClient):
    def __init__(
            self, api_client: client.APIClient, session: Optional[aiohttp.ClientSession] = None,
            config_overrides: dict = {}
    ):
        url = urljoin(
            get_config_value(config.DEFAULTS, "api.websockets.base_url", overrides=config_overrides),
            "/stream"
        )
        super().__init__(
            url, session=session, config_overrides=config_overrides,
            heartbeat=get_config_value(config.DEFAULTS, "api.websockets.heartbeat", overrides=config_overrides)
        )
        self._next_msg_id = int(time.time() * 1000)
        self._cli = api_client

        self._alias_to_channel: Dict[str, Channel] = {}
        self._stream_to_channel: Dict[str, Channel] = {}

    def set_channel_event_source_ex(self, channel: Channel, event_source: core_ws.ChannelEventSource):
        assert channel.alias not in self._alias_to_channel, "channel already registered"
        super().set_channel_event_source(channel.alias, event_source)
        self._alias_to_channel[channel.alias] = channel

    def get_channel_event_source_ex(self, channel: Channel) -> Optional[core_ws.ChannelEventSource]:
        return super().get_channel_event_source(channel.alias)

    async def subscribe_to_channels(self, channel_aliases: List[str], ws_cli: aiohttp.ClientWebSocketResponse):
        logger.debug(logs.StructuredMessage("Subscribing", src=self, channels=channel_aliases))

        channels = [self._alias_to_channel[alias] for alias in channel_aliases]
        # Give the chance for dynamic channels to resolved the subscribe name.
        channels = cast(List[Channel], channels)
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

    async def handle_message(self, message: dict) -> bool:
        coro = None

        # A response to a message we sent.
        if {"result", "id"} <= set(message.keys()):
            coro = self._on_response(message)
        # A message associated to a channel.
        elif stream := message.get("stream"):
            channel = self._stream_to_channel.get(stream)
            assert channel, f"{stream} could not be mapped to a channel instance"
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
