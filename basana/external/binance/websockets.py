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

from typing import Optional, List
from urllib.parse import urljoin
import json
import logging
import time

import aiohttp

from . import client, config
from basana.core import helpers, logs, websockets as core_ws
from basana.core.config import get_config_value


logger = logging.getLogger(__name__)

# Authenticated streams, like USER_STREAM, don't have fixed channel names. Listen keys are used instead. We use aliases
# to simplify dealing with listen keys that have expired.
spot_user_data_stream_alias = "spot_user_data"


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
        self._listen_keys = helpers.FiFoCache(2 * 3)

    async def subscribe_to_channels(self, channels: List[str], ws_cli: aiohttp.ClientWebSocketResponse):
        logger.debug(logs.StructuredMessage("Subscribing", src=self, channels=channels))

        # Replace user_data_stream_alias with the listen key.
        if spot_user_data_stream_alias in channels:
            listen_key = (await self._cli.spot_account.create_listen_key())["listenKey"]
            self._listen_keys.add(listen_key, None)
            channels = [
                listen_key if channel == spot_user_data_stream_alias else channel
                for channel in channels
            ]

        msg_id = self._get_next_msg_id()
        await ws_cli.send_str(json.dumps({
            "id": msg_id,
            "method": "SUBSCRIBE",
            "params": channels
        }))

    async def handle_message(self, message: dict) -> bool:
        coro = None

        # A response to a message we sent.
        if {"result", "id"} <= set(message.keys()):
            coro = self._on_response(message)
        # A message associated to a channel.
        elif channel := message.get("stream"):
            # If the channel refers to a listen key, the map that to user_data_stream_alias.
            if channel in self._listen_keys:
                channel = spot_user_data_stream_alias
            if event_source := self.get_channel_event_source(channel):
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
