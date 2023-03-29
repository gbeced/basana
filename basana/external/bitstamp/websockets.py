# Basana
#
# Copyright 2022-2023 Gabriel Martin Becedillas Ruiz
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

from typing import cast, Any, Awaitable, Callable, List, Optional
import asyncio
import json
import logging

import aiohttp

from basana.core import logs, websockets as core_ws
from basana.core.config import get_config_value
from basana.external.bitstamp import client, config


logger = logging.getLogger(__name__)

CHANNEL_EVENTS = {"trade", "data", "order_created", "order_changed", "order_deleted"}


class WebSocketClient(core_ws.WebSocketClient):
    def __init__(self, session: Optional[aiohttp.ClientSession] = None, config_overrides: dict = {}):
        super().__init__(
            get_config_value(config.DEFAULTS, "api.websockets.base_url", overrides=config_overrides),
            session=session, config_overrides=config_overrides,
            heartbeat=get_config_value(config.DEFAULTS, "api.websockets.heartbeat", overrides=config_overrides)
        )

    async def handle_message(self, message: dict) -> bool:
        coro = None
        event = message.get("event", "")

        # Is it an admin, non-channel, message ?
        message_handler = cast(
            Optional[Callable[[dict], Awaitable[Any]]],
            {
                "bts:request_reconnect": self._on_bts_request_reconnect,
                "bts:subscription_succeeded": self._on_bts_subscription_succeeded,
                "bts:error": self.on_error,
                "bts:subscription_failed": self.on_error,
            }.get(event)
        )
        if message_handler:
            coro = message_handler(message)
        # Is it a channel message ?
        elif event in CHANNEL_EVENTS \
                and (channel := message.get("channel")) \
                and (event_source := self.get_channel_event_source(channel)):
            coro = event_source.push_from_message(message)

        ret = False
        if coro:
            await coro
            ret = True
        return ret

    async def _on_bts_request_reconnect(self, message: dict):
        logger.debug(logs.StructuredMessage("Reconnection requested", message=message))
        self.schedule_reconnection()

    async def _on_bts_subscription_succeeded(self, message: dict):
        logger.debug(logs.StructuredMessage("Subscription suceeded", message=message))


class PublicWebSocketClient(WebSocketClient):
    def __init__(self, session: Optional[aiohttp.ClientSession] = None, config_overrides: dict = {}):
        super().__init__(session=session, config_overrides=config_overrides)

    async def subscribe_to_channels(self, channels: List[str], ws_cli: aiohttp.ClientWebSocketResponse):
        logger.debug(logs.StructuredMessage("Subscribing", src=self, channels=channels))
        await asyncio.gather(*[
            ws_cli.send_str(json.dumps({
                "event": "bts:subscribe",
                "data": {
                    "channel": channel
                }
            }))
            for channel in channels
        ])


class PrivateWebSocketClient(WebSocketClient):
    def __init__(
            self, api_key: str, api_secret: str, session: Optional[aiohttp.ClientSession] = None,
            config_overrides: dict = {}
    ):
        super().__init__(session=session, config_overrides=config_overrides)
        self._client = client.APIClient(api_key, api_secret, session=session, config_overrides=config_overrides)

    async def subscribe_to_channels(self, channels: List[str], ws_cli: aiohttp.ClientWebSocketResponse):
        logger.debug(logs.StructuredMessage("Subscribing", src=self, channels=channels))
        logger.debug(logs.StructuredMessage("Authenticating", src=self))
        websockets_token = await self._client.get_websocket_auth_token()

        await asyncio.gather(*[
            ws_cli.send_str(json.dumps({
                "event": "bts:subscribe",
                "data": {
                    "auth": websockets_token["token"],
                    "channel": "{}-{}".format(channel, websockets_token["user_id"])
                }
            }))
            for channel in channels
        ])
