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

from typing import Any, Dict, List, Optional, Set
import abc
import asyncio
import json
import logging
import time

import aiohttp

from . import event, helpers, logs


logger = logging.getLogger(__name__)


# Base class for event sources that generate events from websocket messages.
class ChannelEventSource(event.FifoQueueEventSource):
    def __init__(self, producer: event.Producer):
        super().__init__(producer=producer)

    @abc.abstractmethod
    async def push_from_message(self, message: dict):
        raise NotImplementedError()


class WebSocketClient(event.Producer, metaclass=abc.ABCMeta):
    """"Base class for channel based web socket clients."""
    def __init__(
            self, url: str, session: Optional[aiohttp.ClientSession] = None, config_overrides: dict = {},
            heartbeat: float = 30
    ):
        super().__init__()
        self._url = url
        self._session = session
        self._config_overrides = config_overrides
        self._event_sources: Dict[str, ChannelEventSource] = {}
        self._reconnect_request = asyncio.Event()
        self._subscribe_request = asyncio.Event()
        self._pending_subscriptions: Set[str] = set()
        self.backoff_secs = 1
        self._run_called = False
        self._heartbeat = heartbeat

    def set_channel_event_source(self, channel: str, event_source: ChannelEventSource):
        assert channel not in self._event_sources, "channel already registered"
        self._event_sources[channel] = event_source
        self._pending_subscriptions.add(channel)
        self._subscribe_request.set()

    def get_channel_event_source(self, channel: str) -> Optional[ChannelEventSource]:
        return self._event_sources.get(channel)

    def schedule_reconnection(self):
        self._reconnect_request.set()

    async def on_error(self, error: Any):
        logger.error(logs.StructuredMessage("Error", src=self, error=error))

    async def on_unknown_message(self, message: aiohttp.WSMessage):
        logger.warning(logs.StructuredMessage("Unknown message", src=self, type=message.type, data=message.data))

    async def main(self):
        assert not self._run_called, "run already called"

        self._run_called = True
        last_connect_ts = 0
        while True:
            # Backoff, if necessary, before connecting.
            prev_attemp_age = time.time() - last_connect_ts
            if prev_attemp_age < self.backoff_secs:
                await asyncio.sleep(self.backoff_secs - prev_attemp_age)

            try:
                logger.debug(logs.StructuredMessage("Connecting websocket", src=self, url=self._url))
                last_connect_ts = time.time()
                async with helpers.use_or_create_session(session=self._session) as session, \
                        session.ws_connect(self._url, heartbeat=self._heartbeat) as ws_cli, \
                        helpers.TaskGroup() as tg:

                    # Turn this off since we just reconnected.
                    self._reconnect_request.clear()

                    # Connect to all channels.
                    self._pending_subscriptions.update(self._event_sources.keys())
                    self._subscribe_request.set()

                    # Run tasks.
                    tg.create_task(self._msg_loop(ws_cli))
                    tg.create_task(self._subscribe_loop(ws_cli))
                    tg.create_task(self._reconnect(ws_cli))
            except Exception as e:
                await self.on_error(e)

    @abc.abstractmethod
    async def subscribe_to_channels(
            self, channels: List[str], ws_cli: aiohttp.ClientWebSocketResponse
    ):
        raise NotImplementedError()

    @abc.abstractmethod
    async def handle_message(self, message: dict) -> bool:
        raise NotImplementedError()

    async def _msg_loop(self, ws_cli: aiohttp.ClientWebSocketResponse):
        logger.debug(logs.StructuredMessage("Running message loop", src=self))

        # The iterator exits normally when the connection is closed with close code 1000 (OK) or 1001 (going away).
        # It raises a ConnectionClosedError when the connection is closed with any other code.
        async for message in ws_cli:
            handled = False
            if message.type == aiohttp.WSMsgType.TEXT:
                json_msg = json.loads(message.data)
                handled = await self.handle_message(json_msg)
            if not handled:
                await self.on_unknown_message(message)

        # If the message loop finished we need to gracefully stop the other tasks.
        self._subscribe_request.set()
        self._reconnect_request.set()

    async def _reconnect(self, ws_cli: aiohttp.ClientWebSocketResponse):
        # Will exit when reconnection is requested or when its canceled.
        await self._reconnect_request.wait()
        self._reconnect_request.clear()
        if not ws_cli.closed:
            await ws_cli.close()

    async def _subscribe_loop(self, ws_cli: aiohttp.ClientWebSocketResponse):
        while not ws_cli.closed:
            await self._subscribe_request.wait()
            self._subscribe_request.clear()
            if not ws_cli.closed and self._pending_subscriptions:
                channels = list(self._pending_subscriptions)
                self._pending_subscriptions = set()
                await self.subscribe_to_channels(channels, ws_cli)
