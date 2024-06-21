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

from typing import List
import asyncio
import json

import aiohttp
import websockets

from basana.core import dt, event
import basana.core.websockets as core_ws


class Event(event.Event):
    def __init__(self, message: dict):
        super().__init__(dt.utc_now())
        self.message = message


class ChannelEventSource(core_ws.ChannelEventSource):
    async def push_from_message(self, message: dict):
        self.push(Event(message))


class WebSocketClient(core_ws.WebSocketClient):
    def __init__(self, url):
        super().__init__(url)

    async def subscribe_to_channels(self, channels: List[str], ws_cli: aiohttp.ClientWebSocketResponse):
        for channel in channels:
            await ws_cli.send_str(json.dumps({"request": "subscribe", "channel": channel}))

    async def handle_message(self, message: dict) -> bool:
        ret = False
        if (channel := message.get("channel")) and (event_source := self.get_channel_event_source(channel)):
            await event_source.push_from_message(message)
            ret = True
        return ret


def test_add_channels(realtime_dispatcher):
    event_source = None
    ws = None
    subscriptions = 0

    async def server_main(ws_server):
        while True:
            message = json.loads(await ws_server.recv())
            if message["request"] == "subscribe":
                await ws_server.send(json.dumps({"response": "subscription_ok", "channel": message["channel"]}))

    async def on_event(event: Event):
        nonlocal subscriptions

        message = event.message
        if message["response"] == "subscription_ok":
            subscriptions += 1
            if message["channel"] == "channel1":
                ws.set_channel_event_source("channel2", event_source)
            elif message["channel"] == "channel2":
                realtime_dispatcher.stop()

    async def test_main():
        nonlocal event_source
        nonlocal ws

        async with websockets.serve(server_main, "127.0.0.1", 0) as server:
            ws_uri = "ws://{}:{}/".format(*server.sockets[0].getsockname())
            ws = WebSocketClient(ws_uri)
            event_source = ChannelEventSource(producer=ws)
            ws.set_channel_event_source("channel1", event_source)
            realtime_dispatcher.subscribe(event_source, on_event)

            await realtime_dispatcher.run()
            assert subscriptions == 2

    asyncio.run(asyncio.wait_for(test_main(), 5))


def test_schedule_reconnection(realtime_dispatcher):
    event_source = None
    subscriptions = 0
    ws = None

    async def server_main(ws_server):
        try:
            while True:
                message = json.loads(await ws_server.recv())
                if message["request"] == "subscribe":
                    await ws_server.send(json.dumps({"response": "subscription_ok", "channel": message["channel"]}))
        except websockets.exceptions.ConnectionClosedOK:
            pass

    async def on_event(event: Event):
        nonlocal subscriptions

        message = event.message
        if message["response"] == "subscription_ok":
            subscriptions += 1

        if subscriptions == 2:
            ws.schedule_reconnection()
        elif subscriptions == 4:
            realtime_dispatcher.stop()

    async def test_main():
        nonlocal event_source
        nonlocal ws

        async with websockets.serve(server_main, "127.0.0.1", 0) as server:
            ws_uri = "ws://{}:{}/".format(*server.sockets[0].getsockname())

            ws = WebSocketClient(ws_uri)
            event_source = ChannelEventSource(producer=ws)
            for channel in ["channel1", "channel2"]:
                ws.set_channel_event_source(channel, event_source)
            realtime_dispatcher.subscribe(event_source, on_event)

            await realtime_dispatcher.run()
            assert subscriptions == 4

    asyncio.run(asyncio.wait_for(test_main(), 5))
