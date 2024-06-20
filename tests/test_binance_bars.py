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

from decimal import Decimal
import asyncio
import json
import time

import websockets

from basana.core import pair
from basana.external.binance import exchange


def test_bars(realtime_dispatcher):
    p = pair.Pair("BNB", "BTC")
    last_bar = None

    async def on_bar_event(bar_event):
        nonlocal last_bar

        last_bar = bar_event.bar
        realtime_dispatcher.stop()

    async def server_main(websocket):
        message = json.loads(await websocket.recv())
        assert message["method"] == "SUBSCRIBE"
        await websocket.send(json.dumps({"result": None, "id": message["id"]}))

        kline_event = {
            "e": "kline",     # Event type
            "E": 123456789,   # Event time
            "s": "BNBBTC",    # Symbol
            "k": {
                "t": 123400000,  # Kline start time
                "T": 123460000,  # Kline close time
                "s": "BNBBTC",   # Symbol
                "i": "1m",       # Interval
                "f": 100,        # First trade ID
                "L": 200,        # Last trade ID
                "o": "0.0010",   # Open price
                "c": "0.0020",   # Close price
                "h": "0.0025",   # High price
                "l": "0.0005",   # Low price
                "v": "1000",     # Base asset volume
                "n": 100,        # Number of trades
                "x": False,      # Is this kline closed?
                "q": "1.0000",   # Quote asset volume
                "V": "500",      # Taker buy base asset volume
                "Q": "0.500",    # Taker buy quote asset volume
                "B": "123456"    # Ignore
            }
        }

        while True:
            timestamp = time.time()
            for kline_closed in (False, True):
                kline_event["E"] = int(timestamp * 1e3)
                kline_event["k"]["x"] = kline_closed
                await websocket.send(json.dumps({
                    "stream": "bnbbtc@kline_1s",
                    "data": kline_event,
                }))
                await asyncio.sleep(0.4)

    async def test_main():
        async with websockets.serve(server_main, "127.0.0.1", 0) as server:
            ws_uri = "ws://{}:{}/".format(*server.sockets[0].getsockname())
            config_overrides = {"api": {"websockets": {"base_url": ws_uri}}}
            e = exchange.Exchange(realtime_dispatcher, config_overrides=config_overrides)
            e.subscribe_to_bar_events(p, 1, on_bar_event)

            await realtime_dispatcher.run()

    asyncio.run(asyncio.wait_for(test_main(), 5))

    assert last_bar is not None
    assert last_bar.pair == p
    assert last_bar.datetime is not None
    assert last_bar.open == Decimal("0.001")
    assert last_bar.high == Decimal("0.0025")
    assert last_bar.low == Decimal("0.0005")
    assert last_bar.close == Decimal("0.002")
    assert last_bar.volume == Decimal(1000)
