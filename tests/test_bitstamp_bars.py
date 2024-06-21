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
from basana.external.bitstamp import exchange


def test_bars_from_trades(realtime_dispatcher):
    p = pair.Pair("BTC", "USD")
    last_bar = None

    async def on_bar_event(bar_event):
        nonlocal last_bar

        last_bar = bar_event.bar
        realtime_dispatcher.stop()

    async def server_main(websocket):
        # We expect to receive a subscription request to start.
        message = json.loads(await websocket.recv())
        if message.get("event") == "bts:subscribe":
            channel = message["data"]["channel"]
            await websocket.send(json.dumps({"event": "bts:subscription_succeeded"}))
            # Keep on sending trade events while the connection is open.
            while websocket.open:
                timestamp = time.time()
                await websocket.send(json.dumps({
                    "event": "trade",
                    "channel": channel,
                    "data": {
                        "id": 246612672,
                        "timestamp": str(int(timestamp)),
                        "amount": 1,
                        "amount_str": "1",
                        "price": 1000,
                        "price_str": "1000",
                        "type": 0,
                        "microtimestamp": str(int(timestamp * 1e6)),
                        "buy_order_id": 1530834271539201,
                        "sell_order_id": 1530834150440960
                    }
                }))
                await asyncio.sleep(0.4)

    async def test_main():
        async with websockets.serve(server_main, "127.0.0.1", 0) as server:
            ws_uri = "ws://{}:{}/".format(*server.sockets[0].getsockname())
            e = exchange.Exchange(
                realtime_dispatcher, "key", "secret",
                config_overrides={
                    "api": {
                        "http": {"base_url": "http://bitstamp.mock/"},
                        "websockets": {"base_url": ws_uri}
                    }
                }
            )
            e.subscribe_to_bar_events(p, 1, on_bar_event)

            await realtime_dispatcher.run()

    asyncio.run(asyncio.wait_for(test_main(), 5))

    assert last_bar is not None
    assert last_bar.pair == p
    assert last_bar.datetime is not None
    assert last_bar.open == Decimal(1000)
    assert last_bar.high == Decimal(1000)
    assert last_bar.low == Decimal(1000)
    assert last_bar.close == Decimal(1000)
    assert last_bar.volume >= Decimal(2) and last_bar.volume <= Decimal(3)
