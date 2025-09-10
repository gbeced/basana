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

from dataclasses import astuple
from decimal import Decimal
import asyncio
import datetime
import json

import websockets

from .helpers import load_json
from basana.core import pair
from basana.external.binance import exchange


def test_websocket_ok(realtime_dispatcher):
    p = pair.Pair("BTC", "USDT")
    order_book_diff = None

    async def on_order_book_diff(event):
        nonlocal order_book_diff

        order_book_diff = event.order_book_diff
        realtime_dispatcher.stop()

    async def server_main(websocket):
        message = json.loads(await websocket.recv())
        assert message["method"] == "SUBSCRIBE"
        await websocket.send(json.dumps({"result": None, "id": message["id"]}))

        order_book_diff = load_json("binance_depth_update.json")
        while websocket.state == websockets.protocol.State.OPEN:
            await websocket.send(json.dumps({"stream": "btcusdt@depth@1000ms", "data": order_book_diff}))
            await asyncio.sleep(0.1)

    async def test_main():
        async with websockets.serve(server_main, "127.0.0.1", 0) as server:
            ws_uri = "ws://{}:{}/".format(*server.sockets[0].getsockname())
            config_overrides = {"api": {"websockets": {"base_url": ws_uri}}}
            e = exchange.Exchange(realtime_dispatcher, config_overrides=config_overrides)
            e.subscribe_to_order_book_diff_events(p, on_order_book_diff)

            await realtime_dispatcher.run()

        assert order_book_diff is not None
        assert order_book_diff.pair == p
        assert order_book_diff.first_update_id == 76043874804
        assert order_book_diff.final_update_id == 76043875762
        assert order_book_diff.datetime == datetime.datetime(
            2025, 9, 10, 1, 15, 56, 14000, tzinfo=datetime.timezone.utc
        )
        assert astuple(order_book_diff.bids[0]) == (Decimal("111179.99000000"), Decimal("17.88084000"))
        assert astuple(order_book_diff.asks[0]) == (Decimal("111178.52000000"), Decimal("0.00000000"))

    asyncio.run(asyncio.wait_for(test_main(), 5))
