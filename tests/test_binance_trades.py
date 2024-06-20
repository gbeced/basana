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
import datetime
import json

import aioresponses
import pytest
import websockets

from basana.core import pair
from basana.external.binance import exchange


TRADE_MSG = {
    "stream": "btcusdt@trade",
    "data": {
        "e": "trade",
        "E": 1669932275175,
        "s": "BTCUSDT",
        "t": 2275696344,
        "p": "16930.90000000",
        "q": "0.05097000",
        "b": 16081955917,
        "a": 16081955890,
        "T": 1669932275174,
        "m": False,
        "M": True
    }
}


@pytest.fixture()
def binance_http_api_mock():
    with aioresponses.aioresponses() as m:
        yield m


async def ignore(*args, **kwargs):
    pass


def test_websocket_ok(realtime_dispatcher):
    p = pair.Pair("BTC", "USDT")
    last_trade = None

    async def on_trade_event(trade_event):
        nonlocal last_trade

        last_trade = trade_event.trade
        realtime_dispatcher.stop()

    async def server_main(websocket):
        message = json.loads(await websocket.recv())
        assert message["method"] == "SUBSCRIBE"
        await websocket.send(json.dumps({"result": None, "id": message["id"]}))

        while True:
            await websocket.send(json.dumps(TRADE_MSG))
            await asyncio.sleep(0.1)

    async def test_main():
        async with websockets.serve(server_main, "127.0.0.1", 0) as server:
            ws_uri = "ws://{}:{}/".format(*server.sockets[0].getsockname())
            config_overrides = {"api": {"websockets": {"base_url": ws_uri}}}
            e = exchange.Exchange(realtime_dispatcher, config_overrides=config_overrides)
            e.subscribe_to_trade_events(p, on_trade_event)

            await realtime_dispatcher.run()

    asyncio.run(asyncio.wait_for(test_main(), 5))

    assert last_trade is not None
    assert last_trade.pair == p
    assert last_trade.datetime == datetime.datetime(2022, 12, 1, 22, 4, 35, 174000, tzinfo=datetime.timezone.utc)
    assert last_trade.id == "2275696344"
    assert last_trade.buy_order_id == "16081955917"
    assert last_trade.sell_order_id == "16081955890"
    assert last_trade.price == Decimal("16930.9")
    assert last_trade.amount == Decimal("0.05097")
