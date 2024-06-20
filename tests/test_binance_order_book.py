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
import logging
import re

import aiohttp
import aioresponses
import pytest
import websockets

from .helpers import wait_caplog
from basana.core import pair
from basana.external.binance import exchange, order_book


ORDER_BOOK = {
    "lastUpdateId": 27229732069,
    "bids": [
        [
            "16757.47000000",
            "0.04893000"
        ],
        [
            "16757.41000000",
            "0.00073000"
        ],
        [
            "16756.52000000",
            "0.00690000"
        ]
    ],
    "asks": [
        [
            "16758.13000000",
            "0.00682000"
        ],
        [
            "16758.55000000",
            "0.04963000"
        ],
        [
            "16759.25000000",
            "0.00685000"
        ]
    ]
}


@pytest.fixture()
def binance_http_api_mock():
    with aioresponses.aioresponses() as m:
        m.get(re.compile(r"http://binance.mock/api/v3/depth\\?.*"), status=200, payload=ORDER_BOOK)
        yield m


def test_poll_ok_with_custom_session(binance_http_api_mock, realtime_dispatcher):
    p = pair.Pair("BTC", "USDT")
    last_ob = None

    async def on_order_book_event(order_book_event):
        nonlocal last_ob

        last_ob = order_book_event.order_book
        realtime_dispatcher.stop()

    async def test_main():
        async with aiohttp.ClientSession() as session:
            realtime_dispatcher.subscribe(
                order_book.PollOrderBook(
                    p, 0.5, limit=100, session=session,
                    config_overrides={"api": {"http": {"base_url": "http://binance.mock/"}}}
                ),
                on_order_book_event
            )

            await realtime_dispatcher.run()

    asyncio.run(asyncio.wait_for(test_main(), 2))

    assert last_ob is not None
    assert last_ob.pair == p
    assert last_ob.bids[0].price == Decimal("16757.47")
    assert last_ob.bids[1].volume == Decimal("0.00073")
    assert last_ob.asks[0].price == Decimal("16758.13")
    assert last_ob.asks[1].volume == Decimal("0.04963")


@pytest.mark.parametrize("response_status, response_body", [
    (500, {}),
])
def test_unhandled_exception_during_poll(
    response_status, response_body, binance_http_api_mock, realtime_dispatcher, caplog
):
    caplog.set_level(logging.INFO)
    binance_http_api_mock.clear()
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/depth\\?.*"), status=response_status, payload=response_body
    )

    async def on_order_book_event(*args, **kwargs):
        assert False

    async def on_error(self, error):
        await order_book.PollOrderBook.on_error(self, error)
        realtime_dispatcher.stop()

    async def test_main():
        async with aiohttp.ClientSession() as session:
            poller = order_book.PollOrderBook(
                pair.Pair("BTC", "USDT"), 0.5, session=session,
                config_overrides={"api": {"http": {"base_url": "http://binance.mock/"}}}
            )
            poller.on_error = lambda error: on_error(poller, error)
            realtime_dispatcher.subscribe(poller, on_order_book_event)

            await realtime_dispatcher.run()
            assert "Error polling order book" in caplog.text

    asyncio.run(asyncio.wait_for(test_main(), 5))


def test_websocket_ok(realtime_dispatcher):
    p = pair.Pair("BTC", "USDT")
    obook_10 = None
    obook_20 = None

    async def on_order_book_10(event):
        nonlocal obook_10

        obook_10 = event.order_book
        if obook_20 is not None:
            realtime_dispatcher.stop()

    async def on_order_book_20(event):
        nonlocal obook_20

        obook_20 = event.order_book
        if obook_10 is not None:
            realtime_dispatcher.stop()

    async def server_main(websocket):
        message = json.loads(await websocket.recv())
        assert message["method"] == "SUBSCRIBE"
        await websocket.send(json.dumps({"result": None, "id": message["id"]}))

        while True:
            await websocket.send(json.dumps({"stream": "btcusdt@depth10", "data": ORDER_BOOK}))
            await websocket.send(json.dumps({"stream": "btcusdt@depth20", "data": ORDER_BOOK}))
            await asyncio.sleep(0.1)

    async def test_main():
        async with websockets.serve(server_main, "127.0.0.1", 0) as server:
            ws_uri = "ws://{}:{}/".format(*server.sockets[0].getsockname())
            config_overrides = {"api": {"websockets": {"base_url": ws_uri}}}
            e = exchange.Exchange(realtime_dispatcher, config_overrides=config_overrides)
            e.subscribe_to_order_book_events(p, on_order_book_10)
            e.subscribe_to_order_book_events(p, on_order_book_20, depth=20)

            await realtime_dispatcher.run()

        for obook in [obook_10, obook_20]:
            assert obook.pair == p
            assert obook.bids[0].price == Decimal("16757.47")
            assert obook.bids[1].volume == Decimal("0.00073")
            assert obook.asks[0].price == Decimal("16758.13")
            assert obook.asks[1].volume == Decimal("0.04963")

    asyncio.run(asyncio.wait_for(test_main(), 5))


def test_websocket_error(realtime_dispatcher, caplog):
    p = pair.Pair("BTC", "USDT")

    async def stop_on_error(timeout):
        if await wait_caplog("lalalala", caplog, timeout=timeout):
            realtime_dispatcher.stop()

    async def on_order_book_event(event):
        assert False

    async def server_main(websocket):
        message = json.loads(await websocket.recv())
        assert message["method"] == "SUBSCRIBE"
        await websocket.send(json.dumps({"result": "lalalala", "id": message["id"]}))

    async def test_main(timeout):
        async with websockets.serve(server_main, "127.0.0.1", 0) as server:
            ws_uri = "ws://{}:{}/".format(*server.sockets[0].getsockname())
            config_overrides = {"api": {"websockets": {"base_url": ws_uri}}}
            e = exchange.Exchange(realtime_dispatcher, config_overrides=config_overrides)
            e.subscribe_to_order_book_events(p, on_order_book_event)

            await asyncio.gather(realtime_dispatcher.run(), stop_on_error(timeout))

    asyncio.run(asyncio.wait_for(test_main(5), 5))
