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
import logging

import aiohttp
import aioresponses
import json
import pytest
import re
import websockets

from .helpers import wait_caplog
from basana.core import pair
from basana.external.bitstamp import exchange, order_book


@pytest.fixture()
def bitstamp_http_api_mock():
    with aioresponses.aioresponses() as m:
        m.get(re.compile(r'http://bitstamp.mock/api/v2/order_book/btcusd/.*'), status=200, payload={
            "timestamp": "1662146819",
            "microtimestamp": "1662146819514365",
            "bids": [
                [
                    "19822",
                    "0.15000000"
                ],
                [
                    "19820",
                    "0.88755147"
                ],
            ],
            "asks": [
                [
                    "19834",
                    "0.81049238"
                ],
                [
                    "19835",
                    "0.15000000"
                ],
            ]
        })

        yield m


def test_websocket_ok(realtime_dispatcher):
    p = pair.Pair("BTC", "USD")
    last_ob = None

    async def on_order_book_event(order_book_event):
        nonlocal last_ob

        last_ob = order_book_event.order_book
        realtime_dispatcher.stop()

    async def server_main(websocket):
        message = json.loads(await websocket.recv())
        # We expect to receive a subscription request to start.
        if message == {
            "event": "bts:subscribe",
            "data": {"channel": "order_book_btcusd"}
        }:
            await websocket.send(json.dumps({"event": "bts:subscription_succeeded"}))
            # Keep on sending order book events while the connection is open.
            while websocket.open:
                await websocket.send(json.dumps({
                    "event": "data",
                    "channel": "order_book_btcusd",
                    "data": {
                        "timestamp": "1662146819",
                        "microtimestamp": "1662146819514365",
                        "bids": [
                            [
                                "20000.01",
                                "0.00000001",
                            ]
                        ],
                        "asks": [
                            [
                                "19999.99",
                                "0.00000002",
                            ]
                        ]
                    }
                }))
                await asyncio.sleep(0.1)

    async def test_main():
        async with websockets.serve(server_main, "127.0.0.1", 0) as server:
            ws_uri = "ws://{}:{}/".format(*server.sockets[0].getsockname())
            e = exchange.Exchange(
                realtime_dispatcher, "", "", config_overrides={"api": {"websockets": {"base_url": ws_uri}}}
            )
            e.subscribe_to_order_book_events(p, on_order_book_event)
            await realtime_dispatcher.run()

    asyncio.run(asyncio.wait_for(test_main(), 5))

    assert last_ob is not None
    assert last_ob.pair == p
    assert last_ob.datetime is not None
    assert Decimal(last_ob.bids[0].price) == Decimal("20000.01")
    assert Decimal(last_ob.bids[0].volume) == Decimal("0.00000001")
    assert Decimal(last_ob.asks[0].price) == Decimal("19999.99")
    assert Decimal(last_ob.asks[0].volume) == Decimal("0.00000002")


@pytest.mark.parametrize("server_message", [
    {},
    {"event": "xyz"},
])
def test_unknown_message_in_websocket(server_message, realtime_dispatcher, caplog):
    p = pair.Pair("BTC", "USD")

    async def stop_on_unknown_message(timeout):
        assert await wait_caplog("Unknown message", caplog, timeout=timeout)
        realtime_dispatcher.stop()

    async def on_order_book_event(*args, **kwargs):
        assert False

    async def server_main(websocket):
        # We expect to receive a subscription request to start.
        message = json.loads(await websocket.recv())
        if message == {
            "event": "bts:subscribe",
            "data": {"channel": "order_book_btcusd"}
        }:
            await websocket.send(json.dumps({"event": "bts:subscription_succeeded"}))
            await websocket.send(json.dumps(server_message))
        else:
            assert False

    async def test_main(timeout):
        async with websockets.serve(server_main, "127.0.0.1", 0) as server:
            ws_uri = "ws://{}:{}/".format(*server.sockets[0].getsockname())
            e = exchange.Exchange(
                realtime_dispatcher, "", "", config_overrides={"api": {"websockets": {"base_url": ws_uri}}}
            )
            e.subscribe_to_order_book_events(p, on_order_book_event)

            await asyncio.gather(realtime_dispatcher.run(), stop_on_unknown_message(timeout))

    asyncio.run(asyncio.wait_for(test_main(6), 5))


def test_reconnect_request(realtime_dispatcher, caplog):
    caplog.set_level(logging.DEBUG)
    p = pair.Pair("BTC", "USD")

    async def stop_on_reconnection_request(timeout):
        assert await wait_caplog("Reconnection requested", caplog, timeout=timeout)
        realtime_dispatcher.stop()

    async def on_order_book_event(*args, **kwargs):
        pass

    async def server_main(websocket):
        await websocket.recv()
        await websocket.send(json.dumps({"event": "bts:request_reconnect"}))
        while True:
            await websocket.recv()

    async def test_main(timeout):
        async with websockets.serve(server_main, "127.0.0.1", 0) as server:
            ws_uri = "ws://{}:{}/".format(*server.sockets[0].getsockname())
            e = exchange.Exchange(
                realtime_dispatcher, "", "", config_overrides={"api": {"websockets": {"base_url": ws_uri}}}
            )
            e.subscribe_to_order_book_events(p, on_order_book_event)

            await asyncio.gather(realtime_dispatcher.run(), stop_on_reconnection_request(timeout))

    asyncio.run(asyncio.wait_for(test_main(6), 5))


def test_poll_ok_with_custom_session(bitstamp_http_api_mock, realtime_dispatcher):
    p = pair.Pair("BTC", "USD")
    last_ob = None

    async def on_order_book_event(order_book_event):
        nonlocal last_ob

        last_ob = order_book_event.order_book
        realtime_dispatcher.stop()

    async def test_main():
        async with aiohttp.ClientSession() as session:
            realtime_dispatcher.subscribe(
                order_book.PollOrderBook(
                    p, 0.5, group=1, session=session,
                    config_overrides={"api": {"http": {"base_url": "http://bitstamp.mock/"}}}
                ),
                on_order_book_event
            )

            await realtime_dispatcher.run()

    asyncio.run(asyncio.wait_for(test_main(), 2))

    assert last_ob is not None
    assert last_ob.pair == p
    assert last_ob.datetime is not None
    assert last_ob.bids[0].price == Decimal("19822")
    assert last_ob.bids[1].volume == Decimal("0.88755147")
    assert last_ob.asks[0].price == Decimal("19834")
    assert last_ob.asks[1].volume == Decimal("0.15000000")


@pytest.mark.parametrize("response_status, response_body", [
    (500, {}),
])
def test_unhandled_exception_during_poll(
    response_status, response_body, bitstamp_http_api_mock, realtime_dispatcher, caplog
):
    caplog.set_level(logging.INFO)
    bitstamp_http_api_mock.get(
        "http://bitstamp.mock/api/v2/order_book/btcpax/", status=response_status, payload=response_body
    )

    async def on_order_book_event(*args, **kwargs):
        assert False

    async def on_error(poller, error):
        await order_book.PollOrderBook.on_error(poller, error)
        realtime_dispatcher.stop()

    async def test_main():
        async with aiohttp.ClientSession() as session:
            poller = order_book.PollOrderBook(
                pair.Pair("BTC", "PAX"), 0.5, session=session,
                config_overrides={"api": {"http": {"base_url": "http://bitstamp.mock/"}}}
            )
            poller.on_error = lambda error: on_error(poller, error)
            realtime_dispatcher.subscribe(poller, on_order_book_event)

            await realtime_dispatcher.run()
            assert "Error polling order book" in caplog.text

    asyncio.run(asyncio.wait_for(test_main(), 5))
