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

import pytest
import websockets

from .helpers import wait_caplog
from basana.core import pair
from basana.core.enums import OrderOperation
from basana.external.bitstamp import exchange


@pytest.mark.parametrize("public_events", [True, False])
def test_websocket_ok(public_events, bitstamp_http_api_mock, realtime_dispatcher):
    if not public_events:
        bitstamp_http_api_mock.post(
            "http://bitstamp.mock/api/v2/websockets_token/", status=200, payload={"user_id": "1234", "token": "1234"}
        )

    p = pair.Pair("BTC", "USD")
    last_order = None

    async def on_order_event(order_event):
        nonlocal last_order

        last_order = order_event.order
        if order_event.type == "order_deleted":
            realtime_dispatcher.stop()

    async def server_main(websocket):
        # We expect to receive a subscription request to start.
        message = json.loads(await websocket.recv())
        if message.get("event") == "bts:subscribe":
            # Remove the '-[user-id]' that was used for channel subscription.
            channel = message["data"]["channel"].rstrip("-1234")
            await websocket.send(json.dumps({"event": "bts:subscription_succeeded"}))
            # Keep on sending order events while the connection is open.
            while websocket.open:
                for event in ["order_created", "order_changed", "order_deleted"]:
                    await websocket.send(json.dumps({
                        "data": {
                            "id": 1531241723363332,
                            "id_str": "1531241723363332",
                            "order_type": 1,
                            "datetime": "1662673286",
                            "microtimestamp": "1662673286025000",
                            "amount": 0.28435528,
                            "amount_str": "0.28435528",
                            "price": 19342,
                            "price_str": "19342"
                        },
                        "channel": channel,
                        "event": event
                    }))
                await asyncio.sleep(0.1)

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
            if public_events:
                e.subscribe_to_public_order_events(p, on_order_event)
            else:
                e.subscribe_to_private_order_events(p, on_order_event)

            await realtime_dispatcher.run()

    asyncio.run(asyncio.wait_for(test_main(), 5))

    assert last_order is not None
    assert last_order.pair == p
    assert last_order.datetime is not None
    assert last_order.id == "1531241723363332"
    assert last_order.type == OrderOperation.SELL
    assert last_order.operation == OrderOperation.SELL
    assert last_order.price == Decimal("19342")
    assert last_order.amount == Decimal("0.28435528")


@pytest.mark.parametrize("server_message, expected_log", [
    ({"event": "bts:subscription_failed"}, "Error"),
    ({"event": "bts:error"}, "Error"),
])
def test_error_in_websocket(server_message, expected_log, realtime_dispatcher, caplog):
    p = pair.Pair("BTC", "USD")

    async def on_order_event(order_event):
        pass

    async def stop_on_error(timeout):
        assert await wait_caplog(expected_log, caplog, timeout=timeout)
        realtime_dispatcher.stop()

    async def server_main(websocket):
        # We expect to receive a subscription request to start.
        message = json.loads(await websocket.recv())
        if message.get("event") == "bts:subscribe":
            await websocket.send(json.dumps(server_message))
        else:
            assert False

    async def test_main(timeout):
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
            e.subscribe_to_public_order_events(p, on_order_event)
            await asyncio.gather(realtime_dispatcher.run(), stop_on_error(timeout))

    asyncio.run(asyncio.wait_for(test_main(6), 5))


def test_authentication_fails(bitstamp_http_api_mock, realtime_dispatcher, caplog):
    bitstamp_http_api_mock.post(
        "http://bitstamp.mock/api/v2/websockets_token/", status=403,
        payload={"status": "error", "reason": "Invalid signature", "code": "API0005"}
    )

    p = pair.Pair("BTC", "USD")

    async def on_order_event(order_event):
        pass

    async def stop_on_error(timeout):
        assert await wait_caplog("Invalid signature", caplog, timeout=timeout)
        realtime_dispatcher.stop()

    async def server_main(websocket):
        await websocket.recv()
        assert False, "The subscription message should not arrive since authentication failed"

    async def test_main(timeout):
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
            e.subscribe_to_private_order_events(p, on_order_event)

            await asyncio.gather(realtime_dispatcher.run(), stop_on_error(timeout))

    asyncio.run(asyncio.wait_for(test_main(6), 5))
