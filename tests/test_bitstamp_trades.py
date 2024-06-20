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
    last_trade = None

    async def on_trade_event(trade_event):
        nonlocal last_trade

        last_trade = trade_event.trade
        realtime_dispatcher.stop()

    async def server_main(websocket):
        # We expect to receive a subscription request to start.
        message = json.loads(await websocket.recv())
        if message.get("event") == "bts:subscribe":
            # Remove the '-[user-id]' that was used for channel subscription.
            channel = message["data"]["channel"].rstrip("-1234")
            await websocket.send(json.dumps({"event": "bts:subscription_succeeded"}))
            # Keep on sending trade events while the connection is open.
            while websocket.open:
                await websocket.send(json.dumps({
                    "event": "trade",
                    "channel": channel,
                    "data": {
                        "id": 246612672,
                        "timestamp": "1662573810",
                        "amount": 0.374,
                        "amount_str": "0.37400000",
                        "price": 19034,
                        "price_str": "19034",
                        "type": 0,
                        "microtimestamp": "1662573810482000",
                        "buy_order_id": 1530834271539201,
                        "sell_order_id": 1530834150440960
                    }
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
                e.subscribe_to_public_trade_events(p, on_trade_event)
            else:
                e.subscribe_to_private_trade_events(p, on_trade_event)

            await realtime_dispatcher.run()

    asyncio.run(asyncio.wait_for(test_main(), 5))

    assert last_trade is not None
    assert last_trade.pair == p
    assert last_trade.datetime is not None
    assert last_trade.id == "246612672"
    assert last_trade.type == OrderOperation.BUY
    assert last_trade.operation == OrderOperation.BUY
    assert last_trade.buy_order_id == "1530834271539201"
    assert last_trade.sell_order_id == "1530834150440960"
    assert last_trade.price == Decimal("19034")
    assert last_trade.amount == Decimal("0.374")
