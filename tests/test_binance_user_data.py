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
import logging
import re

import aioresponses
import pytest
import websockets
import websockets.protocol

from . import helpers
from basana.external.binance import exchange
import basana as bs


listen_key = "12345678"

EXECUTION_REPORT_MSG = {
    "stream": listen_key,
    "data": {
        "C": "",
        "E": 1735256226355,
        "F": "0.00000000",
        "I": 73412626486,
        "L": "95752.01000000",
        "M": True,
        "N": "BTC",
        "O": 1735256217797,
        "P": "0.00000000",
        "Q": "0.00000000",
        "S": "BUY",
        "T": 1735256226354,
        "V": "EXPIRE_MAKER",
        "W": 1735256217797,
        "X": "FILLED",
        "Y": "9.57520100",
        "Z": "9.57520100",
        "c": "web_5f41f24b392d4734b56cf0e32f974375",
        "e": "executionReport",
        "f": "GTC",
        "g": -1,
        "i": 34351225373,
        "l": "0.00010000",
        "m": True,
        "n": "0.00000010",
        "o": "LIMIT",
        "p": "95752.01000000",
        "q": "0.00010000",
        "r": "NONE",
        "s": "BTCUSDT",
        "t": 4339673735,
        "w": False,
        "x": "TRADE",
        "z": "0.00010000",
    }
}

OTHER_MSG = {
    "stream": listen_key,
    "data": {
        "B": [
            {"a": "BTC", "f": "0.00020639", "l": "0.00000000"},
            {"a": "BNB", "f": "0.00000324", "l": "0.00000000"},
            {"a": "USDT", "f": "2495.07648830", "l": "0.00000000"}
        ],
        "E": 1735070948134,
        "e": "outboundAccountPosition",
        "u": 1735070948133
    }
}

LISTEN_KEY_EXPIRED_MSG = {
    "stream": listen_key,
    "data": {
        "e": "listenKeyExpired",
        "E": "1699596037418",
        "listenKey": "OfYGbUzi3PraNagEkdKuFwUHn48brFsItTdsuiIXrucEvD0rhRXZ7I6URWfE8YE8",
    }
}


@pytest.mark.parametrize("account_attr, user_data_stream_url", [
    ("spot_account", "http://binance.mock/api/v3/userDataStream"),
    ("cross_margin_account", "http://binance.mock/sapi/v1/userDataStream"),
    ("isolated_margin_account", "http://binance.mock/sapi/v1/userDataStream/isolated"),
])
def test_websocket_ok(account_attr, user_data_stream_url, realtime_dispatcher, binance_http_api_mock, caplog):
    caplog.set_level(logging.DEBUG)
    order_update_event = None
    user_data_event = None
    keep_alive_ok = False
    license_key_expired_ok = False

    def check_stop_dispatcher():
        stop = user_data_event is not None
        stop = stop and order_update_event is not None
        stop = stop and keep_alive_ok
        stop = stop and license_key_expired_ok
        if stop:
            realtime_dispatcher.stop()

    async def check_keep_alive():
        if await helpers.wait_caplog("Channel keep alive", caplog):
            nonlocal keep_alive_ok
            keep_alive_ok = True

    async def check_re_subscription():
        if await helpers.wait_caplog("License key expired. Scheduling re-subscription", caplog):
            nonlocal license_key_expired_ok
            license_key_expired_ok = True

    async def on_user_data_event(event):
        if event.json["e"] == "outboundAccountPosition":
            nonlocal user_data_event
            user_data_event = event

        check_stop_dispatcher()

    async def on_order_update(event):
        nonlocal order_update_event
        assert event.json["e"] == "executionReport"
        order_update_event = event

        check_stop_dispatcher()

    async def server_main(websocket):
        message = json.loads(await websocket.recv())
        assert message["method"] == "SUBSCRIBE"
        await websocket.send(json.dumps({"result": None, "id": message["id"]}))

        while websocket.state == websockets.protocol.State.OPEN:
            await websocket.send(json.dumps(OTHER_MSG))
            await websocket.send(json.dumps(EXECUTION_REPORT_MSG))
            await websocket.send(json.dumps(LISTEN_KEY_EXPIRED_MSG))
            await asyncio.sleep(0.1)

    async def test_main():
        for meth in [binance_http_api_mock.post, binance_http_api_mock.put]:
            meth(
                re.compile(fr"{user_data_stream_url}.*"),
                status=200, payload={"listenKey": listen_key}, repeat=True
            )

        async with websockets.serve(server_main, "127.0.0.1", 0) as server:
            ws_uri = "ws://{}:{}/".format(*server.sockets[0].getsockname())
            config_overrides = {
                "api": {
                    "websockets": {
                        "base_url": ws_uri,
                        "spot": {
                            "user_data_stream": {
                                "heartbeat": 1,
                            },
                        },
                        "cross_margin": {
                            "user_data_stream": {
                                "heartbeat": 1,
                            },
                        },
                        "isolated_margin": {
                            "user_data_stream": {
                                "heartbeat": 1,
                            },
                        },
                    },
                    "http": {"base_url": "http://binance.mock/"},
                }
            }
            e = exchange.Exchange(
                realtime_dispatcher, api_key="api_key", api_secret="api_secret", config_overrides=config_overrides
            )
            account = getattr(e, account_attr)
            if account_attr == "isolated_margin_account":
                pair = bs.Pair("BTC", "USDT")
                account.subscribe_to_user_data_events(pair, on_user_data_event)
                account.subscribe_to_order_events(pair, on_order_update)
            else:
                account.subscribe_to_user_data_events(on_user_data_event)
                account.subscribe_to_order_events(on_order_update)

            await asyncio.gather(
                realtime_dispatcher.run(),
                check_keep_alive(),
                check_re_subscription(),
            )

    asyncio.run(asyncio.wait_for(test_main(), 5))

    assert user_data_event is not None
    assert user_data_event.when == datetime.datetime(2024, 12, 24, 20, 9, 8, 134000, tzinfo=datetime.timezone.utc)
    assert user_data_event.json["u"] == 1735070948133

    assert order_update_event is not None
    assert order_update_event.order_update.id == "34351225373"
    assert order_update_event.order_update.symbol == "BTCUSDT"
    assert order_update_event.order_update.client_order_id == "web_5f41f24b392d4734b56cf0e32f974375"
    assert order_update_event.order_update.operation == bs.OrderOperation.BUY
    assert order_update_event.order_update.type == "LIMIT"
    assert order_update_event.order_update.time_in_force == "GTC"
    assert order_update_event.order_update.amount == Decimal("0.0001")
    assert order_update_event.order_update.quote_amount is None
    assert order_update_event.order_update.limit_price == Decimal("95752.01")
    assert order_update_event.order_update.stop_price is None
    assert order_update_event.order_update.order_list_id is None
    assert order_update_event.order_update.status == "FILLED"
    assert order_update_event.order_update.is_open is False
    assert order_update_event.order_update.amount_filled == Decimal("0.0001")
    assert order_update_event.order_update.quote_amount_filled == Decimal("9.575201")
    assert order_update_event.order_update.fees == {"BTC": Decimal("0.0000001")}

    assert has_request(binance_http_api_mock, "POST", user_data_stream_url)
    assert has_request(binance_http_api_mock, "PUT", user_data_stream_url)


def has_request(mock: aioresponses.aioresponses, method: str, url: str) -> bool:
    for r_method, r_url in mock.requests:
        if r_method == method and str(r_url) == url:
            return True
    return False
