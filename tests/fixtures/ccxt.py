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

import asyncio
import copy
import datetime
import json
import re
import time
from unittest import mock

from ccxt.base.decimal_to_precision import DECIMAL_PLACES
import pytest
import websockets

from tests import helpers as test_helpers
from basana.external.ccxt import exchange


CLIENT_ORDER_ID = "51557545381C4997BC452AE1E48E0D88"
ORDER_TIME_MS = 1664556432583
ORDER_DATETIME = datetime.datetime(2022, 9, 30, 16, 47, 12, 583000, tzinfo=datetime.timezone.utc)

CCXT_BINANCE_CONFIG = {
    "urls": {"api": {"public": "http://binance.mock/api", "private": "http://binance.mock/api"}},
    "options": {
        "fetchCurrencies": False,
        "fetchMargins": False,
        "fetchMarkets": {"types": ["spot"]},
        "warnOnFetchOpenOrdersWithoutSymbol": False,
    },
}


def ccxt_binance_exchange_info():
    info = copy.deepcopy(test_helpers.load_json("binance_btc_usdt_exchange_info.json"))
    order_types = info["symbols"][0]["orderTypes"]
    for order_type in ["STOP_LOSS", "TAKE_PROFIT"]:
        if order_type not in order_types:
            order_types.append(order_type)
    return info


def mock_ccxt_exchange_info(binance_http_api_mock):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/exchangeInfo.*"), status=200,
        payload=ccxt_binance_exchange_info(), repeat=True,
    )


def binance_order_response(
        order_id, order_type, amount, price=None, stop_price=None, executed="0", cost="0", status="NEW"
):
    return {
        "symbol": "BTCUSDT",
        "orderId": int(order_id),
        "orderListId": -1,
        "clientOrderId": CLIENT_ORDER_ID,
        "price": "0.00000000" if price is None else str(price),
        "origQty": str(amount),
        "executedQty": str(executed),
        "cummulativeQuoteQty": str(cost),
        "status": status,
        "timeInForce": "GTC",
        "type": order_type,
        "side": "BUY",
        "stopPrice": "0.00000000" if stop_price is None else str(stop_price),
        "icebergQty": "0.00000000",
        "time": ORDER_TIME_MS,
        "updateTime": ORDER_TIME_MS,
        "isWorking": True,
        "origQuoteOrderQty": "0.00000000",
    }


def ccxt_binance_exchange(realtime_dispatcher):
    return exchange.Exchange(
        realtime_dispatcher, "binance", api_key="key", api_secret="secret", config=CCXT_BINANCE_CONFIG,
    )


def ccxt_binance_ws_config(host, port):
    config = copy.deepcopy(CCXT_BINANCE_CONFIG)
    config["urls"]["api"]["ws"] = {"spot": f"ws://{host}:{port}/ws"}
    return config


def binance_ccxt_kline_event(open_time_ms, open_price, high, low, close, volume, closed):
    close_time_ms = open_time_ms + 59_999
    return {
        "e": "kline",
        "E": int(time.time() * 1e3),
        "s": "BTCUSDT",
        "k": {
            "t": open_time_ms,
            "T": close_time_ms,
            "s": "BTCUSDT",
            "i": "1m",
            "f": 100,
            "L": 200,
            "o": open_price,
            "h": high,
            "l": low,
            "c": close,
            "v": volume,
            "n": 100,
            "x": closed,
            "q": "1.0000",
            "V": "500",
            "Q": "0.500",
            "B": "123456",
        },
    }


async def binance_ccxt_kline_server(websocket, klines, subscribe_params):
    message = json.loads(await websocket.recv())
    assert message["method"] == "SUBSCRIBE"
    assert message["params"] == subscribe_params
    await websocket.send(json.dumps({"result": None, "id": message["id"]}))

    async def send_klines():
        kline_index = 0
        while websocket.state == websockets.protocol.State.OPEN:
            if kline_index < len(klines):
                open_time_ms, open_price, high, low, close, volume = klines[kline_index]
                for kline_closed in (False, True):
                    await websocket.send(json.dumps(binance_ccxt_kline_event(
                        open_time_ms, open_price, high, low, close, volume, kline_closed,
                    )))
                    await asyncio.sleep(0.4)
                kline_index += 1
            else:
                await asyncio.sleep(0.4)

    async def handle_messages():
        while websocket.state == websockets.protocol.State.OPEN:
            message = json.loads(await websocket.recv())
            if message.get("method") == "UNSUBSCRIBE":
                await websocket.send(json.dumps({"result": None, "id": message["id"]}))

    await asyncio.gather(send_klines(), handle_messages())


@pytest.fixture()
def ccxt_cli_mock():
    cli = mock.MagicMock()
    cli.has = {
        "watchOHLCV": True, "unWatchOHLCV": True,
        "watchTrades": True, "unWatchTrades": True,
        "watchOrderBook": True, "unWatchOrderBook": True,
        "watchOrders": True, "unWatchOrders": True,
    }
    cli.precisionMode = DECIMAL_PLACES
    cli.fetch_ticker = mock.AsyncMock(return_value={
        "bid": "16757.47",
        "ask": "16758.13",
    })
    cli.load_markets = mock.AsyncMock()
    cli.fetch_balance = mock.AsyncMock(return_value={
        "free": {"BTC": "1.5", "USDT": "1000"},
        "used": {"BTC": "0.5", "USDT": "0"},
        "total": {"BTC": "2", "USDT": "1000"},
    })
    cli.create_order = mock.AsyncMock(return_value={
        "id": "1539419698798592",
        "clientOrderId": "51557545381C4997BC452AE1E48E0D88",
        "datetime": "2022-09-30T16:47:12.583Z",
        "symbol": "BTC/USDT",
        "type": "market",
        "side": "buy",
        "price": None,
        "amount": "0.001",
        "filled": 0,
        "remaining": "0.001",
        "status": "open",
    })
    cli.create_stop_order = mock.AsyncMock(return_value={
        "id": "1539419698798593",
        "clientOrderId": "51557545381C4997BC452AE1E48E0D88",
        "datetime": "2022-09-30T16:47:12.583Z",
        "symbol": "BTC/USDT",
        "type": "market",
        "side": "buy",
        "price": None,
        "amount": "0.001",
        "stopPrice": "15000",
        "filled": 0,
        "remaining": "0.001",
        "status": "open",
    })
    cli.create_stop_limit_order = mock.AsyncMock(return_value={
        "id": "1539419698798594",
        "clientOrderId": "51557545381C4997BC452AE1E48E0D88",
        "datetime": "2022-09-30T16:47:12.583Z",
        "symbol": "BTC/USDT",
        "type": "limit",
        "side": "buy",
        "price": "14900",
        "amount": "0.001",
        "stopPrice": "15000",
        "filled": 0,
        "remaining": "0.001",
        "status": "open",
    })
    cli.fetch_order = mock.AsyncMock(return_value={
        "id": "1539419698798592",
        "clientOrderId": "51557545381C4997BC452AE1E48E0D88",
        "datetime": "2022-09-30T16:47:12.583Z",
        "symbol": "BTC/USDT",
        "type": "limit",
        "side": "buy",
        "price": "10",
        "amount": "1",
        "filled": "0.5",
        "remaining": "0.5",
        "cost": "5",
        "status": "open",
    })
    cli.cancel_order = mock.AsyncMock(return_value={
        "id": "1539419698798592",
        "clientOrderId": "51557545381C4997BC452AE1E48E0D88",
        "datetime": "2022-09-30T16:47:12.583Z",
        "symbol": "BTC/USDT",
        "type": "limit",
        "side": "buy",
        "price": "10",
        "amount": "1",
        "filled": "0.5",
        "remaining": "0.5",
        "cost": "5",
        "status": "canceled",
    })
    cli.watch_ohlcv = mock.AsyncMock()
    cli.un_watch_ohlcv = mock.AsyncMock()
    cli.watch_trades = mock.AsyncMock()
    cli.un_watch_trades = mock.AsyncMock()
    cli.watch_order_book = mock.AsyncMock()
    cli.un_watch_order_book = mock.AsyncMock()
    cli.watch_orders = mock.AsyncMock()
    cli.un_watch_orders = mock.AsyncMock()
    cli.parse_timeframe = mock.Mock(return_value=60)
    cli.timeframes = {
        "1m": "1m",
        "5m": "5m",
    }
    cli.close = mock.AsyncMock()
    cli.market.return_value = {
        "precision": {
            "amount": 8,
            "price": 2,
        },
    }
    return cli


@pytest.fixture()
def ccxt_exchange(realtime_dispatcher, ccxt_cli_mock):
    e = exchange.Exchange(realtime_dispatcher, "binance", api_key="key", api_secret="secret")
    e._cli = ccxt_cli_mock
    return e