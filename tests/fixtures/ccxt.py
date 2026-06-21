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

from unittest import mock

import ccxt.async_support as ccxt
import pytest

from basana.external.ccxt import exchange


@pytest.fixture()
def ccxt_cli_mock():
    cli = mock.MagicMock()
    cli.precisionMode = ccxt.DECIMAL_PLACES
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
    e = exchange.Exchange(realtime_dispatcher, "binance")
    e._cli = ccxt_cli_mock
    return e