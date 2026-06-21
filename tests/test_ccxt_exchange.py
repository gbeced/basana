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

import datetime
from decimal import Decimal
from unittest import mock

import aiohttp
import ccxt.async_support as ccxt
import pytest

from basana.core import pair
from basana.core.enums import OrderOperation, PrecisionMode
from basana.external.ccxt import exchange, helpers

CLIENT_ORDER_ID = "51557545381C4997BC452AE1E48E0D88"


async def test_bid_ask(ccxt_exchange):
    bid, ask = await ccxt_exchange.get_bid_ask(pair.Pair("BTC", "USDT"))
    assert bid == Decimal("16757.47")
    assert ask == Decimal("16758.13")
    ccxt_exchange._cli.fetch_ticker.assert_awaited_once_with("BTC/USDT")


async def test_get_pair_info_decimal_places(ccxt_exchange):
    pair_info = await ccxt_exchange.get_pair_info(pair.Pair("BTC", "USDT"))
    assert pair_info.precision_mode == PrecisionMode.DECIMAL_PLACES
    assert pair_info.base_precision == 8
    assert pair_info.quote_precision == 2
    ccxt_exchange._cli.load_markets.assert_awaited_once()
    ccxt_exchange._cli.market.assert_called_once_with("BTC/USDT")


async def test_get_pair_info_tick_size(ccxt_exchange):
    ccxt_exchange._cli.precisionMode = ccxt.TICK_SIZE
    ccxt_exchange._cli.market.return_value = {
        "precision": {
            "amount": "0.00001",
            "price": "0.01",
        },
    }

    pair_info = await ccxt_exchange.get_pair_info(pair.Pair("BTC", "USDT"))
    assert pair_info.precision_mode == PrecisionMode.TICK_SIZE
    assert pair_info.base_precision == 5
    assert pair_info.quote_precision == 2
    assert pair_info.base_tick_size == Decimal("0.00001")
    assert pair_info.quote_tick_size == Decimal("0.01")


async def test_get_pair_info_cached(ccxt_exchange):
    p = pair.Pair("BTC", "USDT")
    await ccxt_exchange.get_pair_info(p)
    await ccxt_exchange.get_pair_info(p)

    ccxt_exchange._cli.load_markets.assert_awaited_once()


async def test_close(ccxt_exchange):
    await ccxt_exchange.close()
    ccxt_exchange._cli.close.assert_awaited_once()


async def test_get_balances(ccxt_exchange):
    balances = await ccxt_exchange.get_balances()

    btc_balance = balances["BTC"]
    assert btc_balance.available == Decimal("1.5")
    assert btc_balance.locked == Decimal("0.5")
    assert btc_balance.total == Decimal("2")

    usdt_balance = balances["USDT"]
    assert usdt_balance.available == Decimal("1000")
    assert usdt_balance.locked == Decimal("0")
    assert usdt_balance.total == Decimal("1000")

    ccxt_exchange._cli.fetch_balance.assert_awaited_once()


async def test_get_balance(ccxt_exchange):
    btc_balance = await ccxt_exchange.get_balance("btc")
    assert btc_balance.available == Decimal("1.5")
    assert btc_balance.locked == Decimal("0.5")
    assert btc_balance.total == Decimal("2")

    eth_balance = await ccxt_exchange.get_balance("ETH")
    assert eth_balance.available == Decimal("0")
    assert eth_balance.locked == Decimal("0")
    assert eth_balance.total == Decimal("0")

    assert ccxt_exchange._cli.fetch_balance.await_count == 2


@pytest.mark.parametrize(
        "create_order_fun, cli_method, expected_id, expected_amount, expected_price, expected_stop_price",
        [
            (
                lambda e: e.create_market_order(
                    OrderOperation.BUY, pair.Pair("BTC", "USDT"), Decimal("0.001"),
                    client_order_id=CLIENT_ORDER_ID
                ),
                "create_order", "1539419698798592", Decimal("0.001"), None, None
            ),
            (
                lambda e: e.create_limit_order(
                    OrderOperation.BUY, pair.Pair("BTC", "USDT"), Decimal("1"), Decimal("10"),
                    client_order_id=CLIENT_ORDER_ID
                ),
                "create_order", "1539419698798592", Decimal("1"), Decimal("10"), None
            ),
            (
                lambda e: e.create_stop_order(
                    OrderOperation.BUY, pair.Pair("BTC", "USDT"), Decimal("0.001"), Decimal("15000"),
                    client_order_id=CLIENT_ORDER_ID
                ),
                "create_stop_order", "1539419698798593", Decimal("0.001"), None, Decimal("15000")
            ),
            (
                lambda e: e.create_stop_limit_order(
                    OrderOperation.BUY, pair.Pair("BTC", "USDT"), Decimal("0.001"), Decimal("15000"),
                    Decimal("14900"), client_order_id=CLIENT_ORDER_ID
                ),
                "create_stop_limit_order", "1539419698798594", Decimal("0.001"), Decimal("14900"), Decimal("15000")
            ),
        ]
)
async def test_order_requests(
        create_order_fun, cli_method, expected_id, expected_amount, expected_price, expected_stop_price,
        ccxt_exchange
):
    cli_mock = getattr(ccxt_exchange._cli, cli_method)
    response = dict(cli_mock.return_value)
    response["amount"] = str(expected_amount)
    if expected_price is not None:
        response["price"] = str(expected_price)
    if expected_stop_price is not None:
        response["stopPrice"] = str(expected_stop_price)
    cli_mock.return_value = response

    order_created = await create_order_fun(ccxt_exchange)
    assert order_created.id == expected_id
    assert order_created.datetime == datetime.datetime(2022, 9, 30, 16, 47, 12, 583000).replace(
        tzinfo=datetime.timezone.utc
    )
    assert order_created.operation == OrderOperation.BUY
    assert order_created.amount == expected_amount
    assert order_created.limit_price == expected_price
    assert order_created.stop_price == expected_stop_price
    assert order_created.client_order_id == CLIENT_ORDER_ID

    cli_mock.assert_awaited_once()
    call_args = cli_mock.await_args
    assert call_args is not None
    assert call_args.args[0] == "BTC/USDT"
    if cli_method == "create_order":
        assert call_args.args[1] == ("market" if expected_price is None else "limit")
        assert call_args.args[2] == "buy"
        assert call_args.args[3] == str(expected_amount)
        if expected_price is not None:
            assert call_args.args[4] == str(expected_price)
            params = call_args.args[5]
        else:
            params = call_args.kwargs["params"]
    elif cli_method == "create_stop_order":
        assert call_args.args[1] == "market"
        assert call_args.args[2] == "buy"
        assert call_args.args[3] == str(expected_amount)
        assert call_args.kwargs["triggerPrice"] == str(expected_stop_price)
        params = call_args.kwargs["params"]
    else:
        assert call_args.args[1] == "buy"
        assert call_args.args[2] == str(expected_amount)
        assert call_args.args[3] == str(expected_price)
        assert call_args.args[4] == str(expected_stop_price)
        params = call_args.args[5]
    assert params["clientOrderId"] == CLIENT_ORDER_ID


@pytest.mark.parametrize("order_id, client_order_id, expected_lookup_id", [
    ("1539419698798592", None, "1539419698798592"),
    (None, CLIENT_ORDER_ID, CLIENT_ORDER_ID),
])
async def test_get_order_info(order_id, client_order_id, expected_lookup_id, ccxt_exchange):
    order_info = await ccxt_exchange.get_order_info(
        pair.Pair("BTC", "USDT"), order_id=order_id, client_order_id=client_order_id
    )
    assert order_info.id == "1539419698798592"
    assert order_info.client_order_id == CLIENT_ORDER_ID
    assert order_info.operation == OrderOperation.BUY
    assert order_info.is_open is True
    assert order_info.amount == Decimal("1")
    assert order_info.amount_filled == Decimal("0.5")
    assert order_info.amount_remaining == Decimal("0.5")
    assert order_info.quote_amount_filled == Decimal("5")
    assert order_info.limit_price == Decimal("10")
    assert order_info.fill_price == Decimal("10")

    ccxt_exchange._cli.fetch_order.assert_awaited_once_with(
        expected_lookup_id, "BTC/USDT",
        {} if client_order_id is None else {"clientOrderId": CLIENT_ORDER_ID}
    )


@pytest.mark.parametrize("order_id, client_order_id, expected_lookup_id", [
    ("1539419698798592", None, "1539419698798592"),
    (None, CLIENT_ORDER_ID, CLIENT_ORDER_ID),
])
async def test_cancel_order(order_id, client_order_id, expected_lookup_id, ccxt_exchange):
    canceled_order = await ccxt_exchange.cancel_order(
        pair.Pair("BTC", "USDT"), order_id=order_id, client_order_id=client_order_id
    )
    assert canceled_order.id == "1539419698798592"
    assert canceled_order.client_order_id == CLIENT_ORDER_ID
    assert canceled_order.operation == OrderOperation.BUY
    assert canceled_order.amount == Decimal("1")
    assert canceled_order.limit_price == Decimal("10")

    ccxt_exchange._cli.cancel_order.assert_awaited_once_with(
        expected_lookup_id, "BTC/USDT",
        {} if client_order_id is None else {"clientOrderId": CLIENT_ORDER_ID}
    )


@pytest.mark.parametrize("trading_pair, expected_symbol", [
    (None, None),
    (pair.Pair("BTC", "USDT"), "BTC/USDT"),
])
async def test_get_open_orders(trading_pair, expected_symbol, ccxt_exchange):
    open_orders = await ccxt_exchange.get_open_orders(trading_pair)
    assert len(open_orders) == 1
    open_order = open_orders[0]
    assert open_order.id == "1539419698798592"
    assert open_order.datetime == datetime.datetime(2022, 9, 30, 16, 47, 12, 583000).replace(
        tzinfo=datetime.timezone.utc
    )
    assert open_order.operation == OrderOperation.BUY
    assert open_order.type == "limit"
    assert open_order.limit_price == Decimal("10")
    assert open_order.amount == Decimal("1")
    assert open_order.amount_filled == Decimal("0.5")
    assert open_order.pair == pair.Pair("BTC", "USDT")
    assert open_order.client_order_id == CLIENT_ORDER_ID

    ccxt_exchange._cli.fetch_open_orders.assert_awaited_once_with(expected_symbol, params={})


async def test_ccxt_property_unwrapped_method(ccxt_exchange):
    ccxt_exchange._cli.fetch_balance = mock.AsyncMock(return_value={
        "free": {"BTC": "1.5"},
    })

    assert ccxt_exchange.ccxt is ccxt_exchange._cli
    balance = await ccxt_exchange.ccxt.fetch_balance()

    assert balance["free"]["BTC"] == "1.5"
    ccxt_exchange._cli.fetch_balance.assert_awaited_once()


def test_invalid_exchange_id(realtime_dispatcher):
    with pytest.raises(AttributeError):
        exchange.Exchange(realtime_dispatcher, "not_a_real_exchange")


def test_pair_to_symbol():
    assert helpers.pair_to_symbol(pair.Pair("BTC", "USDT")) == "BTC/USDT"


def test_symbol_to_pair():
    assert helpers.symbol_to_pair("BTC/USDT") == pair.Pair("BTC", "USDT")


def test_exchange_with_credentials(realtime_dispatcher):
    e = exchange.Exchange(realtime_dispatcher, "binance", api_key="key", api_secret="secret")
    assert e._cli.apiKey == "key"
    assert e._cli.secret == "secret"


async def test_exchange_with_session(realtime_dispatcher):
    async with aiohttp.ClientSession() as session:
        e = exchange.Exchange(realtime_dispatcher, "binance", session=session)
        assert e._cli.session is session
        assert e._cli.own_session is False


def test_pair_info_from_market_tick_size():
    market = {
        "precision": {
            "amount": "0.05",
            "price": "0.0008",
        },
    }
    pair_info = helpers.pair_info_from_market(market, ccxt.TICK_SIZE)
    assert pair_info.precision_mode == PrecisionMode.TICK_SIZE
    assert pair_info.base_precision == 2
    assert pair_info.quote_precision == 4
    assert pair_info.base_tick_size == Decimal("0.05")
    assert pair_info.quote_tick_size == Decimal("0.0008")


def test_order_status_is_open():
    assert helpers.order_status_is_open("open") is True
    assert helpers.order_status_is_open("canceled") is False


def test_order_info_optional_fields():
    order_info = exchange.OrderInfo({
        "id": "1",
        "side": "buy",
        "status": "open",
        "amount": "1",
        "filled": 0,
        "remaining": "1",
    })
    assert order_info.limit_price is None
    assert order_info.stop_price is None
    assert order_info.fill_price is None

    order_info_with_stop = exchange.OrderInfo({
        "id": "1",
        "side": "sell",
        "status": "closed",
        "amount": "1",
        "filled": 0,
        "remaining": "0",
        "stopPrice": "100",
    })
    assert order_info_with_stop.stop_price == Decimal("100")


def test_open_order_optional_fields():
    open_order = exchange.OpenOrder({
        "id": "1",
        "datetime": "2022-09-30T16:47:12.583Z",
        "symbol": "BTC/USDT",
        "type": "market",
        "side": "buy",
        "amount": "1",
        "filled": 0,
    })
    assert open_order.limit_price is None
    assert open_order.stop_price is None
    assert open_order.client_order_id is None


def test_canceled_order_optional_price():
    canceled_order = exchange.CanceledOrder({
        "id": "1",
        "side": "buy",
        "amount": "1",
    })
    assert canceled_order.limit_price is None


def test_pair_info_from_market_significant_digits():
    market = {
        "precision": {
            "amount": 8,
            "price": 5,
        },
    }
    pair_info = helpers.pair_info_from_market(market, ccxt.SIGNIFICANT_DIGITS)
    assert pair_info.base_precision == 8
    assert pair_info.quote_precision == 5
    assert pair_info.precision_mode == PrecisionMode.SIGNIFICANT_DIGITS