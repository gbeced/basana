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
import datetime
from decimal import Decimal
from unittest import mock

import aiohttp
from ccxt.base.decimal_to_precision import DECIMAL_PLACES, SIGNIFICANT_DIGITS, TICK_SIZE
import pytest

from basana.core import pair
from basana.core.enums import OrderOperation, PrecisionMode
from basana.external.ccxt import bars, exchange, helpers
from tests.fixtures.ccxt import (
    CLIENT_ORDER_ID,
    ORDER_DATETIME,
)


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
    ccxt_exchange._cli.precisionMode = TICK_SIZE
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
    assert order_created.datetime == ORDER_DATETIME
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
    assert open_order.datetime == ORDER_DATETIME
    assert open_order.operation == OrderOperation.BUY
    assert open_order.type == "limit"
    assert open_order.limit_price == Decimal("10")
    assert open_order.amount == Decimal("1")
    assert open_order.amount_filled == Decimal("0.5")
    assert open_order.pair == pair.Pair("BTC", "USDT")
    assert open_order.client_order_id == CLIENT_ORDER_ID

    ccxt_exchange._cli.fetch_open_orders.assert_awaited_once_with(expected_symbol, params={})


async def test_bars(realtime_dispatcher, ccxt_exchange):
    p = pair.Pair("BTC", "USDT")
    last_bar = None
    ohlcv_updates = [
        [[1_000, "0.001", "0.0025", "0.0005", "0.002", "1000"]],
        [
            [1_000, "0.001", "0.0025", "0.0005", "0.002", "1000"],
            [61_000, "0.002", "0.003", "0.0015", "0.0025", "50"],
        ],
        [
            [1_000, "0.001", "0.0025", "0.0005", "0.002", "1000"],
            [61_000, "0.002", "0.003", "0.0015", "0.0025", "50"],
            [121_000, "0.0025", "0.0035", "0.002", "0.003", "10"],
        ],
    ]
    call_count = {"n": 0}

    async def watch_ohlcv(*args, **kwargs):
        ret = ohlcv_updates[min(call_count["n"], len(ohlcv_updates) - 1)]
        call_count["n"] += 1
        return ret

    ccxt_exchange._cli.watch_ohlcv = mock.AsyncMock(side_effect=watch_ohlcv)

    async def on_bar_event(bar_event):
        nonlocal last_bar
        last_bar = bar_event.bar
        realtime_dispatcher.stop()

    ccxt_exchange.subscribe_to_bar_events(p, "1m", on_bar_event)
    await realtime_dispatcher.run()

    assert last_bar is not None
    assert last_bar.pair == p
    assert last_bar.open == Decimal("0.002")
    assert last_bar.high == Decimal("0.003")
    assert last_bar.low == Decimal("0.0015")
    assert last_bar.close == Decimal("0.0025")
    assert last_bar.volume == Decimal("50")
    assert last_bar.begin == datetime.datetime(1970, 1, 1, 0, 1, 1, tzinfo=datetime.timezone.utc)
    assert last_bar.duration == datetime.timedelta(seconds=60)
    ccxt_exchange._cli.watch_ohlcv.assert_awaited()
    ccxt_exchange._cli.un_watch_ohlcv.assert_awaited_once_with("BTC/USDT", "1m", params={})


async def test_close(ccxt_exchange):
    await ccxt_exchange.close()
    ccxt_exchange._cli.close.assert_awaited_once()


async def test_access_unwrapped_methods(ccxt_exchange):
    ccxt_exchange._cli.fetch_balance = mock.AsyncMock(return_value={
        "free": {"BTC": 1.5},
    })

    assert ccxt_exchange.ccxt is ccxt_exchange._cli
    balance = await ccxt_exchange.ccxt.fetch_balance()
    assert balance["free"]["BTC"] == 1.5
    ccxt_exchange._cli.fetch_balance.assert_awaited_once()


def test_invalid_exchange_id(realtime_dispatcher):
    with pytest.raises(AttributeError):
        exchange.Exchange(realtime_dispatcher, "not_a_real_exchange")


def test_pair_to_symbol():
    assert helpers.pair_to_symbol(pair.Pair("BTC", "USDT")) == "BTC/USDT"


def test_symbol_to_pair():
    assert helpers.symbol_to_pair("BTC/USDT") == pair.Pair("BTC", "USDT")


def test_ohlcv_to_bar():
    p = pair.Pair("BTC", "USDT")
    candle = [1_000, "0.001", "0.0025", "0.0005", "0.002", "1000"]
    candle_bar = helpers.ohlcv_to_bar(p, candle, 60)
    assert candle_bar.pair == p
    assert candle_bar.open == Decimal("0.001")
    assert candle_bar.high == Decimal("0.0025")
    assert candle_bar.low == Decimal("0.0005")
    assert candle_bar.close == Decimal("0.002")
    assert candle_bar.volume == Decimal("1000")
    assert candle_bar.begin == datetime.datetime(1970, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc)
    assert candle_bar.duration == datetime.timedelta(seconds=60)


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
    pair_info = helpers.pair_info_from_market(market, TICK_SIZE)
    assert pair_info.precision_mode == PrecisionMode.TICK_SIZE
    assert pair_info.base_precision == 2
    assert pair_info.quote_precision == 4
    assert pair_info.base_tick_size == Decimal("0.05")
    assert pair_info.quote_tick_size == Decimal("0.0008")


def test_pair_info_from_market_decimal_places():
    market = {
        "precision": {
            "amount": 8,
            "price": 2,
        },
    }
    pair_info = helpers.pair_info_from_market(market, DECIMAL_PLACES)
    assert pair_info.base_precision == 8
    assert pair_info.quote_precision == 2


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
    pair_info = helpers.pair_info_from_market(market, SIGNIFICANT_DIGITS)
    assert pair_info.base_precision == 8
    assert pair_info.quote_precision == 5
    assert pair_info.precision_mode == PrecisionMode.SIGNIFICANT_DIGITS


async def test_watch_ohlcv_event_source_handle_empty_ohlcv(ccxt_cli_mock, caplog):
    ccxt_cli_mock.watch_ohlcv = mock.AsyncMock(return_value=[])
    event_source = bars.WatchOHLCVEventSource(ccxt_cli_mock, pair.Pair("BTC", "USDT"), "1m")
    event_source._duration_secs = 60
    task = asyncio.create_task(event_source.main())
    await asyncio.sleep(0)
    assert "Error" not in caplog.text
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_watch_ohlcv_event_source_main_error(ccxt_cli_mock, caplog):
    ccxt_cli_mock.watch_ohlcv = mock.AsyncMock(side_effect=Exception("error"))
    event_source = bars.WatchOHLCVEventSource(ccxt_cli_mock, pair.Pair("BTC", "USDT"), "1m")
    task = asyncio.create_task(event_source.main())
    await asyncio.sleep(0)
    assert "Error watching OHLCV" in caplog.text
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_watch_ohlcv_event_source_finalize_error(ccxt_cli_mock, caplog):
    ccxt_cli_mock.un_watch_ohlcv = mock.AsyncMock(side_effect=Exception("error"))
    event_source = bars.WatchOHLCVEventSource(ccxt_cli_mock, pair.Pair("BTC", "USDT"), "1m")
    await event_source.finalize()
    assert "Error unwatching OHLCV" in caplog.text


async def test_watch_ohlcv_event_source_finalize_unsupported(ccxt_cli_mock):
    ccxt_cli_mock.has = {"watchOHLCV": True, "unWatchOHLCV": False}
    event_source = bars.WatchOHLCVEventSource(ccxt_cli_mock, pair.Pair("BTC", "USDT"), "1m")
    await event_source.finalize()
    ccxt_cli_mock.un_watch_ohlcv.assert_not_awaited()


def test_watch_ohlcv_event_source_invalid_timeframe(ccxt_cli_mock):
    with pytest.raises(ValueError, match="Invalid bar_duration: 1h"):
        bars.WatchOHLCVEventSource(ccxt_cli_mock, pair.Pair("BTC", "USDT"), "1h")


def test_watch_ohlcv_event_source_unsupported(ccxt_cli_mock):
    ccxt_cli_mock.has = {"watchOHLCV": False}
    with pytest.raises(NotImplementedError, match="watchOHLCV"):
        bars.WatchOHLCVEventSource(ccxt_cli_mock, pair.Pair("BTC", "USDT"), "1m")