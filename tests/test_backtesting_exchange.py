# Basana
#
# Copyright 2022-2023 Gabriel Martin Becedillas Ruiz
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
import io
import logging

from dateutil import tz
import pytest

from basana.backtesting.exchange import CanceledOrder
from .helpers import abs_data_path, safe_round
from basana.backtesting import exchange, fees, orders, requests
from basana.core import bar, dt, event, helpers
from basana.core.pair import Pair, PairInfo
from basana.external.yahoo import bars


def build_bar(dt, pair, open, high, low, close, adj_close, volume, adjust_ohlc):
    if adjust_ohlc:
        open, high, low, close = bars.adjust_ohlc(open, high, low, close, adj_close)
    return bar.Bar(dt, pair, open, high, low, close, volume)


def test_account_balances(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(
            backtesting_dispatcher,
            {
                "usd": Decimal("1000"),
                "ars": Decimal("-2000.05"),
                "eth": Decimal("0")
            }
        )
        assert (await e.get_balance("usd")).available == Decimal("1000")
        assert (await e.get_balance("usd")).total == Decimal("1000")
        assert (await e.get_balance("ars")).available == Decimal("-2000.05")
        assert (await e.get_balance("eth")).available == Decimal("0")
        assert (await e.get_balance("ltc")).available == Decimal("0")

    asyncio.run(impl())


def test_order_index():
    idx = exchange.OrderIndex()
    for i in range(1, 3):
        idx.add_order(
            orders.MarketOrder(
                str(i), orders.OrderOperation.BUY, Pair("BTC", "USD"), Decimal("1"), orders.OrderState.OPEN
            )
        )
    assert "1" in [o.id for o in idx.get_open_orders()]
    idx.get_order("1").cancel()
    assert "1" not in [o.id for o in idx.get_open_orders()]
    assert "2" in [o.id for o in idx.get_open_orders()]
    assert len(idx._open_orders) == 2
    for _ in range(50 - 2):
        assert "2" in [o.id for o in idx.get_open_orders()]
    assert len(idx._open_orders) == 1


def test_create_get_and_cancel_order(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(
            backtesting_dispatcher,
            {
                "ARS": Decimal("-2000.05"),
                "BTC": Decimal("2"),
                "ETH": Decimal("0"),
                "USD": Decimal("50000"),
            }
        )
        order_request = requests.MarketOrder(exchange.OrderOperation.BUY, Pair("BTC", "USD"), Decimal("1"))
        created_order = await e.create_order(order_request)
        assert created_order is not None

        balances = await e.get_balances()
        for symbol in ["ARS", "BTC", "ETH", "USD"]:
            assert symbol in balances

        order_info = await e.get_order_info(created_order.id)
        assert order_info is not None
        assert order_info.is_open

        await e.cancel_order(created_order.id)

        order_info = await e.get_order_info(created_order.id)
        assert order_info is not None
        assert not order_info.is_open

        assert isinstance(await e.cancel_order(created_order.id), CanceledOrder)

        # There should be no holds in place.
        assert sum(e._balances._holds_by_symbol.values()) == 0
        assert sum(e._balances._holds_by_order.values()) == 0

    asyncio.run(impl())


def test_idempotent_cancel(backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {"USD": Decimal("1e6")})
    p = Pair("BTC", "USD")

    bs = event.FifoQueueEventSource(events=[
        # This one should be used during fill.
        bar.BarEvent(
            dt.local_datetime(2000, 1, 4, 23, 59, 59),
            bar.Bar(
                dt.local_datetime(2000, 1, 4), p, Decimal(2), Decimal(2), Decimal(2), Decimal(2), Decimal(10)
            )
        )
    ])
    e.add_bar_source(bs)
    e.set_pair_info(p, PairInfo(8, 2))

    async def impl():
        created_order = await e.create_order(
            requests.LimitOrder(exchange.OrderOperation.BUY, p, Decimal("0.1"), Decimal("2"))
        )
        await backtesting_dispatcher.run()

        order_info = await e.get_order_info(created_order.id)
        assert order_info is not None
        with pytest.raises(exchange.Error):
            await e.cancel_order(created_order.id)

    asyncio.run(impl())


def test_open_orders(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(
            backtesting_dispatcher,
            {
                "ARS": Decimal("-2000.05"),
                "BTC": Decimal("2"),
                "ETH": Decimal("0"),
                "USD": Decimal("50000"),
            }
        )

        open_orders = await e.get_open_orders()
        open_orders.extend(await e.get_open_orders(Pair("BTC", "USD")))
        assert len(open_orders) == 0

        order_request = requests.MarketOrder(exchange.OrderOperation.BUY, Pair("BTC", "USD"), Decimal("1"))
        created_order = await e.create_order(order_request)
        assert created_order is not None

        open_orders = await e.get_open_orders()
        open_orders.extend(await e.get_open_orders(Pair("BTC", "USD")))
        assert len(open_orders) == 2
        for open_order in open_orders:
            assert open_order.id is not None
            assert open_order.operation == exchange.OrderOperation.BUY
            assert open_order.amount == Decimal(1)
            assert open_order.amount_filled == Decimal(0)

    asyncio.run(impl())


def test_cancel_nonexistent_order(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(
            backtesting_dispatcher,
            {
                "ARS": Decimal("-2000.05"),
                "BTC": Decimal("0"),
                "ETH": Decimal("0"),
                "USD": Decimal("1000"),
            }
        )
        with pytest.raises(exchange.Error):
            await e.cancel_order("1234")

    asyncio.run(impl())


def test_bar_events_from_csv_and_backtesting_log_mode(backtesting_dispatcher, caplog):
    caplog.set_level(logging.INFO)

    # Create a custom logger with a specific format.
    logger = logging.getLogger(__name__)
    buff = io.StringIO()
    ch = logging.StreamHandler(stream=buff)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("[%(asctime)s %(levelname)s %(name)s] %(message)s"))
    logger.addHandler(ch)

    bar_events = []

    async def on_bar(bar_event):
        nonlocal bar_events
        bar_events.append(bar_event)
        logger.info("%s %s", bar_event.bar.pair, bar_event.bar.close)

    async def impl():
        e = exchange.Exchange(
            backtesting_dispatcher,
            {
                "IBM": Decimal("0"),
                "USD": Decimal("1000"),
            }
        )
        p = Pair("IBM", "USD")
        e.add_bar_source(bars.CSVBarSource(p, abs_data_path("orcl-2001-yahoo.csv"), sort=True))
        e.subscribe_to_bar_events(p, on_bar)

        diff = (backtesting_dispatcher.now() - dt.utc_now())
        assert abs(diff.total_seconds()) < 1
        await backtesting_dispatcher.run()
        diff = (backtesting_dispatcher.now() - dt.utc_now())
        assert abs(diff.total_seconds()) > 60

        assert bar_events[0].when == datetime.datetime(
            2001, 1, 2, hour=23, minute=59, second=59, microsecond=999999, tzinfo=tz.tzlocal()
        )
        assert bar_events[0].bar.open == Decimal("29.56")
        assert bar_events[0].bar.high == Decimal("29.75")
        assert bar_events[0].bar.low == Decimal("25.62")
        assert bar_events[0].bar.close == Decimal("26.37")
        assert bar_events[0].bar.volume == Decimal("46285300")

    asyncio.run(impl())

    # Get the log output.
    buff.flush()
    buff.seek(0)
    output = buff.read()

    assert "2001-01-02 23:59:59,999 INFO" in output
    assert "2001-12-31 23:59:59,999 INFO" in output


@pytest.mark.parametrize("order_plan", [
    {
        datetime.date(2000, 1, 3): [
            # Stop order canceled due to insufficient funds. Need to tweak the amount and stop price to get the order
            # accepted, but to fail as soon as it gets processed.
            (
                lambda e: e.create_stop_order(
                    exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal("1e6"), Decimal("0.01")
                ),
                False, None, Decimal(0)
            ),
        ],
    },
    {
        datetime.date(2000, 1, 7): [
            # Market order canceled due to insufficient funds. Need to tweak the amount to get the order accepted, but
            # to fail as soon as it gets processed.
            (
                lambda e: e.create_market_order(
                    exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal("9649")
                ),
                False, None, Decimal(0)
            ),
        ],
    },
    {
        datetime.date(2000, 1, 3): [
            # Buy market.
            (
                lambda e: e.create_market_order(
                    exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal("2")
                ),
                False, Decimal("115.50"), Decimal("0.58")
            ),
            # Limit order within bar.
            (
                lambda e: e.create_limit_order(
                    exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal("4"), Decimal("110.01")
                ),
                False, Decimal("110.01"), Decimal("1.11")
            ),
        ],
        datetime.date(2000, 1, 14): [
            # Sell market.
            (
                lambda e: e.create_market_order(
                    exchange.OrderOperation.SELL, Pair("ORCL", "USD"), Decimal("1")
                ),
                False, Decimal("107.87"), Decimal("0.27")
            ),
            # Limit order within bar.
            (
                lambda e: e.create_limit_order(
                    exchange.OrderOperation.SELL, Pair("ORCL", "USD"), Decimal("1"), Decimal("108")
                ),
                False, Decimal("108"), Decimal("0.27")
            ),
            # Sell stop.
            (
                lambda e: e.create_stop_order(
                    exchange.OrderOperation.SELL, Pair("ORCL", "USD"), Decimal("1"), Decimal("108")
                ),
                False, Decimal("107.87"), Decimal("0.27")
            ),
        ],
        datetime.date(2000, 1, 19): [
            # Stop price should be hit on 2000-01-20 and order should be filled on 2000-01-24.
            (
                lambda e: e.create_stop_limit_order(
                    exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal("5"), Decimal("59.5"),
                    Decimal("58.03")
                ),
                False, Decimal("58.03"), Decimal("0.73")
            ),
        ],
        datetime.date(2000, 3, 9): [
            # Stop price should be hit on 2000-03-10 and order should be filled on 2000-03-13 at open price.
            (
                lambda e: e.create_stop_limit_order(
                    exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal("10"), Decimal("81.62"),
                    Decimal("80.24")
                ),
                False, Decimal("78.50"), Decimal("1.97")
            ),
            # Stop price should be hit on 2000-03-10 and order should be filled on 2000-03-10.
            (
                lambda e: e.create_stop_limit_order(
                    exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal("9"), Decimal("81.62"),
                    Decimal("81")
                ),
                False, Decimal("81"), Decimal("1.83")
            ),
        ],
        datetime.date(2000, 3, 10): [
            # Stop price should be hit on 2000-03-13 and order should be filled on 2000-03-13.
            (
                lambda e: e.create_stop_limit_order(
                    exchange.OrderOperation.SELL, Pair("ORCL", "USD"), Decimal("1"), Decimal("79"),
                    Decimal("78.75")
                ),
                False, Decimal("78.75"), Decimal("0.2")
            ),
            # Stop price should be hit on 2000-03-13 and order should be filled on 2000-03-14.
            (
                lambda e: e.create_stop_limit_order(
                    exchange.OrderOperation.SELL, Pair("ORCL", "USD"), Decimal("1"), Decimal("79"),
                    Decimal("83.65")
                ),
                False, Decimal("83.65"), Decimal("0.21")
            ),
            # Stop price should be hit on 2000-03-13 and order should be filled on 2000-03-15 at open.
            (
                lambda e: e.create_stop_limit_order(
                    exchange.OrderOperation.SELL, Pair("ORCL", "USD"), Decimal("1"), Decimal("79"),
                    Decimal("83.80")
                ),
                False, Decimal("84"), Decimal("0.21")
            ),

        ],
    },
    {
        datetime.date(2001, 1, 2): [
            # Limit order is filled in multiple bars.
            (
                lambda e: e.create_limit_order(
                    exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal("50"), Decimal("10")
                ),
                False, Decimal("5.5"), Decimal("0.69")
            ),
        ],
    },
    {
        datetime.date(2000, 1, 3): [
            # Regression test.
            (
                lambda e: e.create_limit_order(
                    exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal("8600"), Decimal("115.50")
                ),
                False, Decimal("115.50"), Decimal("2483.25")
            ),
        ],
    },
])
def test_order_requests(order_plan, backtesting_dispatcher):
    e = exchange.Exchange(
        backtesting_dispatcher,
        {
            "USD": Decimal("1e6"),
        },
        fee_strategy=fees.Percentage(percentage=Decimal("0.25"))
    )
    expected = {}

    async def on_bar(bar_event):
        nonlocal expected

        order_requests = order_plan.get(bar_event.when.date(), [])
        for create_order_fun, expected_open, expected_fill_price, expected_fee in order_requests:
            created_order = await create_order_fun(e)
            assert created_order is not None
            expected[created_order.id] = {
                "is_open": expected_open,
                "fill_price": expected_fill_price,
                "fee": expected_fee,
            }

    async def impl():
        p = Pair("ORCL", "USD")

        e.add_bar_source(bars.CSVBarSource(p, abs_data_path("orcl-2000-yahoo-sorted.csv")))

        # These are for testing scenarios where fills take place in multiple bars.
        src = event.FifoQueueEventSource(events=[
            bar.BarEvent(
                dt.local_datetime(2001, 1, 2, 23, 59, 59),
                bar.Bar(
                    dt.local_datetime(2001, 1, 2), p, Decimal(5), Decimal(10), Decimal(1), Decimal(5), Decimal("100")
                )
            ),
            bar.BarEvent(
                dt.local_datetime(2001, 1, 3, 23, 59, 59),
                bar.Bar(
                    dt.local_datetime(2001, 1, 3), p, Decimal(5), Decimal(10), Decimal(1), Decimal(5), Decimal("100")
                )
            ),
            bar.BarEvent(
                dt.local_datetime(2001, 1, 4, 23, 59, 59),
                bar.Bar(
                    dt.local_datetime(2001, 1, 4), p, Decimal(5), Decimal(10), Decimal(1), Decimal(5), Decimal("100")
                )
            ),
            bar.BarEvent(
                dt.local_datetime(2001, 1, 5, 23, 59, 59),
                bar.Bar(
                    dt.local_datetime(2001, 1, 5), p, Decimal(5), Decimal(10), Decimal(1), Decimal(5), Decimal("100")
                )
            ),
        ])
        e.add_bar_source(src)

        e.subscribe_to_bar_events(p, on_bar)

        await backtesting_dispatcher.run()

        for order_id, expected_attrs in expected.items():
            order_info = await e.get_order_info(order_id)
            assert order_info is not None
            assert order_info.is_open == expected_attrs["is_open"], order_info
            assert safe_round(order_info.fill_price, 2) == expected_attrs["fill_price"], order_info
            expected_fees = {"USD": expected_attrs["fee"]} if expected_attrs["fee"] else {}
            assert order_info.fees == expected_fees, order_info

        # All orders are expected to be in a final state, so there should be no holds in place.
        assert sum(e._balances._holds_by_symbol.values()) == 0
        assert sum(e._balances._holds_by_order.values()) == 0

    asyncio.run(impl())


@pytest.mark.parametrize("order_request", [
    # Market order: Invalid amount.
    requests.MarketOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(0)),
    requests.MarketOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(-1)),
    requests.MarketOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal("0.1")),
    requests.MarketOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal("1.1")),
    requests.MarketOrder(exchange.OrderOperation.BUY, Pair("BTC", "USD"), Decimal("1.000000001")),
    # Limit order: Invalid amount/price.
    requests.LimitOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(0), Decimal("1")),
    requests.LimitOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("0")),
    requests.LimitOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("0.001")),
    requests.LimitOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("1.001")),
    requests.LimitOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("-0.1")),
    # Stop order: Invalid amount/price.
    requests.StopOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(0), Decimal("1")),
    requests.StopOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("0")),
    requests.StopOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("0.001")),
    requests.StopOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("1.001")),
    requests.StopOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("-0.1")),
    # Stop limit order: Invalid amount/price.
    requests.StopLimitOrder(
        exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(0), Decimal("1"), Decimal("1")
    ),
    requests.StopLimitOrder(
        exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("0"), Decimal("1")
    ),
    requests.StopLimitOrder(
        exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("0.001"), Decimal("1")
    ),
    requests.StopLimitOrder(
        exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("1.001"), Decimal("1")
    ),
    requests.StopLimitOrder(
        exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("-0.1"), Decimal("1")
    ),
    requests.StopLimitOrder(
        exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("1"), Decimal("0")
    ),
    requests.StopLimitOrder(
        exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("1"), Decimal("0.001")
    ),
    requests.StopLimitOrder(
        exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("1"), Decimal("1.001")
    ),
    requests.StopLimitOrder(
        exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1), Decimal("1"), Decimal("-0.1")
    ),
])
def test_invalid_parameter(order_request, backtesting_dispatcher):
    e = exchange.Exchange(
        backtesting_dispatcher,
        {
            "USD": Decimal("1e6"),
        },
        fee_strategy=fees.Percentage(percentage=Decimal("0.25"))
    )
    e.set_pair_info(Pair("BTC", "USD"), PairInfo(8, 2))

    async def impl():
        with pytest.raises(exchange.Error):
            await e.create_order(order_request)

        # Since all orders were rejected there should be no holds in place.
        assert sum(e._balances._holds_by_symbol.values()) == 0
        assert sum(e._balances._holds_by_order.values()) == 0

    asyncio.run(impl())


@pytest.mark.parametrize("order_request", [
    requests.MarketOrder(exchange.OrderOperation.SELL, Pair("ORCL", "USD"), Decimal(1)),
    requests.LimitOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1000), Decimal("1")),
    requests.LimitOrder(exchange.OrderOperation.SELL, Pair("ORCL", "USD"), Decimal(1), Decimal("1")),
    requests.StopOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1000), Decimal("1")),
    requests.StopOrder(exchange.OrderOperation.SELL, Pair("ORCL", "USD"), Decimal(1), Decimal("1")),
    requests.StopLimitOrder(
        exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal(1000), Decimal("1"), Decimal("1")
    ),
    requests.StopLimitOrder(
        exchange.OrderOperation.SELL, Pair("ORCL", "USD"), Decimal(1), Decimal("1"), Decimal("1")
    ),
])
def test_not_enough_balance(order_request, backtesting_dispatcher):
    e = exchange.Exchange(
        backtesting_dispatcher,
        {
            "USD": Decimal("1e3"),
        },
        fee_strategy=fees.Percentage(percentage=Decimal("0.25"))
    )
    e.set_pair_info(Pair("BTC", "USD"), PairInfo(8, 2))

    async def impl():
        with pytest.raises(exchange.Error):
            await e.create_order(order_request)

        # Since all orders were rejected there should be no holds in place.
        assert sum(e._balances._holds_by_symbol.values()) == 0
        assert sum(e._balances._holds_by_order.values()) == 0

    asyncio.run(impl())


def test_small_fill_is_ignored_after_rounding(backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {"USD": Decimal("1e6")})
    p = Pair("BTC", "USD")

    bs = event.FifoQueueEventSource(events=[
        # This one should be ignored since quote amount should be removed after rounding.
        bar.BarEvent(
            dt.local_datetime(2000, 1, 3, 23, 59, 59),
            bar.Bar(
                dt.local_datetime(2000, 1, 3), p, Decimal(1), Decimal(1), Decimal(1), Decimal(1), Decimal("0.01")
            )
        ),
        # This one should be used during fill.
        bar.BarEvent(
            dt.local_datetime(2000, 1, 4, 23, 59, 59),
            bar.Bar(
                dt.local_datetime(2000, 1, 4), p, Decimal(2), Decimal(2), Decimal(2), Decimal(2), Decimal(10)
            )
        )
    ])
    e.add_bar_source(bs)
    e.set_pair_info(p, PairInfo(8, 2))

    async def impl():
        created_order = await e.create_order(
            requests.LimitOrder(exchange.OrderOperation.BUY, p, Decimal("0.1"), Decimal("2"))
        )
        await backtesting_dispatcher.run()

        order_info = await e.get_order_info(created_order.id)
        assert order_info is not None
        assert not order_info.is_open
        assert order_info.fill_price == Decimal(2)

        # There should be no holds in place since the order is completed.
        assert sum(e._balances._holds_by_symbol.values()) == 0
        assert sum(e._balances._holds_by_order.values()) == 0

    asyncio.run(impl())


def test_liquidity_is_exhausted_and_order_is_canceled(backtesting_dispatcher):
    e = exchange.Exchange(
        backtesting_dispatcher,
        {
            "USD": Decimal("1e12")
        }
    )

    async def impl():
        bs = event.FifoQueueEventSource(events=[
            bar.BarEvent(
                dt.local_datetime(2000, 1, 3, 23, 59, 59),
                bar.Bar(
                    dt.local_datetime(2000, 1, 3), Pair("ORCL", "USD"),
                    Decimal("124.62"), Decimal("125.19"), Decimal("111.62"), Decimal("118.12"),
                    Decimal("98122000")
                )
            ),
            bar.BarEvent(
                dt.local_datetime(2000, 1, 7, 23, 59, 59),
                bar.Bar(
                    dt.local_datetime(2000, 1, 7), Pair("ORCL", "USD"),
                    Decimal("95.00"), Decimal("103.50"), Decimal("93.56"), Decimal("103.37"),
                    Decimal("91775600")
                )
            ),
        ])
        e.add_bar_source(bs)

        # Should get filled in the first bar.
        amount_1 = Decimal(int(98122000 * 0.25))
        created_order_1 = await e.create_order(
            requests.MarketOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), amount_1)
        )
        # Should get canceled because all liquidity was consumed by the previous order.
        amount_2 = Decimal(1)
        created_order_2 = await e.create_order(
            requests.MarketOrder(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), amount_2)
        )
        await backtesting_dispatcher.run()

        assert (await e.get_balance("ORCL")).available == amount_1

        order_1_info = await e.get_order_info(created_order_1.id)
        assert order_1_info is not None
        assert not order_1_info.is_open
        assert helpers.round_decimal(order_1_info.fill_price, 2) == Decimal("125.19")

        order_2_info = await e.get_order_info(created_order_2.id)
        assert order_2_info is not None
        assert not order_2_info.is_open

        # There should be no holds in place since the orders are in a final state.
        assert sum(e._balances._holds_by_symbol.values()) == 0
        assert sum(e._balances._holds_by_order.values()) == 0

    asyncio.run(impl())


def test_balance_is_on_hold_while_order_is_open(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(
            backtesting_dispatcher,
            {
                "USD": Decimal(1000)
            }
        )
        p = Pair("ORCL", "USD")

        created_order_1 = await e.create_order(
            requests.LimitOrder(exchange.OrderOperation.BUY, p, Decimal(1), Decimal(750))
        )
        assert (await e.get_balance("ORCL")).available == Decimal(0)
        assert (await e.get_balance("USD")).available == Decimal(250)

        created_order_2 = await e.create_order(requests.LimitOrder(
            exchange.OrderOperation.BUY, p, Decimal(1), Decimal(200)
        ))
        assert (await e.get_balance("ORCL")).available == Decimal(0)
        assert (await e.get_balance("USD")).available == Decimal(50)

        with pytest.raises(exchange.Error):
            await e.create_order(requests.LimitOrder(
                exchange.OrderOperation.BUY, p, Decimal("1"), Decimal(760)
            ))

        await e.cancel_order(created_order_2.id)
        assert (await e.get_balance("ORCL")).available == Decimal(0)
        assert (await e.get_balance("USD")).available == Decimal(250)

        await e.cancel_order(created_order_1.id)
        assert (await e.get_balance("ORCL")).available == Decimal(0)
        assert (await e.get_balance("USD")).available == Decimal(1000)

        # There should be no holds in place since orders got canceled.
        assert sum(e._balances._holds_by_symbol.values()) == 0
        assert sum(e._balances._holds_by_order.values()) == 0

    asyncio.run(impl())


def test_pair_info(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(
            backtesting_dispatcher,
            {
                "USD": Decimal("1e6"),
            },
            fee_strategy=fees.Percentage(percentage=Decimal("0.25"))
        )

        pair_info = await e.get_pair_info(Pair("ORCL", "USD"))
        assert pair_info.base_precision == 0
        assert pair_info.quote_precision == 2

        e.set_pair_info(Pair("ORCL", "EUR"), PairInfo(0, 3))
        pair_info = await e.get_pair_info(Pair("ORCL", "EUR"))
        assert pair_info.base_precision == 0
        assert pair_info.quote_precision == 3

    asyncio.run(impl())


def test_order_info_not_found(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(backtesting_dispatcher, {})
        with pytest.raises(exchange.Error, match="Order not found"):
            await e.get_order_info("unknown")

    asyncio.run(impl())


def test_bid_ask(backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {})

    async def impl():
        p = Pair("ORCL", "USD")
        bs = event.FifoQueueEventSource(events=[
            bar.BarEvent(
                dt.local_datetime(2000, 1, 3, 23, 59, 59),
                bar.Bar(
                    dt.local_datetime(2000, 1, 3), p,
                    Decimal("124.62"), Decimal("125.19"), Decimal("111.62"), Decimal("118.12"), Decimal("98122000")
                )
            )
        ])
        e.add_bar_source(bs)

        bid, ask = await e.get_bid_ask(p)
        assert bid is None and ask is None

        await backtesting_dispatcher.run()

        bid, ask = await e.get_bid_ask(p)
        assert bid == Decimal("117.83")
        assert ask == Decimal("118.41")

    asyncio.run(impl())
