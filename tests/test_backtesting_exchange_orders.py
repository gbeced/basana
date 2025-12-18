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

from collections import defaultdict
from decimal import Decimal
import asyncio
import dataclasses
import datetime

import pytest

from .helpers import abs_data_path
from .common import orcl_pair, aapl_pair, orcl_pair_info, btc_pair, btc_pair_info
from .backtesting_exchange_orders_test_data import \
    test_order_requests_data, test_invalid_requests_data
from basana.backtesting import errors, exchange, fees, requests
from basana.core import bar, dt, event, helpers
from basana.core.enums import OrderOperation
from basana.core.pair import Pair
from basana.external.yahoo import bars


def test_create_get_and_cancel_order(backtesting_dispatcher):
    e = exchange.Exchange(
        backtesting_dispatcher,
        {
            "ARS": Decimal("-2000.05"),
            "BTC": Decimal("2"),
            "ETH": Decimal("0"),
            "USD": Decimal("50000"),
        }
    )
    order_events = []
    bar_events = []

    async def on_order_updated(event: exchange.OrderEvent):
        order_events.append(event)

    async def on_bar(bar_event):
        bar_events.append(bar_event)

        order_request = requests.MarketOrder(OrderOperation.BUY, btc_pair, btc_pair_info, Decimal("1"))
        created_order = await e.create_order(order_request)
        assert created_order is not None

        order_info = await e.get_order_info(created_order.id)
        assert order_info is not None
        assert order_info.is_open
        assert dataclasses.asdict(order_info) == dataclasses.asdict(created_order)

        balances = await e.get_balances()
        for symbol in ["ARS", "BTC", "ETH", "USD"]:
            assert symbol in balances

        assert await e.get_orders() == [order_info]
        assert await e.get_orders(pair=btc_pair) == [order_info]
        assert await e.get_orders(pair=Pair("BTC", "ARS")) == []
        assert await e.get_orders(pair=btc_pair, is_open=True) == [order_info]

        await e.cancel_order(created_order.id)

        order_info = await e.get_order_info(created_order.id)
        assert order_info is not None
        assert not order_info.is_open

        assert await e.get_orders() == [order_info]
        assert await e.get_orders(pair=btc_pair) == [order_info]
        assert await e.get_orders(pair=btc_pair, is_open=False) == [order_info]
        assert await e.get_orders(pair=btc_pair, is_open=True) == []

        # The order was canceled, so trying to cancel it again should raise an error.
        with pytest.raises(exchange.Error):
            await e.cancel_order(created_order.id)

        # There should be no holds in place.
        assert sum(e._balances.holds.values()) == 0
        assert len(e._order_mgr._holds_by_order) == 0

    async def impl():
        e.subscribe_to_bar_events(btc_pair, on_bar)
        e.subscribe_to_order_events(on_order_updated)

        e.add_bar_source(
            event.FifoQueueEventSource(events=[
                bar.BarEvent(
                    dt.local_datetime(2001, 1, 3),
                    bar.Bar(
                        dt.local_datetime(2001, 1, 2), btc_pair,
                        Decimal(5), Decimal(10), Decimal(1), Decimal(5), Decimal(100),
                        datetime.timedelta(days=1)
                    )
                ),
            ])
        )

        await backtesting_dispatcher.run()

        assert len(bar_events) == 1
        assert len(order_events) == 2
        assert order_events[0].order.is_open is True
        assert order_events[1].order.is_open is False

    asyncio.run(impl())


def test_order_updates_have_priority(backtesting_dispatcher):
    e = exchange.Exchange(
        backtesting_dispatcher,
        {"USD": Decimal("50000")}
    )
    order_events = []
    bar_events = []
    all_events = []

    async def on_order_updated(event: exchange.OrderEvent):
        order_events.append(event)
        all_events.append(event)

    async def on_bar(bar_event):
        bar_events.append(bar_event)
        all_events.append(bar_event)

    async def impl():
        e.subscribe_to_bar_events(btc_pair, on_bar)
        e.subscribe_to_order_events(on_order_updated)

        e.add_bar_source(
            event.FifoQueueEventSource(events=[
                bar.BarEvent(
                    dt.local_datetime(2001, 1, 3),
                    bar.Bar(
                        dt.local_datetime(2001, 1, 2), btc_pair,
                        Decimal(5), Decimal(10), Decimal(1), Decimal(5), Decimal(100),
                        datetime.timedelta(days=1)
                    )
                ),
            ])
        )

        await e.create_limit_order(OrderOperation.BUY, btc_pair, Decimal(1), Decimal(10))
        await backtesting_dispatcher.run()

        assert len(bar_events) == 1
        assert len(order_events) == 1
        assert order_events[0].order.amount_filled == Decimal(1)
        assert all_events == order_events + bar_events

    asyncio.run(impl())


def test_order_info_not_found(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(backtesting_dispatcher, {})
        with pytest.raises(exchange.Error, match="Order not found"):
            await e.get_order_info("unknown")

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
        open_orders.extend(await e.get_open_orders(btc_pair))
        assert len(open_orders) == 0

        order_request = requests.MarketOrder(OrderOperation.BUY, btc_pair, btc_pair_info, Decimal("1"))
        created_order = await e.create_order(order_request)
        assert created_order is not None

        open_orders = await e.get_open_orders()
        open_orders.extend(await e.get_open_orders(btc_pair))
        assert len(open_orders) == 2
        for open_order in open_orders:
            assert open_order.id is not None
            assert open_order.operation == OrderOperation.BUY
            assert open_order.amount == Decimal(1)
            assert open_order.amount_filled == Decimal(0)

    asyncio.run(impl())


def test_cancel_inexistent_order(backtesting_dispatcher):
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


@pytest.mark.parametrize("order_plan, immediate_order_processing", test_order_requests_data)
def test_order_requests(order_plan, immediate_order_processing, backtesting_dispatcher):
    e = exchange.Exchange(
        backtesting_dispatcher,
        {"USD": Decimal("1e6")},
        fee_strategy=fees.Percentage(percentage=Decimal("0.25")),
        immediate_order_processing=immediate_order_processing
    )
    expected = {}
    order_events = defaultdict(list)

    async def on_order_updated(event: exchange.OrderEvent):
        order_events[event.order.id].append(event.order)

    async def on_bar(bar_event):
        order_requests = order_plan.get(bar_event.when.date(), [])
        for order_factory, expected_fills, expected_order_events in order_requests:
            created_order = await order_factory(e)
            assert created_order is not None
            expected[created_order.id] = {
                "fills": expected_fills,
                "order_events": expected_order_events,
            }

    async def impl():
        # ORCL bars.
        e.add_bar_source(bars.CSVBarSource(orcl_pair, abs_data_path("orcl-2000-yahoo-sorted.csv")))

        # AAPL bars.
        src = event.FifoQueueEventSource(events=[
            bar.BarEvent(
                dt.local_datetime(2001, 1, 3),
                bar.Bar(
                    dt.local_datetime(2001, 1, 2), aapl_pair,
                    Decimal(5), Decimal(10), Decimal(1), Decimal(5), Decimal("100"),
                    datetime.timedelta(days=1)
                )
            ),
            bar.BarEvent(
                dt.local_datetime(2001, 1, 4),
                bar.Bar(
                    dt.local_datetime(2001, 1, 3), aapl_pair,
                    Decimal(5), Decimal(10), Decimal(1), Decimal(5), Decimal("100"),
                    datetime.timedelta(days=1)
                )
            ),
            bar.BarEvent(
                dt.local_datetime(2001, 1, 5),
                bar.Bar(
                    dt.local_datetime(2001, 1, 4), aapl_pair,
                    Decimal(5), Decimal(10), Decimal(1), Decimal(5), Decimal("100"),
                    datetime.timedelta(days=1)
                )
            ),
            bar.BarEvent(
                dt.local_datetime(2001, 1, 6),
                bar.Bar(
                    dt.local_datetime(2001, 1, 5), aapl_pair,
                    Decimal(5), Decimal(10), Decimal(1), Decimal(5), Decimal("100"),
                    datetime.timedelta(days=1)
                )
            ),
            # bar.BarEvent(
            #     dt.local_datetime(2001, 1, 7),
            #     bar.Bar(
            #         dt.local_datetime(2001, 1, 6), aapl_p,
            #         Decimal(500), Decimal(1000), Decimal(100), Decimal(500), Decimal("10000"),
            #         datetime.timedelta(days=1)
            #     )
            # ),
        ])
        e.add_bar_source(src)

        e.subscribe_to_bar_events(orcl_pair, on_bar)
        e.subscribe_to_bar_events(aapl_pair, on_bar)
        e.subscribe_to_order_events(on_order_updated)

        await backtesting_dispatcher.run()

        for order_id, expected_attrs in expected.items():
            order_info = await e.get_order_info(order_id)
            assert not order_info.is_open, order_info

            # Check fills.
            expected_fills = expected_attrs["fills"]
            if expected_fills is not None:
                assert len(order_info.fills) == len(expected_fills)
                for i, expected_fill in enumerate(expected_fills):
                    order_fill = order_info.fills[i]
                    for attr, expected_value in expected_fill.items():
                        assert getattr(order_fill, attr) == expected_value

            # Check order events
            expected_events = expected_attrs["order_events"]
            if expected_events is not None:
                assert len(order_events.get(order_id, [])) == len(expected_events)
                for i, expected_order_event in enumerate(expected_events):
                    order_event = order_events[order_id][i]
                    for attr, expected_value in expected_order_event.items():
                        assert getattr(order_event, attr) == expected_value

        assert len(expected) == sum([len(orders) for orders in order_plan.values()])

        # All orders are expected to be in a final state, so there should be no holds in place.
        assert sum(e._balances.holds.values()) == 0
        assert len(e._order_mgr._holds_by_order) == 0

    asyncio.run(impl())


@pytest.mark.parametrize("order_request", test_invalid_requests_data)
def test_invalid_requests(order_request, backtesting_dispatcher):
    e = exchange.Exchange(
        backtesting_dispatcher,
        {
            "USD": Decimal("1e6"),
        },
        fee_strategy=fees.Percentage(percentage=Decimal("0.25"))
    )
    e.set_pair_info(btc_pair, btc_pair_info)

    async def impl():
        with pytest.raises(exchange.Error):
            await e.create_order(order_request)

        # Since all orders were rejected there should be no holds in place.
        assert sum(e._balances.holds.values()) == 0
        assert len(e._order_mgr._holds_by_order) == 0

    asyncio.run(impl())


@pytest.mark.parametrize("order_request", [
    requests.MarketOrder(OrderOperation.SELL, orcl_pair, orcl_pair_info, Decimal(1)),
    requests.LimitOrder(OrderOperation.BUY, orcl_pair, orcl_pair_info, Decimal(1000), Decimal("1")),
    requests.LimitOrder(OrderOperation.SELL, orcl_pair, orcl_pair_info, Decimal(1), Decimal("1")),
    requests.StopOrder(OrderOperation.BUY, orcl_pair, orcl_pair_info, Decimal(1000), Decimal("1")),
    requests.StopOrder(OrderOperation.SELL, orcl_pair, orcl_pair_info, Decimal(1), Decimal("1")),
    requests.StopLimitOrder(
        OrderOperation.BUY, orcl_pair, orcl_pair_info, Decimal(1000), Decimal("1"), Decimal("1")
    ),
    requests.StopLimitOrder(
        OrderOperation.SELL, orcl_pair, orcl_pair_info, Decimal(1), Decimal("1"), Decimal("1")
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
    e.set_pair_info(order_request.pair, orcl_pair_info)

    async def impl():
        with pytest.raises(errors.NotEnoughBalance):
            await e.create_order(order_request)

        # Since all orders were rejected there should be no holds in place.
        assert sum(e._balances.holds.values()) == 0
        assert len(e._order_mgr._holds_by_order) == 0

    asyncio.run(impl())


def test_small_fill_is_ignored_after_rounding(backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {"USD": Decimal("1e6")})

    bs = event.FifoQueueEventSource(events=[
        # This one should be ignored since quote amount should be removed after rounding.
        bar.BarEvent(
            dt.local_datetime(2000, 1, 3, 23, 59, 59),
            bar.Bar(
                dt.local_datetime(2000, 1, 3), btc_pair, Decimal(1), Decimal(1), Decimal(1), Decimal(1),
                Decimal("0.01"), datetime.timedelta(days=1)
            )
        ),
        # This one should be used during fill.
        bar.BarEvent(
            dt.local_datetime(2000, 1, 4, 23, 59, 59),
            bar.Bar(
                dt.local_datetime(2000, 1, 4), btc_pair, Decimal(2), Decimal(2), Decimal(2), Decimal(2), Decimal(10),
                datetime.timedelta(days=1)
            )
        )
    ])
    e.add_bar_source(bs)
    e.set_pair_info(btc_pair, btc_pair_info)

    async def impl():
        created_order = await e.create_order(
            requests.LimitOrder(OrderOperation.BUY, btc_pair, btc_pair_info, Decimal("0.1"), Decimal("2"))
        )
        await backtesting_dispatcher.run()

        order_info = await e.get_order_info(created_order.id)
        assert order_info is not None
        assert not order_info.is_open
        assert len(order_info.fills) == 1
        assert order_info.fill_price == Decimal(2)
        assert order_info.operation == exchange.OrderOperation.BUY
        assert order_info.amount == Decimal("0.1")
        assert order_info.limit_price == Decimal("2")

        # There should be no holds in place since the order is completed.
        assert sum(e._balances.holds.values()) == 0
        assert len(e._order_mgr._holds_by_order) == 0

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
                    dt.local_datetime(2000, 1, 3), orcl_pair,
                    Decimal("124.62"), Decimal("125.19"), Decimal("111.62"), Decimal("118.12"),
                    Decimal("98122000"),
                    datetime.timedelta(days=1)
                )
            ),
            bar.BarEvent(
                dt.local_datetime(2000, 1, 7, 23, 59, 59),
                bar.Bar(
                    dt.local_datetime(2000, 1, 7), orcl_pair,
                    Decimal("95.00"), Decimal("103.50"), Decimal("93.56"), Decimal("103.37"),
                    Decimal("91775600"),
                    datetime.timedelta(days=1)
                )
            ),
        ])
        e.add_bar_source(bs)

        # Should get filled in the first bar.
        amount_1 = Decimal(int(98122000 * 0.25))
        created_order_1 = await e.create_order(
            requests.MarketOrder(OrderOperation.BUY, orcl_pair, orcl_pair_info, amount_1)
        )
        # Should get canceled because all liquidity was consumed by the previous order.
        amount_2 = Decimal(1)
        created_order_2 = await e.create_order(
            requests.MarketOrder(OrderOperation.BUY, orcl_pair, orcl_pair_info, amount_2)
        )
        await backtesting_dispatcher.run()

        assert (await e.get_balance("ORCL")).available == amount_1

        order_1_info = await e.get_order_info(created_order_1.id)
        assert order_1_info is not None
        assert not order_1_info.is_open
        assert order_1_info.fill_price is not None
        assert helpers.round_decimal(order_1_info.fill_price, 2) == Decimal("137.08")

        order_2_info = await e.get_order_info(created_order_2.id)
        assert order_2_info is not None
        assert not order_2_info.is_open

        # There should be no holds in place since the orders are in a final state.
        assert sum(e._balances.holds.values()) == 0
        assert len(e._order_mgr._holds_by_order) == 0

    asyncio.run(impl())


def test_balance_is_on_hold_while_order_is_open(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(
            backtesting_dispatcher,
            {
                "USD": Decimal(1000)
            }
        )

        created_order_1 = await e.create_order(
            requests.LimitOrder(OrderOperation.BUY, orcl_pair, orcl_pair_info, Decimal(1), Decimal(750))
        )
        assert (await e.get_balance("ORCL")).available == Decimal(0)
        assert (await e.get_balance("USD")).available == Decimal(250)
        assert (await e.get_balance("USD")).hold == Decimal(750)

        created_order_2 = await e.create_order(requests.LimitOrder(
            OrderOperation.BUY, orcl_pair, orcl_pair_info, Decimal(1), Decimal(200)
        ))
        assert (await e.get_balance("ORCL")).available == Decimal(0)
        assert (await e.get_balance("USD")).available == Decimal(50)
        assert (await e.get_balance("USD")).hold == Decimal(950)

        with pytest.raises(exchange.Error):
            await e.create_order(requests.LimitOrder(
                OrderOperation.BUY, orcl_pair, orcl_pair_info, Decimal("1"), Decimal(760)
            ))

        await e.cancel_order(created_order_2.id)
        assert (await e.get_balance("ORCL")).available == Decimal(0)
        assert (await e.get_balance("USD")).available == Decimal(250)
        assert (await e.get_balance("USD")).hold == Decimal(750)

        await e.cancel_order(created_order_1.id)
        assert (await e.get_balance("ORCL")).available == Decimal(0)
        assert (await e.get_balance("USD")).available == Decimal(1000)
        assert (await e.get_balance("USD")).hold == Decimal(0)

        # There should be no holds in place since orders got canceled.
        assert sum(e._balances.holds.values()) == 0
        assert len(e._order_mgr._holds_by_order) == 0

    asyncio.run(impl())
