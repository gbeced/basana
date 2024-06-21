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
from typing import List, Tuple

import pytest

from basana.backtesting import exchange, fees
from basana.backtesting.lending import margin
from basana.core import bar, dt, event
from basana.core.enums import OrderOperation
from basana.core.pair import Pair, PairInfo


def build_bar_source(
        pair: Pair,
        bar_duration: datetime.timedelta,
        prices: List[Tuple[datetime.datetime, Decimal]]
):
    bs = event.FifoQueueEventSource()
    for bar_begin, price in prices:
        bs.push(bar.BarEvent(
            bar_begin + bar_duration,
            bar.Bar(
                bar_begin, pair,
                price, price, price, price, Decimal("1e7")
            )
        ))
    return bs


@pytest.mark.parametrize("entry_dt, entry_limit_price, exit_dt, exit_limit_price, amount, final_balances", [
    # Limit order buy.
    (
        dt.local_datetime(2000, 1, 2), Decimal(1000),
        dt.local_datetime(2000, 1, 3), Decimal(2000),
        Decimal("5"),
        {
            "BTC": Decimal(0),
            "USD": Decimal("1000") + Decimal("5000") - Decimal("12.50") - Decimal("25") - Decimal("2.19")
        },
    ),
    # Limit order sell.
    (
        dt.local_datetime(2000, 1, 4), Decimal(3000),
        dt.local_datetime(2000, 1, 6), Decimal(1000),
        Decimal("-1"),
        {
            "BTC": Decimal(0),
            "USD": Decimal("1000") + Decimal("2000") - Decimal("7.5") - Decimal("2.5") - Decimal("1")
        },
    ),
    # Market order buy.
    (
        dt.local_datetime(2000, 1, 2), None,
        dt.local_datetime(2000, 1, 3), None,
        Decimal("5"),
        {
            "BTC": Decimal(0),
            "USD": Decimal("1000") + Decimal("5000") - Decimal("12.50") - Decimal("25") - Decimal("2.19")
        },
    ),
    # Market order sell.
    (
        dt.local_datetime(2000, 1, 4), None,
        dt.local_datetime(2000, 1, 6), None,
        Decimal("-1"),
        {
            "BTC": Decimal(0),
            "USD": Decimal("1000") + Decimal("2000") - Decimal("7.5") - Decimal("2.5") - Decimal("1")
        },
    ),
])
def test_entry_and_exit_ok(
        entry_dt, entry_limit_price, exit_dt, exit_limit_price, amount, final_balances, backtesting_dispatcher, caplog
):
    caplog.set_level(0)
    pair = Pair("BTC", "USD")
    order_ids = []
    # Should be able to borrow up to 5 times our initial balance.
    lending_strategy = margin.MarginLoans(
        "USD",
        default_conditions=margin.MarginLoanConditions(
            interest_symbol="USD", interest_percentage=Decimal(10), interest_period=datetime.timedelta(days=365),
            min_interest=Decimal(1), margin_requirement=Decimal("0.2")
        )
    )
    e = exchange.Exchange(
        backtesting_dispatcher, {"BTC": Decimal(0), "USD": Decimal(1000)},
        fee_strategy=fees.Percentage(percentage=Decimal("0.25")),
        lending_strategy=lending_strategy
    )

    async def enter_position():
        operation = OrderOperation.BUY if amount > Decimal(0) else OrderOperation.SELL
        if entry_limit_price:
            coro = e.create_limit_order(operation, pair, abs(amount), entry_limit_price, auto_borrow=True)
        else:
            coro = e.create_market_order(operation, pair, abs(amount), auto_borrow=True)
        order = await coro
        order_ids.append(order.id)

    async def exit_position():
        operation = OrderOperation.SELL if amount > Decimal(0) else OrderOperation.BUY
        if entry_limit_price:
            coro = e.create_limit_order(operation, pair, abs(amount), exit_limit_price, auto_repay=True)
        else:
            coro = e.create_market_order(operation, pair, abs(amount), auto_repay=True)
        order = await coro
        order_ids.append(order.id)

    async def on_bar(bar_event):
        action = {
            entry_dt: enter_position,
            exit_dt: exit_position,
        }.get(bar_event.when)
        if action:
            await action()

    async def impl():
        e.set_symbol_precision("BTC", 8)
        e.set_symbol_precision("USD", 2)
        e.subscribe_to_bar_events(pair, on_bar)

        # Load bars
        bs = build_bar_source(
            pair, datetime.timedelta(days=1),
            [
                (dt.local_datetime(2000, 1, 1), Decimal(1000)),
                (dt.local_datetime(2000, 1, 2), Decimal(1000)),
                (dt.local_datetime(2000, 1, 3), Decimal(2000)),
                (dt.local_datetime(2000, 1, 4), Decimal(3000)),
                (dt.local_datetime(2000, 1, 5), Decimal(2000)),
                (dt.local_datetime(2000, 1, 6), Decimal(1000)),
                (dt.local_datetime(2000, 1, 7), Decimal(800)),
            ]
        )
        e.add_bar_source(bs)
        await backtesting_dispatcher.run()

        # All orders must be completed.
        assert len(order_ids) == 2
        for order_id in order_ids:
            order_info = await e.get_order_info(order_id)
            assert order_info.is_open is False
            assert order_info.amount_remaining == Decimal(0)

        # There should be no open loans.
        open_loans = await e.get_loans(is_open=True)
        assert open_loans == []

        # Check available balances.
        balances = await e.get_balances()
        assert len(balances) == len(final_balances)
        for symbol, balance in balances.items():
            assert balance.available == final_balances[symbol]
            assert balance.hold == 0
            assert balance.borrowed == 0

    asyncio.run(impl())


def test_rollback_if_borrowing_fails(backtesting_dispatcher, caplog):
    caplog.set_level(0)
    pair = Pair("BTC", "USD")
    order_ids = []
    # Should be able to borrow up to 2 times our initial balance.
    lending_strategy = margin.MarginLoans(
        "USD",
        default_conditions=margin.MarginLoanConditions(
            interest_symbol="USD", interest_percentage=Decimal(10), interest_period=datetime.timedelta(days=365),
            min_interest=Decimal(1), margin_requirement=Decimal("0.5")
        )
    )
    e = exchange.Exchange(
        backtesting_dispatcher, {"ETH": Decimal(1)},
        fee_strategy=fees.Percentage(percentage=Decimal("0.25"), min_fee=Decimal("2001")),
        lending_strategy=lending_strategy
    )

    async def enter_position():
        # Short sell so we need to borrow not only BTC, but also USD for the fees.
        order = await e.create_limit_order(
            OrderOperation.SELL, pair, Decimal(2), Decimal(1000), auto_borrow=True
        )
        order_ids.append(order.id)

    async def on_bar(bar_event):
        action = {
            dt.local_datetime(2000, 1, 3): enter_position,
        }.get(bar_event.when)
        if action:
            await action()

    async def impl():
        e.set_symbol_precision("BTC", 8)
        e.set_symbol_precision("ETH", 18)
        e.set_symbol_precision("USD", 2)
        e.set_pair_info(pair, PairInfo(8, 2))
        e.subscribe_to_bar_events(pair, on_bar)

        # Load BTC bars.
        e.add_bar_source(
            build_bar_source(
                pair, datetime.timedelta(days=1),
                [
                    (dt.local_datetime(2000, 1, 2), Decimal(1000)),
                    (dt.local_datetime(2000, 1, 3), Decimal(1000)),
                ]
            )
        )
        # This is just to get prices for ETH.
        e.add_bar_source(
            build_bar_source(
                Pair("ETH", "USD"), datetime.timedelta(days=1),
                [
                    (dt.local_datetime(2000, 1, 2), Decimal(1000)),
                ]
            )
        )

        await backtesting_dispatcher.run()

        # There should be no orders created.
        assert len(order_ids) == 0

        # There should be no open loans.
        open_loans = await e.get_loans(is_open=True)
        assert open_loans == []

        # Check available balances.
        balances = await e.get_balances()
        assert balances["ETH"].available == 1
        for symbol, balance in balances.items():
            if symbol == "ETH":
                continue
            assert balance.available == 0
            assert balance.hold == 0
            assert balance.borrowed == 0

    asyncio.run(impl())

    assert caplog.text.count("Borrowing for order") == 2
    assert caplog.text.count("Canceling loan") == 1
    assert caplog.text.count("Not enough balance to accept order") == 1


def test_repay_fails(backtesting_dispatcher, caplog):
    caplog.set_level(0)
    pair = Pair("BTC", "USD")
    order_ids = []
    # Should be able to borrow up to 2 times our initial balance.
    lending_strategy = margin.MarginLoans(
        "USD",
        default_conditions=margin.MarginLoanConditions(
            interest_symbol="USD", interest_percentage=Decimal(10), interest_period=datetime.timedelta(days=365),
            min_interest=Decimal(1), margin_requirement=Decimal("0.5")
        )
    )
    e = exchange.Exchange(
        backtesting_dispatcher, {"BTC": Decimal(0), "USD": Decimal(1000)},
        fee_strategy=fees.NoFee(),
        lending_strategy=lending_strategy
    )
    amount = Decimal("1.8")

    async def enter_position():
        order = await e.create_limit_order(OrderOperation.BUY, pair, amount, Decimal(1000), auto_borrow=True)
        order_ids.append(order.id)

    async def exit_position():
        order = await e.create_market_order(OrderOperation.SELL, pair, amount, auto_repay=True)
        order_ids.append(order.id)

    async def on_bar(bar_event):
        action = {
            dt.local_datetime(2000, 1, 2): enter_position,
            dt.local_datetime(2000, 1, 3): exit_position,
        }.get(bar_event.when)
        if action:
            await action()

    async def impl():
        e.set_symbol_precision("BTC", 8)
        e.set_symbol_precision("USD", 2)
        e.set_pair_info(pair, PairInfo(8, 2))
        e.subscribe_to_bar_events(pair, on_bar)

        # Load bars
        bs = build_bar_source(
            pair, datetime.timedelta(days=1),
            [
                (dt.local_datetime(2000, 1, 1), Decimal(1000)),
                (dt.local_datetime(2000, 1, 2), Decimal(1000)),
                (dt.local_datetime(2000, 1, 3), Decimal(10)),
            ]
        )
        e.add_bar_source(bs)
        await backtesting_dispatcher.run()

        # All orders must be completed.
        assert len(order_ids) == 2
        for order_id in order_ids:
            order_info = await e.get_order_info(order_id)
            assert order_info.is_open is False
            assert order_info.amount_remaining == Decimal(0)

        # There should be 1 open loans.
        open_loans = await e.get_loans(is_open=True)
        assert len(open_loans) == 1

        # Check balances.
        balances = await e.get_balances()
        assert len(balances) == 2

        assert balances["BTC"].available == 0
        assert balances["BTC"].borrowed == 0
        assert balances["BTC"].hold == 0

        assert balances["USD"].available == Decimal("18")
        assert balances["USD"].borrowed == Decimal("800")
        assert balances["USD"].hold == 0

    asyncio.run(impl())
