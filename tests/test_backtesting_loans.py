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

import pytest

from basana.backtesting import exchange, lending
from basana.core import dt
from basana.core.bar import Bar, BarEvent
from basana.core.pair import Pair


def test_no_loans(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(backtesting_dispatcher, {})

        symbol = "USD"
        amount = Decimal(10000)
        with pytest.raises(Exception, match="Lending is not supported"):
            await e.create_loan(symbol, amount)

    asyncio.run(impl())


@pytest.mark.parametrize(
    ("loan_amount, loan_symbol, collateral_symbol, margin_requirement, "
     "interest_rate, interest_symbol, interest_period, min_interest, "
     "repay_after, "
     "initial_balances, intermediate_balances, final_balances"),
    [
        # Borrow USD using USD as collateral.
        # Minimum interest charged in USD.
        (
            Decimal(10000), "USD", "USD", Decimal("0.04"),
            Decimal("0.07"), "USD", datetime.timedelta(days=360), Decimal("1"),
            datetime.timedelta(seconds=0),
            {
                "USD": Decimal(401)
            },
            {
                "USD": dict(available=Decimal(10001), borrowed=Decimal(10000), hold=Decimal(400))
            },
            {
                "USD": dict(available=Decimal(400), borrowed=Decimal(0), hold=Decimal(0))
            },
        ),
        # Borrow BTC using USD as collateral.
        # No interest.
        (
            Decimal(1), "BTC", "USD", Decimal("0.2"),
            Decimal("0"), "USD", datetime.timedelta(seconds=0), Decimal("0"),
            datetime.timedelta(seconds=0),
            {
                "BTC": Decimal(0), "USD": Decimal(14000)
            },
            {
                "BTC": dict(available=Decimal(1), borrowed=Decimal(1), hold=Decimal(0)),
                "USD": dict(available=Decimal(0), borrowed=Decimal(0), hold=Decimal(14000))
            },
            {
                "BTC": dict(available=Decimal(0), borrowed=Decimal(0), hold=Decimal(0)),
                "USD": dict(available=Decimal(14000), borrowed=Decimal(0), hold=Decimal(0))
            },
        ),
        # Borrow BTC. No collateral. Proportional interest in USD.
        (
            Decimal(1), "BTC", "USD", Decimal(0),
            Decimal("0.2"), "USD", datetime.timedelta(days=360), Decimal("0"),
            datetime.timedelta(days=180),
            {
                "BTC": Decimal(0), "USD": Decimal(7000)
            },
            {
                "BTC": dict(available=Decimal(1), borrowed=Decimal(1), hold=Decimal(0)),
                "USD": dict(available=Decimal(7000), borrowed=Decimal(0), hold=Decimal(0))
            },
            {
                "BTC": dict(available=Decimal(0), borrowed=Decimal(0), hold=Decimal(0)),
                "USD": dict(available=Decimal(0), borrowed=Decimal(0), hold=Decimal(0))
            },
        ),
    ]
)
def test_borrow_and_repay(
    loan_amount, loan_symbol, collateral_symbol, margin_requirement,
    interest_rate, interest_symbol, interest_period, min_interest,
    repay_after,
    initial_balances, intermediate_balances, final_balances,
    backtesting_dispatcher
):
    async def impl():
        exchange_rates = {
            Pair("BTC", "USD"): Decimal(70000),
        }
        precisions = {
            "BTC": Decimal(8),
            "USD": Decimal(2),
        }

        loan_conditions = {
            loan_symbol: lending.LoanConditions(
                interest_symbol=interest_symbol, interest_rate=interest_rate,
                interest_period=interest_period, min_interest=min_interest,
                collateral_symbol=collateral_symbol, margin_requirement=margin_requirement
            ),
        }
        lending_strategy = lending.MarginLoans(conditions_by_symbol=loan_conditions)
        e = exchange.Exchange(backtesting_dispatcher, initial_balances, lending_strategy=lending_strategy)

        # Configure the exchange.
        for symbol, precision in precisions.items():
            e.set_symbol_precision(symbol, precision)

        # This is necessary to have prices since we're not doing bar events.
        now = dt.local_now()
        for pair, exchange_rate in exchange_rates.items():
            e._prices.on_bar_event(BarEvent(
                now,
                Bar(now, pair, exchange_rate, exchange_rate, exchange_rate, exchange_rate, Decimal(10))
            ))

        loan = await e.create_loan(loan_symbol, loan_amount)
        loans = await e.get_open_loans()
        assert loans == [loan]
        assert await e.get_loan(loan.id) == loan
        assert loan.is_open
        assert loan.borrowed_symbol == loan_symbol
        assert loan.borrowed_amount == loan_amount

        for symbol, expected_balance in intermediate_balances.items():
            balance = await e.get_balance(symbol)
            assert balance.available == expected_balance["available"], symbol
            assert balance.borrowed == expected_balance["borrowed"], symbol

        if repay_after:
            backtesting_dispatcher._last_dt = backtesting_dispatcher.now() + repay_after

        await e.repay_loan(loan.id)

        for symbol, expected_balance in final_balances.items():
            balance = await e.get_balance(symbol)
            assert balance.available == expected_balance["available"], symbol
            assert balance.borrowed == expected_balance["borrowed"], symbol

        loans = await e.get_open_loans()
        assert loans == []

        loan = await e.get_loan(loan.id)
        assert not loan.is_open
        assert loan.borrowed_symbol == loan_symbol
        assert loan.borrowed_amount == loan_amount

    asyncio.run(impl())


def test_repay_inexistent(backtesting_dispatcher):
    async def impl():
        lending_strategy = lending.MarginLoans(conditions_by_symbol={}, default_conditions=None)
        e = exchange.Exchange(backtesting_dispatcher, {}, lending_strategy=lending_strategy)

        with pytest.raises(Exception, match="Loan not found"):
            await e.repay_loan("inexistent")

    asyncio.run(impl())


def test_invalid_borrow_amount(backtesting_dispatcher):
    async def impl():
        lending_strategy = lending.MarginLoans(conditions_by_symbol={}, default_conditions=None)
        e = exchange.Exchange(backtesting_dispatcher, {}, lending_strategy=lending_strategy)

        with pytest.raises(Exception, match="Invalid amount"):
            await e.create_loan("USD", Decimal(-10000))

    asyncio.run(impl())
