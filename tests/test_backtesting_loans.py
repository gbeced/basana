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
    ("loan_symbol, collateral_symbol, interest_symbol, repay_after, initial_balances, intermediate_balances, "
     "final_balances"),
    [
        # Minimum interest, charged in base symbol.
        (
            "BTC", "USD", "BTC", datetime.timedelta(seconds=0),
            {
                "BTC": Decimal("0.01"),
                "USD": Decimal("0"),
            },
            {
                "BTC": dict(available=Decimal("1.01"), borrowed=Decimal("1")),
                "USD": dict(available=Decimal("0"), borrowed=Decimal("0")),
            },
            {
                "BTC": dict(available=Decimal("0"), borrowed=Decimal("0")),
                "USD": dict(available=Decimal("0"), borrowed=Decimal("0")),
            }
        ),
        # Proportional interest, charged in base symbol.
        (
            "BTC", "USD", "BTC", datetime.timedelta(hours=1),
            {
                "BTC": Decimal("0.03"),
                "USD": Decimal("0"),
            },
            {
                "BTC": dict(available=Decimal("1.03"), borrowed=Decimal("1")),
                "USD": dict(available=Decimal("0"), borrowed=Decimal("0")),
            },
            {
                "BTC": dict(available=Decimal("0"), borrowed=Decimal("0")),
                "USD": dict(available=Decimal("0"), borrowed=Decimal("0")),
            }
        ),
        # Minimum interest, charged in quote symbol.
        (
            "BTC", "USD", "USD", datetime.timedelta(seconds=0),
            {
                "BTC": Decimal("0"),
                "USD": Decimal("0.1"),
            },
            {
                "BTC": dict(available=Decimal("1"), borrowed=Decimal("1")),
                "USD": dict(available=Decimal("0.1"), borrowed=Decimal("0")),
            },
            {
                "BTC": dict(available=Decimal("0"), borrowed=Decimal("0")),
                "USD": dict(available=Decimal("0"), borrowed=Decimal("0")),
            }
        ),
        # Proportional interest, charged in quote symbol.
        (
            "BTC", "USD", "USD", datetime.timedelta(hours=1),
            {
                "BTC": Decimal("0"),
                "USD": Decimal("0.3"),
            },
            {
                "BTC": dict(available=Decimal("1"), borrowed=Decimal("1")),
                "USD": dict(available=Decimal("0.3"), borrowed=Decimal("0")),
            },
            {
                "BTC": dict(available=Decimal("0"), borrowed=Decimal("0")),
                "USD": dict(available=Decimal("0"), borrowed=Decimal("0")),
            }
        ),
    ]
)
def test_borrow_and_repay(
        loan_symbol, collateral_symbol, interest_symbol, repay_after, initial_balances, intermediate_balances,
        final_balances, backtesting_dispatcher
):
    async def impl():
        loan_amount = Decimal("1")
        interest_rate = Decimal("0.03")
        interest_period = datetime.timedelta(hours=1)
        min_interest = Decimal("0.01")
        margin_requirement = Decimal(0)

        loan_conditions = {
            loan_symbol: lending.LoanConditions(
                interest_symbol=interest_symbol, interest_rate=interest_rate,
                interest_period=interest_period, min_interest=min_interest,
                collateral_symbol=collateral_symbol, margin_requirement=margin_requirement
            ),
        }
        loan_factory = lending.CollateralizedInterestLoans(conditions_by_symbol=loan_conditions)
        e = exchange.Exchange(backtesting_dispatcher, initial_balances, loan_factory=loan_factory)

        # Configure the exchange.
        for symbol in [loan_symbol, collateral_symbol]:
            e.set_symbol_precision(symbol, 2)

        # This is necessary to have prices since we're not doing bar events.
        now = dt.local_now()
        e._prices.on_bar_event(BarEvent(
            now,
            Bar(
                now,
                Pair(loan_symbol, collateral_symbol), Decimal(10), Decimal(10), Decimal(10), Decimal(10), Decimal(10)
            )
        ))

        loan = await e.create_loan(loan_symbol, loan_amount)
        loans = await e.get_open_loans()
        assert loans == [loan]
        assert await e.get_loan(loan.id) == loan

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

    asyncio.run(impl())


def test_repay_inexistent(backtesting_dispatcher):
    async def impl():
        loan_factory = lending.CollateralizedInterestLoans(conditions_by_symbol={}, default_conditions=None)
        e = exchange.Exchange(backtesting_dispatcher, {}, loan_factory=loan_factory)

        with pytest.raises(Exception, match="Loan not found"):
            await e.repay_loan("inexistent")

    asyncio.run(impl())


def test_invalid_borrow_amount(backtesting_dispatcher):
    async def impl():
        loan_factory = lending.CollateralizedInterestLoans(conditions_by_symbol={}, default_conditions=None)
        e = exchange.Exchange(backtesting_dispatcher, {}, loan_factory=loan_factory)

        with pytest.raises(Exception, match="Invalid amount"):
            await e.create_loan("USD", Decimal(-10000))

    asyncio.run(impl())
