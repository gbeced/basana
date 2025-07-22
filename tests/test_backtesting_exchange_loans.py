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

import pytest

from basana.backtesting import errors, exchange
from basana.backtesting.lending import margin
from basana.core import dt, helpers
from basana.core.bar import Bar, BarEvent
from basana.core.pair import Pair


def test_no_loans(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(backtesting_dispatcher, {})

        symbol = "USD"
        amount = Decimal(10000)

        # This is necessary to have prices since we're not doing bar events.
        backtesting_dispatcher._set_now(dt.local_now())

        with pytest.raises(Exception, match="Lending is not supported"):
            await e.create_loan(symbol, amount)

    asyncio.run(impl())


@pytest.mark.parametrize(
    (
        "loan_amount, loan_symbol, margin_requirement, "
        "interest_percentage, interest_symbol, interest_period, min_interest, "
        "repay_after, "
        "initial_balances, intermediate_balances, final_balances, "
        "intermediate_margin_level, intermediate_interest, paid_interest"
    ),
    [
        # Borrow USD using USD as collateral.
        # Minimum interest charged in USD.
        (
            Decimal(10000), "USD", Decimal("0.04"),
            Decimal("7"), "USD", datetime.timedelta(days=360), Decimal(1),
            datetime.timedelta(seconds=0),
            {
                "USD": Decimal(400)
            },
            {
                "USD": dict(available=Decimal(10400), borrowed=Decimal(10000))
            },
            {
                "USD": dict(available=Decimal(399), borrowed=Decimal(0))
            },
            Decimal("99.75"),  # Minimum interest accrued in margin level calculation.
            {"USD": Decimal(1)},
            {"USD": Decimal(1)},
        ),
        # Borrow BTC using USD as collateral.
        # No interest.
        (
            Decimal(1), "BTC", Decimal("0.2"),
            Decimal("0"), "USD", datetime.timedelta(seconds=0), Decimal("0"),
            datetime.timedelta(seconds=0),
            {
                "BTC": Decimal(0), "USD": Decimal(14000)
            },
            {
                "BTC": dict(available=Decimal(1), borrowed=Decimal(1)),
                "USD": dict(available=Decimal(14000), borrowed=Decimal(0))
            },
            {
                "BTC": dict(available=Decimal(0), borrowed=Decimal(0)),
                "USD": dict(available=Decimal(14000), borrowed=Decimal(0))
            },
            Decimal(100),
            {},
            {},
        ),
        # Borrow BTC. No collateral required. Proportional interest in USD.
        (
            Decimal(1), "BTC", Decimal("0"),
            Decimal("20"), "USD", datetime.timedelta(days=360), Decimal("0"),
            datetime.timedelta(days=180),
            {
                "BTC": Decimal("0.01"), "USD": Decimal(14000)
            },
            {
                "BTC": dict(available=Decimal("1.01"), borrowed=Decimal(1)),
                "USD": dict(available=Decimal(14000), borrowed=Decimal(0))
            },
            {
                "BTC": dict(available=Decimal("0.01"), borrowed=Decimal(0)),
                "USD": dict(available=Decimal(7000), borrowed=Decimal(0))
            },
            Decimal(0),
            {"USD": Decimal(7000)},
            {"USD": Decimal(7000)},
        ),
        # Borrow BTC using BTC as collateral. No interest.
        (
            Decimal(1), "BTC", Decimal("0.1"),
            Decimal("0"), "USD", datetime.timedelta(days=360), Decimal("0"),
            datetime.timedelta(days=180),
            {
                "BTC": Decimal("0.1")
            },
            {
                "BTC": dict(available=Decimal("1.1"), borrowed=Decimal(1)),
            },
            {
                "BTC": dict(available=Decimal("0.1"), borrowed=Decimal(0)),
            },
            Decimal(100),
            {},
            {},
        ),
    ]
)
def test_borrow_and_repay(
        loan_amount, loan_symbol, margin_requirement,
        interest_percentage, interest_symbol, interest_period, min_interest,
        repay_after,
        initial_balances, intermediate_balances, final_balances,
        intermediate_margin_level, intermediate_interest, paid_interest,
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
            loan_symbol: margin.MarginLoanConditions(
                interest_symbol=interest_symbol, interest_percentage=interest_percentage,
                interest_period=interest_period, min_interest=min_interest,
                margin_requirement=margin_requirement
            ),
        }
        lending_strategy = margin.MarginLoans("USD")
        for symbol, conditions in loan_conditions.items():
            lending_strategy.set_conditions(symbol, conditions)

        e = exchange.Exchange(backtesting_dispatcher, initial_balances, lending_strategy=lending_strategy)

        # Configure the exchange.
        for symbol, precision in precisions.items():
            e.set_symbol_precision(symbol, precision)

        # This is necessary to have prices and dates since we're not doing bar events.
        now = dt.local_now()
        for pair, exchange_rate in exchange_rates.items():
            e._prices.on_bar_event(BarEvent(
                now,
                Bar(now, pair, exchange_rate, exchange_rate, exchange_rate, exchange_rate, Decimal(10))
            ))
        backtesting_dispatcher._set_now(now)

        # Create loan
        assert lending_strategy.margin_level == Decimal(0)
        loan = await e.create_loan(loan_symbol, loan_amount)

        # Checks while loan is open.
        loans = await e.get_loans(is_open=True)
        assert loans == [loan]
        assert await e.get_loan(loan.id) == loan
        assert loan.is_open
        assert loan.borrowed_symbol == loan_symbol
        assert loan.borrowed_amount == loan_amount

        # Time to repay. Move clock if necessary.
        backtesting_dispatcher._set_now(dt.local_now())
        if repay_after:
            backtesting_dispatcher._set_now(backtesting_dispatcher.now() + repay_after)

        # Checks before repay.
        assert helpers.round_decimal(lending_strategy.margin_level, 2) == intermediate_margin_level

        for symbol, expected_balance in intermediate_balances.items():
            balance = await e.get_balance(symbol)
            assert balance.available == expected_balance["available"], symbol
            assert balance.borrowed == expected_balance["borrowed"], symbol
            assert balance.hold == Decimal(0)

        loan = await e.get_loan(loan.id)
        assert loan.outstanding_interest == intermediate_interest
        assert loan.paid_interest == {}

        await e.repay_loan(loan.id)
        assert loan.paid_interest == {}  # Regression check.

        # Checks after repay.
        for symbol, expected_balance in final_balances.items():
            balance = await e.get_balance(symbol)
            assert balance.available == expected_balance["available"], symbol
            assert balance.borrowed == expected_balance["borrowed"], symbol
            assert balance.hold == Decimal(0)

        loans = await e.get_loans(borrowed_symbol=loan_symbol, is_open=True)
        assert loans == []

        loan = await e.get_loan(loan.id)
        assert not loan.is_open
        assert loan.borrowed_symbol == loan_symbol
        assert loan.borrowed_amount == loan_amount
        assert loan.outstanding_interest == {}
        assert loan.paid_interest == paid_interest

        assert lending_strategy.margin_level == Decimal(0)

    asyncio.run(impl())


def test_margin_exceeded(backtesting_dispatcher):
    async def impl():
        exchange_rates = {
            Pair("BTC", "USD"): Decimal(70000),
        }
        precisions = {
            "BTC": Decimal(8),
            "USD": Decimal(2),
        }

        loan_conditions = {
            "USD": margin.MarginLoanConditions(
                interest_symbol="USD", interest_percentage=Decimal("15"),
                interest_period=datetime.timedelta(days=365), min_interest=Decimal(0),
                margin_requirement=Decimal("0.2")
            ),
        }
        lending_strategy = margin.MarginLoans("USD")
        for symbol, conditions in loan_conditions.items():
            lending_strategy.set_conditions(symbol, conditions)

        initial_balances = {"USD": Decimal(1000)}
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
        backtesting_dispatcher._set_now(dt.local_now())

        # We should be able to borrow up to 5k.
        assert lending_strategy.margin_level == Decimal(0)
        for _ in range(5):
            await e.create_loan("USD", Decimal(1000))
        assert lending_strategy.margin_level == Decimal(100)
        assert (await e.get_balance("USD")).borrowed == Decimal(5000)

        with pytest.raises(errors.NotEnoughBalance, match="Margin level too low"):
            await e.create_loan("USD", Decimal("0.01"))

        assert lending_strategy.margin_level == Decimal(100)

    asyncio.run(impl())


def test_repay_inexistent(backtesting_dispatcher):
    async def impl():
        lending_strategy = margin.MarginLoans("USD", default_conditions=None)
        e = exchange.Exchange(backtesting_dispatcher, {}, lending_strategy=lending_strategy)

        # This is necessary to have prices since we're not doing bar events.
        backtesting_dispatcher._set_now(dt.local_now())

        with pytest.raises(Exception, match="Loan not found"):
            await e.repay_loan("inexistent")
        with pytest.raises(Exception, match="Loan not found"):
            await e.get_loan("inexistent")

    asyncio.run(impl())


def test_invalid_borrow_amount(backtesting_dispatcher):
    async def impl():
        lending_strategy = margin.MarginLoans("USD")
        e = exchange.Exchange(backtesting_dispatcher, {}, lending_strategy=lending_strategy)

        # This is necessary to have prices since we're not doing bar events.
        backtesting_dispatcher._set_now(dt.local_now())

        with pytest.raises(Exception, match="Invalid amount"):
            await e.create_loan("USD", Decimal(-10000))

    asyncio.run(impl())


def test_no_lending_conditions_for_symbol(backtesting_dispatcher):
    async def impl():
        lending_strategy = margin.MarginLoans("USD")
        e = exchange.Exchange(backtesting_dispatcher, {"USD": 1000}, lending_strategy=lending_strategy)

        # This is necessary to have prices since we're not doing bar events.
        backtesting_dispatcher._set_now(dt.local_now())

        with pytest.raises(Exception, match="No lending conditions for USD"):
            await e.create_loan("USD", Decimal(700))

    asyncio.run(impl())


def test_repay_twice(backtesting_dispatcher):
    async def impl():
        lending_strategy = margin.MarginLoans(
            "USD",
            default_conditions=margin.MarginLoanConditions(
                interest_symbol="USD", interest_percentage=Decimal(0), interest_period=datetime.timedelta(days=1),
                min_interest=Decimal(0), margin_requirement=Decimal(0)
            )
        )
        e = exchange.Exchange(backtesting_dispatcher, {}, lending_strategy=lending_strategy)
        e.set_symbol_precision("USD", 2)

        # This is necessary to have prices since we're not doing bar events.
        backtesting_dispatcher._set_now(dt.local_now())

        loan = await e.create_loan("USD", Decimal("1000"))
        await e.repay_loan(loan.id)
        with pytest.raises(Exception, match="Loan is not open"):
            await e.repay_loan(loan.id)

    asyncio.run(impl())


def test_not_enough_balance_to_repay(backtesting_dispatcher):
    async def impl():
        lending_strategy = margin.MarginLoans(
            "USD",
            default_conditions=margin.MarginLoanConditions(
                interest_symbol="USD", interest_percentage=Decimal(0), interest_period=datetime.timedelta(days=1),
                min_interest=Decimal(1), margin_requirement=Decimal(0)
            )
        )
        e = exchange.Exchange(backtesting_dispatcher, {}, lending_strategy=lending_strategy)
        e.set_symbol_precision("USD", 2)

        # This is necessary to have prices since we're not doing bar events.
        backtesting_dispatcher._set_now(dt.local_now())

        loan = await e.create_loan("USD", Decimal("1000"))
        with pytest.raises(Exception, match="Not enough USD available"):
            await e.repay_loan(loan.id)

    asyncio.run(impl())


def test_cancel_loan(backtesting_dispatcher):
    async def impl():
        lending_strategy = margin.MarginLoans(
            "USD",
            default_conditions=margin.MarginLoanConditions(
                interest_symbol="USD", interest_percentage=Decimal(0), interest_period=datetime.timedelta(days=1),
                min_interest=Decimal(1), margin_requirement=Decimal(0)
            )
        )
        e = exchange.Exchange(backtesting_dispatcher, {"USD": Decimal(0)}, lending_strategy=lending_strategy)
        e.set_symbol_precision("USD", 2)

        # This is necessary to have prices since we're not doing bar events.
        backtesting_dispatcher._set_now(dt.local_now())

        balances_pre = await e.get_balances()
        loan = await e.create_loan("USD", Decimal("1000"))
        e._loan_mgr.cancel_loan(loan.id)
        balances_post = await e.get_balances()

        loan = await e.get_loan(loan.id)
        assert loan.is_open is False
        assert loan.outstanding_interest == {}
        assert loan.paid_interest == {}

        assert balances_pre == balances_post

    asyncio.run(impl())
