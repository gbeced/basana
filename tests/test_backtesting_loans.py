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


def test_no_loans(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(backtesting_dispatcher, {})

        symbol = "USD"
        amount = Decimal(10000)
        with pytest.raises(Exception, match="Lending is not supported"):
            await e.create_loan(symbol, amount)

    asyncio.run(impl())


def test_borrow_and_repay_min_interest(backtesting_dispatcher):
    async def impl():
        min_interest = Decimal("0.001")
        symbol = "USD"
        lending_strategy = lending.BasicLoans(
            interest_rate=Decimal("0.00002"), interest_period=datetime.timedelta(hours=1),
            min_interest=min_interest
        )
        initial_balance = min_interest
        e = exchange.Exchange(backtesting_dispatcher, {symbol: initial_balance}, lending_strategy=lending_strategy)

        balance = await e.get_balance(symbol)
        assert balance.available == initial_balance
        assert balance.borrowed == 0
        assert balance.total == initial_balance

        loans = await e.get_open_loans()
        assert loans == []

        loan_amount = Decimal(10000)
        loan = await e.create_loan(symbol, loan_amount)

        balance = await e.get_balance(symbol)
        assert balance.available == initial_balance + loan_amount
        assert balance.borrowed == loan_amount
        assert balance.total == initial_balance

        loans = await e.get_open_loans()
        assert loans == [loan]

        assert await e.get_loan(loan.id) == loan

        await e.repay_loan(loan.id)

        balance = await e.get_balance(symbol)
        assert balance.available == 0
        assert balance.borrowed == 0
        assert balance.total == 0

        loans = await e.get_open_loans()
        assert loans == []

    asyncio.run(impl())


def test_borrow_and_repay(backtesting_dispatcher):
    async def impl():
        min_interest = Decimal("0.0001")
        symbol = "USD"
        interest_rate = Decimal("0.05")
        interest_period = datetime.timedelta(hours=1)
        lending_strategy = lending.BasicLoans(
            interest_rate=interest_rate, interest_period=interest_period, min_interest=min_interest
        )
        initial_balance = interest_rate
        e = exchange.Exchange(backtesting_dispatcher, {symbol: initial_balance}, lending_strategy=lending_strategy)

        balance = await e.get_balance(symbol)
        assert balance.available == initial_balance
        assert balance.borrowed == 0
        assert balance.total == initial_balance

        loans = await e.get_open_loans()
        assert loans == []

        loan_amount = Decimal(10000)
        loan = await e.create_loan(symbol, loan_amount)

        balance = await e.get_balance(symbol)
        assert balance.available == initial_balance + loan_amount
        assert balance.borrowed == loan_amount
        assert balance.total == initial_balance

        loans = await e.get_open_loans()
        assert loans == [loan]

        assert await e.get_loan(loan.id) == loan

        backtesting_dispatcher._last_dt = backtesting_dispatcher.now() + interest_period
        await e.repay_loan(loan.id)

        balance = await e.get_balance(symbol)
        assert balance.available == 0
        assert balance.borrowed == 0
        assert balance.total == 0

        loans = await e.get_open_loans()
        assert loans == []

    asyncio.run(impl())


def test_repay_inexistent(backtesting_dispatcher):
    async def impl():
        lending_strategy = lending.BasicLoans(
            interest_rate=Decimal("0"), interest_period=datetime.timedelta(days=365), min_interest=Decimal("0.001")
        )
        e = exchange.Exchange(backtesting_dispatcher, {}, lending_strategy=lending_strategy)

        with pytest.raises(Exception, match="Loan not found"):
            await e.repay_loan("inexistent")

    asyncio.run(impl())


def test_invalid_borrow_amount(backtesting_dispatcher):
    async def impl():
        lending_strategy = lending.BasicLoans(
            interest_rate=Decimal("0"), interest_period=datetime.timedelta(days=365), min_interest=Decimal("0.001")
        )
        e = exchange.Exchange(backtesting_dispatcher, {}, lending_strategy=lending_strategy)

        with pytest.raises(Exception, match="Invalid amount"):
            await e.create_loan("USD", Decimal(-10000))

    asyncio.run(impl())
