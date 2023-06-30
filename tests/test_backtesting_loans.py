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

import pytest

from basana.backtesting import exchange, lending


def test_no_loans(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(backtesting_dispatcher, {})

        symbol = "USD"
        amount = Decimal(10000)
        assert not await e.can_lend(symbol, amount)
        with pytest.raises(Exception, match="Lending is not supported"):
            await e.create_loan(symbol, amount)

    asyncio.run(impl())


def test_unlimited_loans(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(backtesting_dispatcher, {})
        e._lending_strategy = lending.UnlimitedLoans()

        symbol = "USD"
        amount = Decimal(10000)
        assert await e.can_lend(symbol, amount)

        balance = await e.get_balance(symbol)
        assert balance.available == 0
        assert balance.borrowed == 0
        assert balance.total == 0

        loans = await e.get_loans()
        assert loans == []

        loan = await e.create_loan(symbol, amount)

        balance = await e.get_balance(symbol)
        assert balance.available == amount
        assert balance.borrowed == amount
        assert balance.total == amount

        loans = await e.get_loans()
        assert loans == [loan]

        await e.repay_loan(loan.id)
        with pytest.raises(Exception, match="Loan not found"):
            await e.repay_loan(loan.id)

        balance = await e.get_balance(symbol)
        assert balance.available == 0
        assert balance.borrowed == 0
        assert balance.total == 0

        loans = await e.get_loans()
        assert loans == []

    asyncio.run(impl())


def test_invalid_loan_amount(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(backtesting_dispatcher, {})
        e._lending_strategy = lending.UnlimitedLoans()

        with pytest.raises(Exception, match="Invalid amount"):
            await e.create_loan("USD", Decimal(-10000))

    asyncio.run(impl())
