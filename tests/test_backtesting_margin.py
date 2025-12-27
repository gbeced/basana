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
import datetime

import pytest

from basana.backtesting.lending import MarginLoanConditions, MarginLoans
from basana.backtesting.prices import Prices
from basana.backtesting.config import Config, SymbolInfo
from basana.backtesting.loan_mgr import LoanManager
from basana.backtesting.lending.base import ExchangeContext
from basana.backtesting.account_balances import AccountBalances
from basana.core.dispatcher import backtesting_dispatcher
from basana.core.pair import Pair, PairInfo
from basana.core.bar import Bar, BarEvent


@pytest.mark.parametrize(
    (
        "borrowed_symbol, borrowed_amount, loan_age, current_price, initial_balances,"
        # Loan conditions
        "interest_symbol, interest_percentage, interest_period, min_interest, margin_requirement,"
        # Expected results
        "expected_margin_level, expected_interest"
    ),
    [
        (
            "USDT", Decimal("1000"), datetime.timedelta(days=0), Decimal("1"),
            {"USDT": Decimal("1000")},
            "USDT", Decimal("0"), datetime.timedelta(days=30), Decimal("0"), Decimal("0.5"),

            Decimal("100"), Decimal("0")
        )
    ]
)
def test_loans(
    borrowed_symbol, borrowed_amount, loan_age, current_price, initial_balances,
    interest_symbol, interest_percentage, interest_period, min_interest, margin_requirement,
    expected_margin_level, expected_interest
):
    quote_symbol = "USDT"

    # Setup config and prices
    config = Config(
        default_symbol_info=SymbolInfo(precision=2),
        default_pair_info=PairInfo(base_precision=2, quote_precision=2)
    )
    pair = Pair(borrowed_symbol, quote_symbol)
    prices = Prices(bid_ask_spread_pct=Decimal('0.01'), config=config)

    # Set a bar for price conversion
    bar = Bar(
        begin=datetime.datetime.now(datetime.timezone.utc),
        pair=pair,
        open=current_price,
        high=current_price,
        low=current_price,
        close=current_price,
        volume=Decimal('1000'),
        duration=datetime.timedelta(days=1)
    )
    prices.on_bar_event(BarEvent(bar.datetime, bar))

    # Setup account balances
    account_balances = AccountBalances(initial_balances)

    # Setup dispatcher
    dispatcher = backtesting_dispatcher()
    dispatcher._set_now(bar.datetime)  # Set dispatcher time

    # Setup exchange context
    exchange_ctx = ExchangeContext(
        dispatcher=dispatcher,
        account_balances=account_balances,
        prices=prices,
        config=config
    )

    # Setup margin loan conditions and strategy
    loan_conditions = MarginLoanConditions(
        interest_symbol=interest_symbol,
        interest_percentage=interest_percentage,
        interest_period=interest_period,
        min_interest=min_interest
    )
    margin_loans = MarginLoans(quote_symbol, margin_requirement)
    margin_loans.set_conditions(borrowed_symbol, loan_conditions)

    # Setup loan manager
    loan_mgr = LoanManager(margin_loans, exchange_ctx)
    margin_loans.set_exchange_context(loan_mgr, exchange_ctx)

    # Create the loan
    loan_info = loan_mgr.create_loan(borrowed_symbol, borrowed_amount)

    # Check margin level and interest
    margin_level = margin_loans.margin_level
    # Get the actual loan object (not LoanInfo)
    loan_obj = None
    for loan in loan_mgr._loans.get_all():
        if loan.id == loan_info.id:
            loan_obj = loan
            break
    assert loan_obj is not None, 'Loan object not found'
    interest = loan_obj.calculate_interest(bar.datetime + loan_age, prices)
    # If interest is a dict, get the value for interest_symbol
    interest_value = interest.get(interest_symbol, Decimal('0'))

    assert margin_level == expected_margin_level
    assert interest_value == expected_interest
