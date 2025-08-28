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
from typing import Dict, Optional
import dataclasses
import datetime
import uuid

from basana.backtesting import account_balances, errors, loan_mgr, prices
from basana.backtesting.lending import base
from basana.backtesting.value_map import ValueMap, ValueMapDict


@dataclasses.dataclass
class MarginLoanConditions:
    #: The symbol for the interest.
    interest_symbol: str
    #: The interest percentage.
    interest_percentage: Decimal
    #: The interest period.
    interest_period: datetime.timedelta
    #: The minimum interest to charge.
    min_interest: Decimal


class MarginLoan(base.Loan):
    def __init__(
            self, id: str, borrowed_symbol: str,  borrowed_amount: Decimal, created_at: datetime.datetime,
            conditions: MarginLoanConditions
    ):
        super().__init__(id, borrowed_symbol, borrowed_amount, created_at)
        self._conditions = conditions

    def calculate_interest(self, at: datetime.datetime, prices: prices.Prices) -> Dict[str, Decimal]:
        assert at >= self._created_at

        interest = self._conditions.interest_percentage / Decimal(100) * self.borrowed_amount
        if self._conditions.interest_period:
            time_ellapsed = at - self._created_at
            interest *= Decimal(time_ellapsed.total_seconds() / self._conditions.interest_period.total_seconds())

        # Currency conversion if interest symbol is different from borrowed symbol.
        if self._conditions.interest_symbol != self.borrowed_symbol:
            interest = prices.convert(interest, self._borrowed_symbol, self._conditions.interest_symbol)

        interest = max(interest, self._conditions.min_interest)
        return {self._conditions.interest_symbol: interest}

    def calculate_collateral(self, prices: prices.Prices) -> Dict[str, Decimal]:
        # Collateral will be managed through CheckMarginLevel at the account level.
        return {}


class MarginLoans(base.LendingStrategy):
    """
    This strategy will use the accounts assets as collateral for the loans.

    :param quote_symbol: The symbol to use to normalize balances.
    :param margin_requirement: Minimum threshold for the value of the collateral relative to the total position.
    :param default_conditions: The default margin loan conditions.
    """
    def __init__(
            self, quote_symbol: str, margin_requirement: Decimal,
            default_conditions: Optional[MarginLoanConditions] = None
    ):
        assert margin_requirement > 0, "Margin requirement must be greater than zero"

        self._quote_symbol = quote_symbol
        self._margin_requirement = margin_requirement
        self._conditions: Dict[str, MarginLoanConditions] = {}
        self._default_conditions = default_conditions
        self._loan_mgr: Optional[loan_mgr.LoanManager] = None
        self._exchange_ctx: Optional[base.ExchangeContext] = None

    def set_conditions(self, symbol: str, conditions: MarginLoanConditions):
        """
        Set the lending conditions for a given symbol.

        :param symbol: The symbol whose conditions are being set.
        :param conditions: The lending conditions.
        """
        self._conditions[symbol] = conditions

    def get_conditions(self, symbol: str) -> MarginLoanConditions:
        """
        Returns the lending conditions for a given symbol.

        :param symbol: The symbol.
        """
        conditions = self._conditions.get(symbol, self._default_conditions)
        if not conditions:
            raise errors.Error(f"No lending conditions for {symbol}")
        return conditions

    def set_exchange_context(self, loan_mgr: loan_mgr.LoanManager, exchange_context: base.ExchangeContext):
        self._loan_mgr = loan_mgr
        self._exchange_ctx = exchange_context
        self._exchange_ctx.account_balances.push_update_rule(CheckMarginLevel(self))

    def create_loan(self, symbol: str, amount: Decimal, created_at: datetime.datetime) -> base.Loan:
        conditions = self.get_conditions(symbol)
        return MarginLoan(uuid.uuid4().hex, symbol, amount, created_at, conditions)

    @property
    def margin_level(self) -> Decimal:
        """
        The current margin level.
        """
        assert self._exchange_ctx, "Not yet connected with the exchange"
        acc_balances = self._exchange_ctx.account_balances
        return self.calculate_margin_level(
            acc_balances.balances, acc_balances.holds, acc_balances.borrowed
        )

    def calculate_margin_level(
            self, updated_balances: ValueMapDict, updated_holds: ValueMapDict, updated_borrowed: ValueMapDict
    ) -> Decimal:
        assert self._exchange_ctx and self._loan_mgr, "Not yet connected with the exchange"

        # If we haven't borrowed anything yet, the margin level is infinite.
        if all(v == Decimal(0) for v in updated_borrowed.values()):
            # used_margin = 0,  margin_level = Infinity
            return Decimal("Infinity")

        # Calculate outstanding interest.
        outstanding_interest = ValueMap()
        for loan in self._loan_mgr.get_loans(is_open=True):
            outstanding_interest += loan.outstanding_interest
        outstanding_interest = self._exchange_ctx.prices.convert_value_map(outstanding_interest, self._quote_symbol)

        # Calculate margin level.
        borrowed = self._exchange_ctx.prices.convert_value_map(updated_borrowed, self._quote_symbol)
        total_position_size = self._exchange_ctx.prices.convert_value_map(updated_balances, self._quote_symbol)
        total_position_size -= outstanding_interest
        equity = total_position_size - borrowed
        used_margin = Decimal(sum(total_position_size.values())) * self._margin_requirement
        margin_level = Decimal(sum(equity.values())) / used_margin * Decimal(100)
        return margin_level


class CheckMarginLevel(account_balances.UpdateRule):
    def __init__(self, margin_loans: MarginLoans):
        self._margin_loans = margin_loans
        self._threshold = Decimal(100)

    def check(
            self, updated_balances: ValueMap, updated_holds: ValueMap, updated_borrowed: ValueMap,
            delta_balances: ValueMap, delta_holds: ValueMap, delta_borrowed: ValueMap
    ):
        # If we're increasing any borrowed amount we need to check the margin level.
        if any(v > 0 for v in delta_borrowed.values()):
            margin_level = self._margin_loans.calculate_margin_level(updated_balances, updated_holds, updated_borrowed)
            if margin_level < self._threshold:
                raise errors.NotEnoughBalance(f"Margin level too low {margin_level}")
