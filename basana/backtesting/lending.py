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
from typing import Dict, Optional
import abc
import dataclasses
import datetime
import logging
import uuid

from basana.backtesting import account_balances, config, errors, prices
from basana.backtesting.value_map import ValueMap


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class LoanInfo:
    #: The loan id.
    id: str
    #: True if the loan is open, False otherwise.
    is_open: bool
    #: The symbol being borrowed.
    borrowed_symbol: str
    #: The amount being borrowed.
    borrowed_amount: Decimal


class Loan(metaclass=abc.ABCMeta):
    def __init__(
            self, id: str, borrowed_symbol: str,  borrowed_amount: Decimal, created_at: datetime.datetime
    ):
        assert borrowed_amount > Decimal(0), f"Invalid amount {borrowed_amount}"

        self._id = id
        self._borrowed_symbol = borrowed_symbol
        self._borrowed_amount = borrowed_amount
        self._is_open = True
        self._created_at = created_at

    def get_loan_info(self) -> LoanInfo:
        return LoanInfo(
            id=self._id, is_open=self._is_open, borrowed_symbol=self._borrowed_symbol,
            borrowed_amount=self._borrowed_amount
        )

    @property
    def id(self) -> str:
        return self._id

    @property
    def is_open(self) -> bool:
        return self._is_open

    @property
    def borrowed_symbol(self) -> str:
        return self._borrowed_symbol

    @property
    def borrowed_amount(self) -> Decimal:
        return self._borrowed_amount

    def close(self):
        assert self._is_open
        self._is_open = False

    @abc.abstractmethod
    def calculate_interest(self, at: datetime.datetime, prices: prices.Prices) -> Dict[str, Decimal]:
        raise NotImplementedError()

    @abc.abstractmethod
    def calculate_collateral(self, prices: prices.Prices) -> Dict[str, Decimal]:
        raise NotImplementedError()


class LendingStrategy(metaclass=abc.ABCMeta):
    """
    Base class for lending strategies.
    """

    def set_exchange_ctx(
            self, account_balances: account_balances.AccountBalances, prices: prices.Prices, config: config.Config
    ):
        """
        This method will be called by the exchange during initialization to give lending strategies a chance to later
        use those services.
        """
        pass

    @abc.abstractmethod
    def create_loan(self, symbol: str, amount: Decimal, created_at: datetime.datetime) -> Loan:
        raise NotImplementedError()


class NoLoans(LendingStrategy):
    """
    Lending not supported.
    """

    def create_loan(self, symbol: str, amount: Decimal, created_at: datetime.datetime) -> Loan:
        raise errors.Error("Lending is not supported")


@dataclasses.dataclass
class MarginLoanConditions:
    interest_symbol: str
    interest_rate: Decimal
    interest_period: datetime.timedelta
    min_interest: Decimal
    # Minimum threshold for the value of the collateral relative to the loan amount.
    margin_requirement: Decimal


class MarginLoan(Loan):
    def __init__(
            self, id: str, borrowed_symbol: str,  borrowed_amount: Decimal, created_at: datetime.datetime,
            conditions: MarginLoanConditions
    ):
        super().__init__(id, borrowed_symbol, borrowed_amount, created_at)
        self._conditions = conditions

    def calculate_interest(self, at: datetime.datetime, prices: prices.Prices) -> Dict[str, Decimal]:
        assert at > self._created_at

        interest = self._conditions.interest_rate * self.borrowed_amount
        if self._conditions.interest_period:
            time_ellapsed = at - self._created_at
            interest *= Decimal(time_ellapsed / self._conditions.interest_period)
        interest = max(interest, self._conditions.min_interest)

        # Currency conversion if interest symbol is different from borrowed symbol.
        if self._conditions.interest_symbol != self.borrowed_symbol:
            interest = prices.convert(interest, self._borrowed_symbol, self._conditions.interest_symbol)

        return {self._conditions.interest_symbol: interest}

    def calculate_collateral(self, prices: prices.Prices) -> Dict[str, Decimal]:
        # Collateral will be managed through CheckMarginLevel at the account level.
        return {}


class MarginLoans(LendingStrategy):
    """
    This strategy will use the accounts assets as collateral for the loans.

    :param quote_symbol:
    :param default_conditions:
    """
    def __init__(self, quote_symbol: str, default_conditions: Optional[MarginLoanConditions] = None):
        self._quote_symbol = quote_symbol
        self._conditions: Dict[str, MarginLoanConditions] = {}
        self._default_conditions = default_conditions
        self._account_balances: Optional[account_balances.AccountBalances] = None
        self._prices: Optional[prices.Prices] = None
        self._config: Optional[config.Config] = None

    def set_conditions(self, symbol: str, conditions: MarginLoanConditions):
        self._conditions[symbol] = conditions

    def get_conditions(self, symbol: str) -> MarginLoanConditions:
        conditions = self._conditions.get(symbol, self._default_conditions)
        if not conditions:
            raise errors.Error(f"No lending conditions for {symbol}")
        return conditions

    def set_exchange_ctx(
            self, account_balances: account_balances.AccountBalances, prices: prices.Prices, config: config.Config
    ):
        self._account_balances = account_balances
        self._prices = prices
        self._config = config
        self._account_balances.push_update_rule(CheckMarginLevel(self))

    def create_loan(self, symbol: str, amount: Decimal, created_at: datetime.datetime) -> Loan:
        conditions = self.get_conditions(symbol)
        return MarginLoan(uuid.uuid4().hex, symbol, amount, created_at, conditions)

    @property
    def margin_level(self) -> Decimal:
        assert self._account_balances, "Not yet connected with the exchange"
        return self._calculate_margin_level(
            self._account_balances.balances, self._account_balances.holds, self._account_balances.borrowed
        )

    def _calculate_margin_level(
            self, updated_balances: ValueMap, updated_holds: ValueMap, updated_borrowed: ValueMap
    ) -> Decimal:
        assert self._prices, "Not yet connected with the exchange"

        # Calculate equity.
        equity = Decimal(0)
        for symbol, balance in updated_balances.items():
            borrowed = updated_borrowed.get(symbol, Decimal(0))
            net = balance - borrowed
            if net <= Decimal(0):
                continue

            if symbol != self._quote_symbol:
                net = self._prices.convert(net, symbol, self._quote_symbol)

            equity += net

        # Calculate used margin.
        used_margin = Decimal(0)
        for symbol, borrowed in updated_borrowed.items():
            margin = borrowed * self.get_conditions(symbol).margin_requirement
            if symbol != self._quote_symbol:
                margin = self._prices.convert(margin, symbol, self._quote_symbol)

            used_margin += margin

        if used_margin == Decimal(0):
            return Decimal(0)

        return equity / used_margin * Decimal(100)

    def _check_margin_level(self, updated_balances: ValueMap, updated_holds: ValueMap, updated_borrowed: ValueMap):
        margin_level = self._calculate_margin_level(updated_balances, updated_holds, updated_borrowed)
        if margin_level > Decimal(0) and margin_level < Decimal(100):
            raise errors.NotEnoughBalance(f"Margin level too low {margin_level}")


class CheckMarginLevel(account_balances.UpdateRule):
    def __init__(self, margin_loans: MarginLoans):
        self._margin_loans = margin_loans

    def check(self, updated_balances: ValueMap, updated_holds: ValueMap, updated_borrowed: ValueMap):
        self._margin_loans._check_margin_level(updated_balances, updated_holds, updated_borrowed)
