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
from typing import Dict
import abc
import dataclasses
import datetime

from basana.backtesting import account_balances, config, errors, prices
from basana.backtesting.value_map import ValueMap, ValueMapDict
from basana.core import dispatcher


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
    #: The outstanding interest. Only valid for open loans.
    outstanding_interest: Dict[str, Decimal]
    #: The paid interest. Only valid for closed loans.
    paid_interest: Dict[str, Decimal]


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
        self._paid_interest = ValueMap()

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

    @property
    def created_at(self) -> datetime.datetime:
        return self._created_at

    @property
    def paid_interest(self) -> ValueMapDict:
        return self._paid_interest

    def close(self):
        assert self._is_open
        self._is_open = False

    def add_paid_interest(self, interest: ValueMapDict):
        self._paid_interest += interest

    @abc.abstractmethod
    def calculate_interest(self, at: datetime.datetime, prices: prices.Prices) -> ValueMapDict:
        raise NotImplementedError()

    @abc.abstractmethod
    def calculate_collateral(self, prices: prices.Prices) -> ValueMapDict:
        raise NotImplementedError()


@dataclasses.dataclass
class ExchangeContext:
    dispatcher: dispatcher.BacktestingDispatcher
    account_balances: account_balances.AccountBalances
    prices: prices.Prices
    config: config.Config


class LendingStrategy(metaclass=abc.ABCMeta):
    """
    Base class for lending strategies.
    """

    def set_exchange_context(self, loan_mgr, exchange_context: ExchangeContext):
        """
        This method will be called during exchange initialization to give lending strategies a chance to later
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
