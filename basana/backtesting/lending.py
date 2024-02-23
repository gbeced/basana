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
from typing import Dict
import abc
import dataclasses
import datetime
import uuid


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
            self, id: str, borrowed_symbol: str,  borrowed_amount: Decimal,
            required_collateral: Dict[str, Decimal], created_at: datetime.datetime
    ):
        assert borrowed_amount > Decimal(0), f"Invalid amount {borrowed_amount}"

        self._id = id
        self._borrowed_symbol = borrowed_symbol
        self._borrowed_amount = borrowed_amount
        self._is_open = True
        self._required_collateral = required_collateral
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

    @property
    def required_collateral(self) -> Dict[str, Decimal]:
        return self._required_collateral

    @abc.abstractmethod
    def calculate_interest(self, at: datetime.datetime) -> Dict[str, Decimal]:
        raise NotImplementedError()

    def close(self):
        assert self._is_open
        self._is_open = False


class LendingStrategy(metaclass=abc.ABCMeta):
    """Base class for strategies that model lending schemes.

    .. note::

        * This is a base class and should not be used directly.
    """

    @abc.abstractmethod
    def create_loan(self, symbol: str, amount: Decimal, created_at: datetime.datetime) -> Loan:
        raise NotImplementedError()


class NoLoans(LendingStrategy):
    """No loans."""

    def create_loan(self, symbol: str, amount: Decimal, created_at: datetime.datetime) -> Loan:
        raise Exception("Lending is not supported")


class BasicLoan(Loan):
    def __init__(
            self, id: str, borrowed_symbol: str,  borrowed_amount: Decimal,
            required_collateral: Dict[str, Decimal], created_at: datetime.datetime,
            interest_rate: Decimal, interest_period: datetime.timedelta, min_interest: Decimal
    ):
        super().__init__(id, borrowed_symbol, borrowed_amount, required_collateral, created_at)
        self._interest_rate = interest_rate
        self._interest_period = interest_period
        self._min_interest = min_interest

    def calculate_interest(self, at: datetime.datetime) -> Dict[str, Decimal]:
        assert at > self._created_at
        time_ellapsed = at - self._created_at
        interest = self._interest_rate * Decimal(time_ellapsed / self._interest_period)
        interest = max(interest, self._min_interest)
        return {self.borrowed_symbol: interest}


class BasicLoans(LendingStrategy):
    def __init__(self, interest_rate: Decimal, interest_period: datetime.timedelta, min_interest: Decimal):
        self._interest_rate = interest_rate
        self._interest_period = interest_period
        self._min_interest = min_interest

    def create_loan(self, symbol: str, amount: Decimal, created_at: datetime.datetime) -> Loan:
        return BasicLoan(
            uuid.uuid4().hex, symbol, amount, {}, created_at, self._interest_rate, self._interest_period,
            self._min_interest
        )
