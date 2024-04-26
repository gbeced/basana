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
import copy
import dataclasses
import datetime
import logging
import uuid

from basana.backtesting import errors, prices


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


class LoanFactory(metaclass=abc.ABCMeta):
    """
    Base class for loan factories.
    """

    @abc.abstractmethod
    def create_loan(self, symbol: str, amount: Decimal, created_at: datetime.datetime) -> Loan:
        raise NotImplementedError()


class NoLoans(LoanFactory):
    """
    Lending is not supported.
    """

    def create_loan(self, symbol: str, amount: Decimal, created_at: datetime.datetime) -> Loan:
        raise errors.Error("Lending is not supported")


@dataclasses.dataclass
class LoanConditions:
    interest_symbol: str
    interest_rate: Decimal
    interest_period: datetime.timedelta
    min_interest: Decimal
    collateral_symbol: str
    # Minimum threshold for the value of the collateral relative to the loan amount.
    margin_requirement: Decimal


class CollateralizedInterestLoan(Loan):
    def __init__(
            self, id: str, borrowed_symbol: str,  borrowed_amount: Decimal, created_at: datetime.datetime,
            conditions: LoanConditions
    ):
        super().__init__(id, borrowed_symbol, borrowed_amount, created_at)
        self._conditions = conditions

    def calculate_interest(self, at: datetime.datetime, prices: prices.Prices) -> Dict[str, Decimal]:
        assert at > self._created_at
        time_ellapsed = at - self._created_at
        interest = self._conditions.interest_rate * self.borrowed_amount * \
            Decimal(time_ellapsed / self._conditions.interest_period)
        interest = max(interest, self._conditions.min_interest)

        if self._conditions.interest_symbol != self.borrowed_symbol:
            interest = prices.convert(interest, self._borrowed_symbol, self._conditions.interest_symbol)

        return {self._conditions.interest_symbol: interest}

    def calculate_collateral(self, prices: prices.Prices) -> Dict[str, Decimal]:
        return {}


class CollateralizedInterestLoans(LoanFactory):
    """
    Loan with interest and collateral.

    :param conditions_by_symbol: Loan conditions by symbol.
    :param default_conditions: Optional default conditions if the symbol was not found in conditions.
    """
    def __init__(
            self, conditions_by_symbol: Dict[str, LoanConditions] = {},
            default_conditions: Optional[LoanConditions] = None
    ):
        self._conditions = copy.copy(conditions_by_symbol)
        self._default_conditions = default_conditions

    def create_loan(self, symbol: str, amount: Decimal, created_at: datetime.datetime) -> Loan:
        conditions = self._conditions.get(symbol, self._default_conditions)
        if not conditions:
            raise errors.Error(f"No lending conditions for {symbol}")
        return CollateralizedInterestLoan(uuid.uuid4().hex, symbol, amount, created_at, conditions)
