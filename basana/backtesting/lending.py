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
from typing import Dict, List, Optional
import abc
import dataclasses
import datetime
import uuid
import logging

from basana.backtesting import config, errors
from basana.backtesting import helpers as bt_helpers
from basana.backtesting.account_balances import AccountBalances
from basana.core import logs
import basana.core.helpers as core_helpers


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


class LoanManager:
    def __init__(self, lending_strategy: LendingStrategy, account_balances: AccountBalances, config: config.Config):
        self._loans = bt_helpers.ExchangeObjectContainer[Loan]()
        self._lending_strategy = lending_strategy
        self._balances = account_balances
        self._config = config

    def create_loan(self, symbol: str, amount: Decimal, now: datetime.datetime) -> LoanInfo:
        if amount <= 0:
            raise errors.Error("Invalid amount")

        loan = self._lending_strategy.create_loan(symbol, amount, now)

        self._balances.update(
            balance_updates={loan.borrowed_symbol: loan.borrowed_amount},
            borrowed_updates={loan.borrowed_symbol: loan.borrowed_amount},
            hold_updates=loan.required_collateral
        )

        self._loans.add(loan)

        return loan.get_loan_info()

    def get_open_loans(self) -> List[LoanInfo]:
        return list(map(lambda loan: loan.get_loan_info(), self._loans.get_open()))

    def get_loan(self, loan_id: str) -> Optional[LoanInfo]:
        loan = self._loans.get(loan_id)
        return None if loan is None else loan.get_loan_info()

    def repay_loan(self, loan_id: str, now: datetime.datetime):
        loan = self._loans.get(loan_id)
        if not loan:
            raise errors.Error("Loan not found")
        if not loan.is_open:
            raise errors.Error("Loan is not open")

        interest = loan.calculate_interest(now)
        for symbol, amount in interest.items():
            interest[symbol] = core_helpers.truncate_decimal(amount, self._config.get_symbol_info(symbol).precision)

        try:
            self._balances.update(
                balance_updates=bt_helpers.sub_amounts(
                    {loan.borrowed_symbol: -loan.borrowed_amount}, interest
                ),
                borrowed_updates={loan.borrowed_symbol: -loan.borrowed_amount},
                hold_updates={symbol: -amount for symbol, amount in loan.required_collateral.items()}
            )
        except errors.NotEnoughBalance as e:
            logger.debug(
                logs.StructuredMessage("NotEnoughBalance", symbol=symbol, short=e.balance_short, loan_id=loan_id)
            )
            raise

        loan.close()
