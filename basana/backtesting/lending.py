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
import abc
import dataclasses
import uuid


@dataclasses.dataclass
class Loan:
    #: The loan id.
    id: str
    #: The symbol being borrowed.
    symbol: str
    #: The loan amount.
    amount: Decimal
    #: True if the loan is open, False otherwise.
    is_open: bool


class LendingStrategy(metaclass=abc.ABCMeta):
    """Base class for strategies that model lending schemes.

    .. note::

        * This is a base class and should not be used directly.
    """

    @abc.abstractmethod
    def can_lend(self, symbol: str, amount: Decimal) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def create_loan(self, symbol: str, amount: Decimal) -> Loan:
        raise NotImplementedError()


class NoLoans(LendingStrategy):
    """No loans."""

    def can_lend(self, symbol: str, amount: Decimal) -> bool:
        return False

    def create_loan(self, symbol: str, amount: Decimal) -> Loan:
        raise Exception("Lending is not supported")


class UnlimitedLoans(LendingStrategy):
    """Unlimited loans."""

    def can_lend(self, symbol: str, amount: Decimal) -> bool:
        return True

    def create_loan(self, symbol: str, amount: Decimal) -> Loan:
        return Loan(id=uuid.uuid4().hex, symbol=symbol, amount=amount, is_open=True)
