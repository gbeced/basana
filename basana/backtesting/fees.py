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
import enum

from . import orders


class FeeCurrency(enum.Enum):
    """The currency in which fees are charged."""

    #: Fees are charged in quote currency.
    QUOTE = "quote"
    #: Fees are charged in base currency.
    BASE = "base"


class FeeStrategy(metaclass=abc.ABCMeta):
    """Base class for strategies that model fee schemes.

    .. note::

        * This is a base class and should not be used directly.
    """

    @abc.abstractmethod
    def calculate_fees(
            self, order: orders.Order, balance_updates: Dict[str, Decimal]
    ) -> Dict[str, Decimal]:
        raise NotImplementedError()


class NoFee(FeeStrategy):
    """This strategy applies no fees to the trades."""

    def calculate_fees(self, order: orders.Order, balance_updates: Dict[str, Decimal]) -> Dict[str, Decimal]:
        return {}


class Percentage(FeeStrategy):
    """This strategy applies a fixed percentage per trade.

    :param percentage: The percentage to apply.
    :param min_fee: Minimum fee amount, in the fee currency.
    :param fee_currency: The currency in which fees are charged. Defaults to :attr:`FeeCurrency.QUOTE`.
    """

    def __init__(
            self, percentage: Decimal, min_fee: Decimal = Decimal(0),
            fee_currency: FeeCurrency = FeeCurrency.QUOTE
    ):
        assert percentage >= 0 and percentage < 100, f"Invalid percentage {percentage}"
        assert min_fee >= 0, f"Minimum fee cannot be negative {min_fee}"
        self._percentage = percentage
        self._min_fee = min_fee
        self._fee_currency = fee_currency

    def calculate_fees(self, order: orders.Order, balance_updates: Dict[str, Decimal]) -> Dict[str, Decimal]:
        ret = {}

        if self._fee_currency == FeeCurrency.BASE:
            symbol = order.pair.base_symbol
        else:
            symbol = order.pair.quote_symbol

        # Rounding may have taken place in previous fills, so fees may have been overcharged. For that reason we
        # calculate the total fees to charge, and subtract what we have charged so far.
        charged_fee_amount = order.fees.get(symbol, Decimal(0))
        assert charged_fee_amount <= Decimal(0), "Fees should always be negative"
        total_amount = order.balance_updates.get(symbol, Decimal(0)) + balance_updates.get(symbol, Decimal(0))
        total_fee_amount = -max(abs(total_amount) * self._percentage / Decimal(100), self._min_fee)
        pending_fee = total_fee_amount - charged_fee_amount
        if pending_fee < Decimal(0):
            ret[symbol] = pending_fee

        return ret
