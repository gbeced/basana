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
import abc

from basana.backtesting import errors, orders
from basana.core import helpers
from basana.core.enums import OrderOperation
from basana.core.pair import Pair, PairInfo


class ExchangeOrder(metaclass=abc.ABCMeta):
    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal,
            auto_borrow: bool = False, auto_repay: bool = False
    ):
        self._operation = operation
        self._pair = pair
        self._amount = amount
        self._auto_borrow = auto_borrow
        self._auto_repay = auto_repay

    @property
    def pair(self) -> Pair:
        return self._pair

    @property
    def amount(self) -> Decimal:
        return self._amount

    @property
    def operation(self) -> OrderOperation:
        return self._operation

    @property
    def auto_borrow(self) -> bool:
        return self._auto_borrow

    @property
    def auto_repay(self) -> bool:
        return self._auto_repay

    def validate(self, pair_info: PairInfo):
        if self.amount <= Decimal(0):
            raise errors.Error("Amount must be > 0")
        if self.amount != helpers.truncate_decimal(self.amount, pair_info.base_precision):
            raise errors.Error(
                "{} exceeds maximum precision of {} decimal digits".format(self.amount, pair_info.base_precision)
            )

    @abc.abstractmethod
    def create_order(self, id: str) -> orders.Order:
        raise NotImplementedError()


class MarketOrder(ExchangeOrder):
    """Market order request.

    A market order is an order to buy or sell a stock at the best available price.
    Generally, this type of order will be executed immediately. However, the price at which a market order will be
    executed is not guaranteed.
    """

    def validate(self, pair_info: PairInfo):
        super().validate(pair_info)

    def create_order(self, id: str) -> orders.Order:
        return orders.MarketOrder(
            id, self.operation, self.pair, self.amount, auto_borrow=self.auto_borrow,
            auto_repay=self.auto_repay
        )


class LimitOrder(ExchangeOrder):
    """Limit order request.

    A limit order is an order to buy or sell a stock at a specific price or better.
    A buy limit order can only be executed at the limit price or lower, and a sell limit order can only be executed
    at the limit price or higher.
    """

    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, limit_price: Decimal,
            auto_borrow: bool = False, auto_repay: bool = False
    ):
        super().__init__(operation, pair, amount, auto_borrow=auto_borrow, auto_repay=auto_repay)
        self._limit_price = limit_price

    @property
    def limit_price(self) -> Decimal:
        return self._limit_price

    def validate(self, pair_info: PairInfo):
        super().validate(pair_info)
        if self.limit_price <= Decimal(0):
            raise errors.Error("Limit price must be > 0")
        if self.limit_price != helpers.truncate_decimal(self.limit_price, pair_info.quote_precision):
            raise errors.Error(
                "{} exceeds maximum precision of {} decimal digits".format(self.limit_price, pair_info.quote_precision)
            )

    def create_order(self, id: str) -> orders.Order:
        return orders.LimitOrder(
            id, self.operation, self.pair, self.amount, self._limit_price,
            auto_borrow=self.auto_borrow, auto_repay=self.auto_repay
        )


class StopOrder(ExchangeOrder):
    """Stop order request.

    A stop order, also referred to as a stop-loss order, is an order to buy or sell a stock once the price of the
    stock reaches a specified price, known as the stop price.
    When the stop price is reached, a stop order becomes a market order.
    A buy stop order is entered at a stop price above the current market price. Investors generally use a buy stop
    order to limit a loss or to protect a profit on a stock that they have sold short.
    A sell stop order is entered at a stop price below the current market price. Investors generally use a sell
    stop order to limit a loss or to protect a profit on a stock that they own.
    """

    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal,
            auto_borrow: bool = False, auto_repay: bool = False
    ):
        super().__init__(operation, pair, amount, auto_borrow=auto_borrow, auto_repay=auto_repay)
        self._stop_price = stop_price

    @property
    def stop_price(self) -> Decimal:
        return self._stop_price

    def validate(self, pair_info: PairInfo):
        super().validate(pair_info)
        if self.stop_price <= Decimal(0):
            raise errors.Error("Stop price must be > 0")
        if self.stop_price != helpers.truncate_decimal(self.stop_price, pair_info.quote_precision):
            raise errors.Error(
                "{} exceeds maximum precision of {} decimal digits".format(self.stop_price, pair_info.quote_precision)
            )

    def create_order(self, id: str) -> orders.Order:
        return orders.StopOrder(
            id, self.operation, self.pair, self.amount, self._stop_price,
            auto_borrow=self.auto_borrow, auto_repay=self.auto_repay
        )


class StopLimitOrder(ExchangeOrder):
    """Stop limit order request.

    A stop-limit order is an order to buy or sell a stock that combines the features of a stop order and a limit
    order.
    Once the stop price is reached, a stop-limit order becomes a limit order that will be executed at a specified
    price (or better). The benefit of a stop-limit order is that the investor can control the price at which the
    order can be executed.
    """

    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal, limit_price: Decimal,
            auto_borrow: bool = False, auto_repay: bool = False
    ):
        super().__init__(operation, pair, amount, auto_borrow=auto_borrow, auto_repay=auto_repay)
        self._stop_price = stop_price
        self._limit_price = limit_price

    @property
    def stop_price(self) -> Decimal:
        return self._stop_price

    @property
    def limit_price(self) -> Decimal:
        return self._limit_price

    def validate(self, pair_info: PairInfo):
        super().validate(pair_info)
        if self.stop_price <= Decimal(0):
            raise errors.Error("Stop price must be > 0")
        if self.limit_price <= Decimal(0):
            raise errors.Error("Limit price must be > 0")
        for value in [self.stop_price, self.limit_price]:
            if value != helpers.truncate_decimal(value, pair_info.quote_precision):
                raise errors.Error(
                    "{} exceeds maximum precision of {} decimal digits".format(value, pair_info.quote_precision)
                )

    def create_order(self, id: str) -> orders.Order:
        return orders.StopLimitOrder(
            id, self.operation, self.pair, self.amount, self._stop_price, self._limit_price,
            auto_borrow=self.auto_borrow, auto_repay=self.auto_repay
        )
