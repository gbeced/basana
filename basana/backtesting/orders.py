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
import enum
import logging

from basana.backtesting import helpers, liquidity
from basana.core import bar, logs
from basana.core.enums import OrderOperation
from basana.core.pair import Pair


logger = logging.getLogger(__name__)


@enum.unique
class OrderState(enum.Enum):
    OPEN = 100
    COMPLETED = 101
    CANCELED = 102


@dataclasses.dataclass
class OrderInfo:
    #: The order id.
    id: str
    #: True if the order is open, False otherwise.
    is_open: bool
    #: The amount filled.
    amount_filled: Decimal
    #: The amount remaining.
    amount_remaining: Decimal
    #: The quote amount filled.
    quote_amount_filled: Decimal
    #: The fees charged.
    fees: Dict[str, Decimal]

    @property
    def fill_price(self) -> Optional[Decimal]:
        """The fill price."""
        fill_price = None
        if self.amount_filled:
            fill_price = self.quote_amount_filled / self.amount_filled
        return fill_price


@dataclasses.dataclass
class Fill:
    when: datetime.datetime
    balance_updates: Dict[str, Decimal]
    fees: Dict[str, Decimal]


# This is an internal abstraction to be used by the exchange.
class Order:
    def __init__(self, id: str, operation: OrderOperation, pair: Pair, amount: Decimal, state: OrderState):
        assert amount > Decimal(0), f"Invalid amount {amount}"

        self._id = id
        self._operation = operation
        self._pair = pair
        self._amount = amount
        self._state = state
        self._balance_updates: Dict[str, Decimal] = {}
        self._fees: Dict[str, Decimal] = {}
        self._fills: List[Fill] = []

    @property
    def id(self) -> str:
        return self._id

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
    def state(self) -> OrderState:
        return self._state

    @property
    def is_open(self) -> bool:
        return self._state == OrderState.OPEN

    @property
    def balance_updates(self) -> Dict[str, Decimal]:
        return self._balance_updates

    @property
    def fees(self) -> Dict[str, Decimal]:
        return self._fees

    @property
    def amount_filled(self) -> Decimal:
        return abs(self._balance_updates.get(self.pair.base_symbol, Decimal(0)))

    @property
    def amount_pending(self) -> Decimal:
        return self._amount - self.amount_filled

    @property
    def quote_amount_filled(self) -> Decimal:
        return abs(self._balance_updates.get(self.pair.quote_symbol, Decimal(0)))

    @property
    def fills(self) -> List[Fill]:
        return self._fills

    def cancel(self):
        assert self._state == OrderState.OPEN
        self._state = OrderState.CANCELED

    def add_fill(self, when: datetime.datetime, balance_updates: Dict[str, Decimal], fees: Dict[str, Decimal]):
        self._balance_updates = helpers.add_amounts(self._balance_updates, balance_updates)
        self._fees = helpers.add_amounts(self._fees, fees)
        if self.amount_filled >= self.amount:
            self._state = OrderState.COMPLETED
        self._fills.append(Fill(when=when, balance_updates=balance_updates, fees=fees))

    def get_order_info(self) -> OrderInfo:
        return OrderInfo(
            id=self.id, is_open=self._state == OrderState.OPEN, amount_filled=self.amount_filled,
            amount_remaining=self.amount_pending, quote_amount_filled=self.quote_amount_filled,
            fees={symbol: -amount for symbol, amount in self._fees.items() if amount}
        )

    @abc.abstractmethod
    def get_balance_updates(
            self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy
    ) -> Dict[str, Decimal]:
        """Returns the balance updates required to fill the order.

        :param bar: The bar that summarizes the trading activity.
        :param liquidity_strategy: The strategy used to model available liquidity.
        :returns: A dictionary that maps the symbol to the amount.

        .. note::

            * It can be either a complete or partial fill, based on the trading activity summarized by the bar and the
              available liquidity.
            * It should include both the base amount and the quote amount, with opposite signs depending on the
              operation.
        """
        raise NotImplementedError()

    def not_filled(self):
        """Called every time the order was processed but no fill took place."""
        pass


class MarketOrder(Order):
    def __init__(
            self, id: str, operation: OrderOperation, pair: Pair, amount: Decimal, state: OrderState
    ):
        super().__init__(id, operation, pair, amount, state)

    def not_filled(self):
        # Fill or kill market orders.
        self.cancel()

    def get_balance_updates(self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy) -> Dict[str, Decimal]:
        # No partial fills for market orders.
        if self.amount_pending > liquidity_strategy.available_liquidity:
            logger.debug(logs.StructuredMessage("Not enough liquidity to fill order", order_id=self.id))
            return {}

        amount = self.amount_pending
        base_sign = helpers.get_base_sign_for_operation(self.operation)
        if self.operation == OrderOperation.BUY:
            price = slipped_price(bar.open, self.operation, amount, liquidity_strategy, cap_high=bar.high)
        else:
            assert self.operation == OrderOperation.SELL
            price = slipped_price(bar.open, self.operation, amount, liquidity_strategy, cap_low=bar.low)

        return {
            self.pair.base_symbol: amount * base_sign,
            self.pair.quote_symbol: price * amount * -base_sign
        }


class LimitOrder(Order):
    def __init__(
            self, id: str, operation: OrderOperation, pair: Pair, amount: Decimal, limit_price: Decimal,
            state: OrderState
    ):
        assert limit_price > Decimal(0), "Invalid limit_price {limit_price}"

        super().__init__(id, operation, pair, amount, state)
        self._limit_price = limit_price

    def get_balance_updates(self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy) -> Dict[str, Decimal]:
        price = None
        amount = min(self.amount_pending, liquidity_strategy.available_liquidity)
        base_sign = helpers.get_base_sign_for_operation(self.operation)

        if self.operation == OrderOperation.BUY:
            # Limit price was hit at bar open.
            if bar.open < self._limit_price:
                price = slipped_price(bar.open, self.operation, amount, liquidity_strategy, cap_high=self._limit_price)
            # The price went down to limit price or lower.
            elif bar.low <= self._limit_price:
                price = self._limit_price
        else:
            assert self.operation == OrderOperation.SELL
            # Limit price was hit at bar open.
            if bar.open > self._limit_price:
                price = slipped_price(bar.open, self.operation, amount, liquidity_strategy, cap_low=self._limit_price)
            # The price went up to limit price or higher.
            elif bar.high >= self._limit_price:
                price = self._limit_price

        ret = {}
        if price:
            ret = {
                self.pair.base_symbol: amount * base_sign,
                self.pair.quote_symbol: price * amount * -base_sign
            }
        return ret


class StopOrder(Order):
    def __init__(
            self, id: str, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal,
            state: OrderState
    ):
        assert stop_price > Decimal(0), "Invalid stop_price {stop_price}"

        super().__init__(id, operation, pair, amount, state)
        self._stop_price = stop_price

    def not_filled(self):
        # Fill or kill stop orders.
        self.cancel()

    def get_balance_updates(self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy) -> Dict[str, Decimal]:
        # No partial fills for stop orders.
        if self.amount_pending > liquidity_strategy.available_liquidity:
            logger.debug(logs.StructuredMessage("Not enough liquidity to fill order", order_id=self.id))
            return {}

        price = None
        amount = self.amount_pending
        base_sign = helpers.get_base_sign_for_operation(self.operation)
        if self.operation == OrderOperation.BUY:
            # Stop price was hit at bar open.
            if bar.open >= self._stop_price:
                price = bar.open
            # The price went up to stop price or higher.
            elif bar.high >= self._stop_price:
                price = self._stop_price

            if price:
                price = slipped_price(price, self.operation, amount, liquidity_strategy, cap_high=bar.high)
        else:
            assert self.operation == OrderOperation.SELL
            # Stop price was hit at bar open.
            if bar.open <= self._stop_price:
                price = bar.open
            # The price went down to stop price or lower.
            elif bar.low <= self._stop_price:
                price = self._stop_price

            if price:
                price = slipped_price(price, self.operation, amount, liquidity_strategy, cap_low=bar.low)

        ret = {}
        if price:
            ret = {
                self.pair.base_symbol: amount * base_sign,
                self.pair.quote_symbol: price * amount * -base_sign
            }
        return ret


class StopLimitOrder(Order):
    def __init__(
            self, id: str, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal,
            limit_price: Decimal, state: OrderState
    ):
        assert stop_price > Decimal(0), "Invalid stop_price {stop_price}"
        assert limit_price > Decimal(0), "Invalid limit_price {limit_price}"

        super().__init__(id, operation, pair, amount, state)
        self._stop_price = stop_price
        self._limit_price = limit_price
        self._stop_price_hit = False

    def get_balance_updates_before_stop_hit(
            self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy
    ) -> Dict[str, Decimal]:
        assert not self._stop_price_hit

        price = None
        amount = min(self.amount_pending, liquidity_strategy.available_liquidity)
        base_sign = helpers.get_base_sign_for_operation(self.operation)

        if self.operation == OrderOperation.BUY:
            # Stop price was hit at bar open.
            if bar.open >= self._stop_price:
                self._stop_price_hit = True
                # Limit price was also hit at open.
                if bar.open <= self._limit_price:
                    price = bar.open
                # Limit price was hit some time later within the bar.
                elif bar.low <= self._limit_price <= bar.high:
                    price = self._limit_price
            # The price went up to stop price or higher.
            elif bar.high >= self._stop_price:
                self._stop_price_hit = True
                # Limit price was hit some time later within the bar.
                if bar.low <= self._limit_price <= bar.high:
                    price = self._limit_price
            # Calculate slippage if necessary.
            if price is not None and price != self._limit_price:
                price = slipped_price(price, self.operation, amount, liquidity_strategy, cap_high=self._limit_price)
        else:
            assert self.operation == OrderOperation.SELL
            # Stop price was hit at bar open.
            if bar.open <= self._stop_price:
                self._stop_price_hit = True
                # Limit price was also hit at open.
                if bar.open >= self._limit_price:
                    price = bar.open
                # Limit price was hit some time later within the bar.
                elif bar.low <= self._limit_price <= bar.high:
                    price = self._limit_price
            # The price went down to stop price or lower.
            elif bar.low <= self._stop_price:
                self._stop_price_hit = True
                # Limit price was hit some time later within the bar.
                if bar.low <= self._limit_price <= bar.high:
                    price = self._limit_price
            # Calculate slippage if necessary.
            if price is not None and price != self._limit_price:
                price = slipped_price(price, self.operation, amount, liquidity_strategy, cap_low=self._limit_price)

        ret = {}
        if price is not None:
            ret = {
                self.pair.base_symbol: amount * base_sign,
                self.pair.quote_symbol: price * amount * -base_sign
            }

        return ret

    def get_balance_updates_after_stop_hit(
            self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy
    ) -> Dict[str, Decimal]:
        price = None
        amount = min(self.amount_pending, liquidity_strategy.available_liquidity)
        base_sign = helpers.get_base_sign_for_operation(self.operation)

        if self.operation == OrderOperation.BUY:
            # Limit price was hit at bar open.
            if bar.open < self._limit_price:
                price = slipped_price(bar.open, self.operation, amount, liquidity_strategy, cap_high=self._limit_price)
            # The price went down to limit price or lower.
            elif bar.low <= self._limit_price:
                price = self._limit_price
        else:
            assert self.operation == OrderOperation.SELL
            # Limit price was hit at bar open.
            if bar.open > self._limit_price:
                price = slipped_price(bar.open, self.operation, amount, liquidity_strategy, cap_low=self._limit_price)
            # The price went up to limit price or higher.
            elif bar.high >= self._limit_price:
                price = self._limit_price

        ret = {}
        if price:
            ret = {
                self.pair.base_symbol: amount * base_sign,
                self.pair.quote_symbol: price * amount * -base_sign
            }
        return ret

    def get_balance_updates(
            self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy
    ) -> Dict[str, Decimal]:
        if not self._stop_price_hit:
            ret = self.get_balance_updates_before_stop_hit(bar, liquidity_strategy)
        else:
            ret = self.get_balance_updates_after_stop_hit(bar, liquidity_strategy)
        return ret


def slipped_price(
        price: Decimal, operation: OrderOperation, amount: Decimal, liquidity_strategy: liquidity.LiquidityStrategy,
        cap_low: Optional[Decimal] = None, cap_high: Optional[Decimal] = None
) -> Decimal:
    price_impact = liquidity_strategy.calculate_price_impact(amount)
    if operation == OrderOperation.BUY:
        price *= (Decimal(1) + price_impact)
    else:
        assert operation == OrderOperation.SELL
        price *= (Decimal(1) - price_impact)

    if cap_low is not None:
        price = max(price, cap_low)
    if cap_high is not None:
        price = min(price, cap_high)

    return price
