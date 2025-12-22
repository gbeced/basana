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
from typing import Dict, List, Optional, Set
import abc
import copy
import dataclasses
import datetime
import enum
import logging

from basana.backtesting import helpers, liquidity, value_map
from basana.core import bar, logs
from basana.core.enums import OrderOperation
from basana.core.helpers import round_decimal, truncate_decimal
from basana.core.pair import Pair, PairInfo


logger = logging.getLogger(__name__)


@enum.unique
class OrderState(enum.Enum):
    OPEN = 100
    COMPLETED = 101
    CANCELED = 102


@dataclasses.dataclass
class Fill:
    #: The time when the fill took place.
    when: datetime.datetime
    #: The balance updates.
    balance_updates: Dict[str, Decimal]
    #: The fees.
    fees: Dict[str, Decimal]
    #: The fill price.
    fill_price: Decimal


@dataclasses.dataclass
class OrderInfo:
    #: The order id.
    id: str
    #: The pair.
    pair: Pair
    #: True if the order is open, False otherwise.
    is_open: bool
    #: The operation.
    operation: OrderOperation
    #: The original amount.
    amount: Decimal
    #: The amount filled.
    amount_filled: Decimal
    #: The amount remaining.
    amount_remaining: Decimal
    #: The quote amount filled.
    quote_amount_filled: Decimal
    #: The fees charged.
    fees: Dict[str, Decimal]
    #: The limit price.
    limit_price: Optional[Decimal] = None
    #: The stop price.
    stop_price: Optional[Decimal] = None
    #: The ids of the associated loans.
    loan_ids: List[str] = dataclasses.field(default_factory=list)
    #: The fills.
    fills: List[Fill] = dataclasses.field(default_factory=list)

    @property
    def fill_price(self) -> Optional[Decimal]:
        """The fill price."""
        wsum = Decimal(0)
        for fill in self.fills:
            fill_amount = abs(fill.balance_updates.get(self.pair.base_symbol, Decimal(0)))
            wsum += fill_amount * fill.fill_price
        return None if not self.amount_filled else wsum / self.amount_filled


# This is an internal abstraction to be used by the exchange.
class Order(metaclass=abc.ABCMeta):
    def __init__(
            self, id: str, operation: OrderOperation, pair: Pair, pair_info: PairInfo,
            amount: Decimal, auto_borrow: bool = False, auto_repay: bool = False
    ):
        assert amount > Decimal(0), f"Invalid amount {amount}"

        self._id = id
        self._operation = operation
        self._pair = pair
        self._pair_info = pair_info
        self._amount = amount
        self._state = OrderState.OPEN
        self._balance_updates = value_map.ValueMap()
        self._fees = value_map.ValueMap()
        self._fills: List[Fill] = []
        self._auto_borrow = auto_borrow
        self._auto_repay = auto_repay
        self._loan_ids: Set[str] = set()

    @property
    def id(self) -> str:
        return self._id

    @property
    def pair(self) -> Pair:
        return self._pair

    @property
    def pair_info(self) -> PairInfo:
        return self._pair_info

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
    def balance_updates(self) -> value_map.ValueMap:
        return self._balance_updates

    @property
    def fees(self) -> value_map.ValueMap:
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

    @property
    def auto_borrow(self) -> bool:
        return self._auto_borrow

    @property
    def auto_repay(self) -> bool:
        return self._auto_repay

    def cancel(self):
        assert self._state == OrderState.OPEN
        self._state = OrderState.CANCELED

    def add_fill(self, fill: Fill):
        self._balance_updates += fill.balance_updates
        self._fees += fill.fees
        if self.amount_filled >= self.amount:
            self._state = OrderState.COMPLETED
        self._fills.append(fill)

    def add_loan(self, loan_id: str):
        self._loan_ids.add(loan_id)

    def get_order_info(self) -> OrderInfo:
        return OrderInfo(
            id=self.id, pair=self.pair, is_open=self._state == OrderState.OPEN, operation=self.operation,
            amount=self.amount, amount_filled=self.amount_filled, amount_remaining=self.amount_pending,
            quote_amount_filled=self.quote_amount_filled,
            fees={symbol: -amount for symbol, amount in self._fees.items() if amount},
            loan_ids=[loan_id for loan_id in self._loan_ids], fills=copy.copy(self.fills)
        )

    @abc.abstractmethod
    def try_fill(self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy) -> Optional[Fill]:
        """
        Try to fill the order.

        :param bar: The bar that summarizes the trading activity.
        :param liquidity_strategy: The strategy used to model available liquidity.

        .. note::

            * It can be either a complete or partial fill, based on the trading activity summarized by the bar and the
              available liquidity.
            * The balance updates in the fill should include both the base amount and the quote amount, with opposite
              signs depending on the operation.
            * The order state should not change inside try_fill.
        """
        raise NotImplementedError()

    def calculate_estimated_fill_price(self) -> Optional[Decimal]:
        """
        Optionally override to return an estimate for the fill price.
        This will be used to reserve the funds that will be required later for processing the order.
        """
        return None

    def not_filled(self):
        """Called every time the order was processed but no fill took place."""
        pass

    def get_debug_info(self) -> dict:
        return {
            "id": self.id,
            "pair": self.pair,
            "operation": self.operation,
            "amount": self.amount,
            "amount_filled": self.amount_filled,
        }


class MarketOrder(Order):
    def __init__(
            self, id: str, operation: OrderOperation, pair: Pair, pair_info: PairInfo,
            amount: Decimal, auto_borrow: bool = False, auto_repay: bool = False
    ):
        super().__init__(id, operation, pair, pair_info, amount, auto_borrow=auto_borrow, auto_repay=auto_repay)

    def not_filled(self):
        self.cancel()

    def try_fill(self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy) -> Optional[Fill]:
        # No partial fills for market orders.
        if self.amount_pending > liquidity_strategy.available_liquidity:
            logger.debug(logs.StructuredMessage("Not enough liquidity to fill order", order_id=self.id))
            return None

        amount = self.amount_pending
        price = slipped_price(bar.open, amount, self, liquidity_strategy)
        base_sign = helpers.get_base_sign_for_operation(self.operation)
        return Fill(
            when=bar.begin,
            balance_updates={
                self.pair.base_symbol: amount * base_sign,
                self.pair.quote_symbol: round_decimal(price * amount * -base_sign, self.pair_info.quote_precision)
            },
            fees={},
            fill_price=price
        )


class LimitOrder(Order):
    def __init__(
            self, id: str, operation: OrderOperation, pair: Pair, pair_info: PairInfo,
            amount: Decimal, limit_price: Decimal, auto_borrow: bool = False, auto_repay: bool = False
    ):
        assert limit_price > Decimal(0), "Invalid limit_price {limit_price}"

        super().__init__(id, operation, pair, pair_info, amount, auto_borrow=auto_borrow, auto_repay=auto_repay)
        self._limit_price = limit_price

    def try_fill(self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy) -> Optional[Fill]:
        amount = min(self.amount_pending, liquidity_strategy.available_liquidity)
        amount = truncate_decimal(amount, self.pair_info.base_precision)
        if not amount:
            return None

        price = None
        fill_dt = None
        base_sign = helpers.get_base_sign_for_operation(self.operation)

        if self.operation == OrderOperation.BUY:
            # Limit price was hit at bar open.
            if bar.open <= self._limit_price:
                price = slipped_price(bar.open, amount, self, liquidity_strategy, cap_high=self._limit_price)
                fill_dt = bar.begin
            # The price went down to limit price or lower.
            elif bar.low <= self._limit_price:
                price = self._limit_price
                fill_dt = bar.begin
        else:
            # Limit price was hit at bar open.
            if bar.open >= self._limit_price:
                price = slipped_price(bar.open, amount, self, liquidity_strategy, cap_low=self._limit_price)
                fill_dt = bar.begin
            # The price went up to limit price or higher.
            elif bar.high >= self._limit_price:
                price = self._limit_price
                fill_dt = bar.begin

        ret = None
        if price and fill_dt:
            ret = Fill(
                when=fill_dt,
                balance_updates={
                    self.pair.base_symbol: amount * base_sign,
                    self.pair.quote_symbol: round_decimal(price * amount * -base_sign, self.pair_info.quote_precision)
                },
                fees={},
                fill_price=price
            )
        return ret

    def calculate_estimated_fill_price(self) -> Optional[Decimal]:
        # It will be the limit price or a better one.
        return self._limit_price

    def get_debug_info(self) -> dict:
        ret = super().get_debug_info()
        ret["limit_price"] = self._limit_price
        return ret

    def get_order_info(self) -> OrderInfo:
        ret = super().get_order_info()
        ret.limit_price = self._limit_price
        return ret


class StopOrder(Order):
    def __init__(
            self, id: str, operation: OrderOperation, pair: Pair, pair_info: PairInfo,
            amount: Decimal, stop_price: Decimal, auto_borrow: bool = False, auto_repay: bool = False
    ):
        assert stop_price > Decimal(0), "Invalid stop_price {stop_price}"

        super().__init__(id, operation, pair, pair_info, amount, auto_borrow=auto_borrow, auto_repay=auto_repay)
        self._stop_price = stop_price
        self._stop_price_hit = False

    def not_filled(self):
        if self._stop_price_hit:
            self.cancel()

    def try_fill(self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy) -> Optional[Fill]:
        # No partial fills for stop orders.
        if self.amount_pending > liquidity_strategy.available_liquidity:
            logger.debug(logs.StructuredMessage("Not enough liquidity to fill order", order_id=self.id))
            return None

        price = None
        fill_dt = None
        amount = self.amount_pending
        base_sign = helpers.get_base_sign_for_operation(self.operation)

        if self.operation == OrderOperation.BUY:
            # Stop price was hit at bar open.
            if bar.open >= self._stop_price:
                price = bar.open
                fill_dt = bar.begin
            # The price went up to stop price or higher.
            elif bar.high >= self._stop_price:
                price = self._stop_price
                fill_dt = bar.begin
        else:
            # Stop price was hit at bar open.
            if bar.open <= self._stop_price:
                price = bar.open
                fill_dt = bar.begin
            # The price went down to stop price or lower.
            elif bar.low <= self._stop_price:
                price = self._stop_price
                fill_dt = bar.begin

        if price:
            price = slipped_price(price, amount, self, liquidity_strategy)

        ret = None
        if price and fill_dt:
            self._stop_price_hit = True
            ret = Fill(
                when=fill_dt,
                balance_updates={
                    self.pair.base_symbol: amount * base_sign,
                    self.pair.quote_symbol: round_decimal(price * amount * -base_sign, self.pair_info.quote_precision)
                },
                fees={},
                fill_price=price
            )
        return ret

    def calculate_estimated_fill_price(self) -> Optional[Decimal]:
        # It should be around the stop price, or at least we hope so.
        return self._stop_price

    def get_debug_info(self) -> dict:
        ret = super().get_debug_info()
        ret["stop_price"] = self._stop_price
        return ret

    def get_order_info(self) -> OrderInfo:
        ret = super().get_order_info()
        ret.stop_price = self._stop_price
        return ret


class StopLimitOrder(Order):
    def __init__(
            self, id: str, operation: OrderOperation, pair: Pair, pair_info: PairInfo,
            amount: Decimal, stop_price: Decimal, limit_price: Decimal, auto_borrow: bool = False,
            auto_repay: bool = False
    ):
        assert stop_price > Decimal(0), "Invalid stop_price {stop_price}"
        assert limit_price > Decimal(0), "Invalid limit_price {limit_price}"

        super().__init__(id, operation, pair, pair_info, amount, auto_borrow=auto_borrow, auto_repay=auto_repay)
        self._stop_price = stop_price
        self._limit_price = limit_price
        self._stop_price_hit = False

    def try_fill_before_stop_hit(
            self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy, amount: Decimal
    ) -> Optional[Fill]:
        assert not self._stop_price_hit

        price = None
        fill_dt = None
        base_sign = helpers.get_base_sign_for_operation(self.operation)

        if self.operation == OrderOperation.BUY:
            # Stop price was hit at bar open.
            if bar.open >= self._stop_price:
                self._stop_price_hit = True
                # Limit price was also hit at open.
                if bar.open <= self._limit_price:
                    price = bar.open
                    fill_dt = bar.begin
                # Limit price was hit some time later within the bar.
                elif bar.low <= self._limit_price <= bar.high:
                    price = self._limit_price
                    fill_dt = bar.begin
            # The price went up to stop price or higher.
            elif bar.high >= self._stop_price:
                self._stop_price_hit = True
                # Limit price was hit some time later within the bar.
                if bar.low <= self._limit_price <= bar.high:
                    price = self._limit_price
                    fill_dt = bar.begin
            # Calculate slippage if necessary.
            if price is not None and price != self._limit_price:
                price = slipped_price(price, amount, self, liquidity_strategy, cap_high=self._limit_price)
        else:
            # Stop price was hit at bar open.
            if bar.open <= self._stop_price:
                self._stop_price_hit = True
                # Limit price was also hit at open.
                if bar.open >= self._limit_price:
                    price = bar.open
                    fill_dt = bar.begin
                # Limit price was hit some time later within the bar.
                elif bar.low <= self._limit_price <= bar.high:
                    price = self._limit_price
                    fill_dt = bar.begin
            # The price went down to stop price or lower.
            elif bar.low <= self._stop_price:
                self._stop_price_hit = True
                # Limit price was hit some time later within the bar.
                if bar.low <= self._limit_price <= bar.high:
                    price = self._limit_price
                    fill_dt = bar.begin
            # Calculate slippage if necessary.
            if price is not None and price != self._limit_price:
                price = slipped_price(price, amount, self, liquidity_strategy, cap_low=self._limit_price)

        ret = None
        if price and fill_dt:
            ret = Fill(
                when=fill_dt,
                balance_updates={
                    self.pair.base_symbol: amount * base_sign,
                    self.pair.quote_symbol: round_decimal(price * amount * -base_sign, self.pair_info.quote_precision)
                },
                fees={},
                fill_price=price
            )
        return ret

    def try_fill_after_stop_hit(
            self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy, amount: Decimal
    ) -> Optional[Fill]:
        price = None
        fill_dt = None
        base_sign = helpers.get_base_sign_for_operation(self.operation)

        if self.operation == OrderOperation.BUY:
            # Limit price was hit at bar open.
            if bar.open < self._limit_price:
                price = slipped_price(bar.open, amount, self, liquidity_strategy, cap_high=self._limit_price)
                fill_dt = bar.begin
            # The price went down to limit price or lower.
            elif bar.low <= self._limit_price:
                price = self._limit_price
                fill_dt = bar.begin
        else:
            # Limit price was hit at bar open.
            if bar.open > self._limit_price:
                price = slipped_price(bar.open, amount, self, liquidity_strategy, cap_low=self._limit_price)
                fill_dt = bar.begin
            # The price went up to limit price or higher.
            elif bar.high >= self._limit_price:
                price = self._limit_price
                fill_dt = bar.begin

        ret = None
        if price and fill_dt:
            ret = Fill(
                when=fill_dt,
                balance_updates={
                    self.pair.base_symbol: amount * base_sign,
                    self.pair.quote_symbol: round_decimal(price * amount * -base_sign, self.pair_info.quote_precision)
                },
                fees={},
                fill_price=price
            )
        return ret

    def try_fill(self, bar: bar.Bar, liquidity_strategy: liquidity.LiquidityStrategy) -> Optional[Fill]:
        amount = min(self.amount_pending, liquidity_strategy.available_liquidity)
        amount = truncate_decimal(amount, self.pair_info.base_precision)
        if not amount:
            return None

        if not self._stop_price_hit:
            ret = self.try_fill_before_stop_hit(bar, liquidity_strategy, amount)
        else:
            ret = self.try_fill_after_stop_hit(bar, liquidity_strategy, amount)
        return ret

    def calculate_estimated_fill_price(self) -> Optional[Decimal]:
        # It will be the limit price or a better one.
        return self._limit_price

    def get_debug_info(self) -> dict:
        ret = super().get_debug_info()
        ret["limit_price"] = self._limit_price
        ret["stop_price"] = self._stop_price
        return ret

    def get_order_info(self) -> OrderInfo:
        ret = super().get_order_info()
        ret.limit_price = self._limit_price
        ret.stop_price = self._stop_price
        return ret


def slipped_price(
        price: Decimal, amount: Decimal,
        order: Order, liquidity_strategy: liquidity.LiquidityStrategy,
        cap_low: Optional[Decimal] = None, cap_high: Optional[Decimal] = None
) -> Decimal:
    price_impact = liquidity_strategy.calculate_price_impact(amount)
    if order.operation == OrderOperation.BUY:
        price *= (Decimal(1) + price_impact)
    else:
        price *= (Decimal(1) - price_impact)

    if cap_low is not None:
        price = max(price, cap_low)
    if cap_high is not None:
        price = min(price, cap_high)

    return round_decimal(price, order.pair_info.quote_precision)
