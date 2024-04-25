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
from typing import Callable, Dict, Generator, Iterable, Optional
import copy
import decimal
import logging

from basana.backtesting import account_balances, config, errors, fees, helpers, liquidity, prices
from basana.backtesting.orders import Order
from basana.core import bar, helpers as core_helpers, logs
from basana.core.pair import Pair


logger = logging.getLogger(__name__)

LiquidityStrategyFactory = Callable[[], liquidity.LiquidityStrategy]


class OrderManager:
    def __init__(
            self, balances: account_balances.AccountBalances, prices: prices.Prices,
            fee_strategy: fees.FeeStrategy, liquidity_strategy_factory: LiquidityStrategyFactory,
            config: config.Config
    ):
        self._balances = balances
        self._prices = prices
        self._fee_strategy = fee_strategy
        self._liquidity_strategy_factory = liquidity_strategy_factory
        self._config = config
        self._liquidity_strategies: Dict[Pair, liquidity.LiquidityStrategy] = {}
        self._orders = helpers.ExchangeObjectContainer[Order]()
        self._holds_by_order: Dict[str, Dict[str, Decimal]] = {}

    def on_bar_event(self, bar_event: bar.BarEvent):
        if (liquidity_strategy := self._liquidity_strategies.get(bar_event.bar.pair)) is None:
            liquidity_strategy = self._liquidity_strategy_factory()
        liquidity_strategy.on_bar(bar_event.bar)
        for order in filter(lambda o: o.pair == bar_event.bar.pair, self._orders.get_open()):
            self._process_order(order, bar_event, liquidity_strategy)

    def add_order(self, order: Order):
        try:
            # When an order gets accepted we need to hold any required balance that will be debited as the order gets
            # filled.
            if required_balances := self._estimate_required_balances(order):
                self._balances.update(hold_updates=required_balances)
                self._holds_by_order[order.id] = copy.copy(required_balances)

            self._orders.add(order)

        except errors.NotEnoughBalance as e:
            logger.debug(logs.StructuredMessage(
                "Not enough balance to accept order", order=order.get_debug_info(), symbol=e.symbol,
                short=e.balance_short
            ))
            raise

    def get_order(self, order_id: str) -> Optional[Order]:
        return self._orders.get(order_id)

    def get_all_orders(self) -> Iterable[Order]:
        return self._orders.get_all()

    def get_open_orders(self) -> Generator[Order, None, None]:
        return self._orders.get_open()

    def cancel_order(self, order_id: str):
        order = self._orders.get(order_id)
        if order is None:
            raise errors.Error("Order not found")
        if not order.is_open:
            raise errors.Error("Order {} is in {} state and can't be canceled".format(order_id, order.state))
        order.cancel()
        self._order_updated(order)

    def get_balance_on_hold_for_order(self, order_id: str, symbol: str) -> Decimal:
        return self._holds_by_order.get(order_id, {}).get(symbol, Decimal(0))

    def _update_balances(self, order: Order, balance_updates: Dict[str, Decimal]):
        # If we have holds associated with the order, it may be time to release some/all of those.
        hold_updates = {}
        order_holds = self._holds_by_order.get(order.id, {})
        if order_holds:
            if order.is_open:
                hold_updates = {
                    symbol: max(amount, -order_holds.get(symbol, Decimal(0)))
                    for symbol, amount in balance_updates.items()
                    if amount < Decimal(0) and symbol in order_holds
                }
            else:
                hold_updates = {symbol: -amount for symbol, amount in order_holds.items()}

        # Update holds and balances.
        if balance_updates or hold_updates:
            self._balances.update(balance_updates=balance_updates, hold_updates=hold_updates)

        # Update holds by order.
        if order_holds:
            if order.is_open:
                for symbol, update in hold_updates.items():
                    order_holds[symbol] += update
                    assert order_holds[symbol] >= Decimal(0)
            else:
                del self._holds_by_order[order.id]

    def _order_updated(self, order: Order):
        if not order.is_open:
            # The order is closed and there might be balances on hold that have to be released.
            self._update_balances(order, {})

    def _process_order(
            self, order: Order, bar_event: bar.BarEvent, liquidity_strategy: liquidity.LiquidityStrategy
    ):
        def order_not_filled():
            order.not_filled()
            self._order_updated(order)
            if not order.is_open:
                logger.debug(logs.StructuredMessage("Order closed", order_id=order.id, order_state=order.state))

        logger.debug(logs.StructuredMessage(
            "Processing order", order=order.get_debug_info(),
            bar={
                "open": bar_event.bar.open, "high": bar_event.bar.high, "low": bar_event.bar.low,
                "close": bar_event.bar.close, "volume": bar_event.bar.volume,
            }
        ))
        prev_state = order.state
        balance_updates = order.get_balance_updates(bar_event.bar, liquidity_strategy)
        assert order.state == prev_state, "The order state should not change inside get_balance_updates"

        # If there are no balance updates then there is nothing left to do.
        if not balance_updates:
            order_not_filled()
            return

        # Sanity checks. Base and quote amounts should be there.
        base_sign = helpers.get_base_sign_for_operation(order.operation)
        assert_balance_update_value(balance_updates, order.pair.base_symbol, base_sign)
        assert_balance_update_value(balance_updates, order.pair.quote_symbol, -base_sign)

        # If base/quote amounts were removed after rounding then there is nothing left to do.
        balance_updates = self._round_balance_updates(balance_updates, order.pair)
        logger.debug(logs.StructuredMessage("Processing order", order_id=order.id, balance_updates=balance_updates))
        if order.pair.base_symbol not in balance_updates or order.pair.quote_symbol not in balance_updates:
            order_not_filled()
            return

        # Get fees, round them, and combine them with the balance updates.
        fees = self._fee_strategy.calculate_fees(order, balance_updates)
        fees = self._round_fees(fees, order.pair)
        logger.debug(logs.StructuredMessage("Processing order", order_id=order.id, fees=fees))
        final_updates = helpers.add_amounts(balance_updates, fees)
        final_updates = helpers.remove_empty_amounts(final_updates)

        try:
            # Update balances. This may fail if there is not enough balance, so we do this first.
            self._update_balances(order, final_updates)
            logger.debug(logs.StructuredMessage(
                "Order updated", order_id=order.id, final_updates=final_updates, order_state=order.state
            ))
            # Update the liquidity strategy.
            liquidity_strategy.take_liquidity(abs(balance_updates[bar_event.bar.pair.base_symbol]))
            # Update the order and release any pending balance on hold if the order is now closed.
            order.add_fill(bar_event.when, balance_updates, fees)
            self._order_updated(order)

        except errors.NotEnoughBalance as e:
            logger.debug(logs.StructuredMessage(
                "Not enough balance processing order", order=order.get_debug_info(), symbol=e.symbol,
                short=e.balance_short
            ))
            order_not_filled()

    def _round_balance_updates(self, balance_updates: Dict[str, Decimal], pair: Pair) -> Dict[str, Decimal]:
        ret = copy.copy(balance_updates)
        pair_info = self._config.get_pair_info(pair)

        # For the base amount we truncate instead of rounding to avoid exceeding available liquidity.
        base_amount = ret.get(pair.base_symbol)
        if base_amount:
            base_amount = core_helpers.truncate_decimal(base_amount, pair_info.base_precision)
            ret[pair.base_symbol] = base_amount

        # For the quote amount we simply round.
        quote_amount = ret.get(pair.quote_symbol)
        if quote_amount:
            quote_amount = core_helpers.round_decimal(quote_amount, pair_info.quote_precision)
            ret[pair.quote_symbol] = quote_amount

        return helpers.remove_empty_amounts(ret)

    def _round_fees(self, fees: Dict[str, Decimal], pair: Pair) -> Dict[str, Decimal]:
        ret = copy.copy(fees)
        pair_info = self._config.get_pair_info(pair)
        precisions = {
            pair.base_symbol: pair_info.base_precision,
            pair.quote_symbol: pair_info.quote_precision,
        }
        for symbol in ret.keys():
            amount = ret.get(symbol)
            # If there is a fee in a symbol other that base/quote we won't round it since we don't know the precision.
            if amount and (precision := precisions.get(symbol)) is not None:
                amount = core_helpers.round_decimal(amount, precision, rounding=decimal.ROUND_UP)
                ret[symbol] = amount

        return helpers.remove_empty_amounts(ret)

    def _estimate_required_balances(self, order: Order) -> Dict[str, Decimal]:
        # Get an estimated fill price. If the order can't provide one, use the last price available.
        estimated_fill_price = order.calculate_estimated_fill_price()
        if not estimated_fill_price:
            try:
                estimated_fill_price = self._prices.get_price(order.pair)
            except errors.NoPrice:
                pass

        # Build a dictionary of balance updates suitable for calculating fees.
        base_sign = helpers.get_base_sign_for_operation(order.operation)
        estimated_balance_updates = {
            order.pair.base_symbol: order.amount * base_sign
        }
        if estimated_fill_price:
            estimated_balance_updates[order.pair.quote_symbol] = \
                order.amount * estimated_fill_price * -base_sign
        estimated_balance_updates = self._round_balance_updates(estimated_balance_updates, order.pair)

        # Calculate fees.
        fees = {}
        if len(estimated_balance_updates) == 2:
            fees = self._fee_strategy.calculate_fees(order, estimated_balance_updates)
            fees = self._round_fees(fees, order.pair)
        estimated_balance_updates = helpers.add_amounts(estimated_balance_updates, fees)

        # Return only negative balance updates as required balances.
        return {symbol: -amount for symbol, amount in estimated_balance_updates.items() if amount < Decimal(0)}


def assert_balance_update_value(balance_updates: Dict[str, Decimal], symbol: str, sign: Decimal):
    value = balance_updates.get(symbol)
    assert value is not None, f"{symbol} is missing"
    assert value != Decimal(0), f"{symbol} is zero"
    assert helpers.get_sign(value) == sign, f"{symbol} sign is wrong. It should be {sign}"
