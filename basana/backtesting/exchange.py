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

import decimal
from decimal import Decimal
from typing import cast, Any, Awaitable, Callable, Dict, Generator, List, Optional, Sequence, Tuple
import copy
import dataclasses
import logging
import uuid

from basana.backtesting import account_balances, errors, fees, liquidity, orders, requests
from basana.backtesting import helpers as bt_helpers
from basana.core import bar, dispatcher, enums, event, logs
from basana.core import helpers as core_helpers
from basana.core.pair import Pair, PairInfo


logger = logging.getLogger(__name__)

BarEventHandler = Callable[[bar.BarEvent], Awaitable[Any]]
Error = errors.Error
LiquidityStrategyFactory = Callable[[], liquidity.LiquidityStrategy]
OrderInfo = orders.OrderInfo
OrderOperation = enums.OrderOperation


def assert_has_value(balance_updates: Dict[str, Decimal], symbol: str, sign: Decimal):
    value = balance_updates.get(symbol)
    assert value is not None, f"{symbol} is missing"
    assert value != Decimal(0), f"{symbol} is zero"
    assert bt_helpers.get_sign(value) == sign, f"{symbol} sign is wrong. It should be {sign}"


class OrderIndex:
    def __init__(self):
        self._orders = {}
        self._open_orders = []
        self._reindex_every = 50
        self._reindex_counter = 0

    def add_order(self, order: orders.Order):
        assert order.id not in self._orders

        self._orders[order.id] = order
        if order.is_open:
            self._open_orders.append(order)

    def get_order(self, id: str) -> Optional[orders.Order]:
        return self._orders.get(id)

    def get_open_orders(self) -> Generator[orders.Order, None, None]:
        self._reindex_counter += 1
        new_open_orders: Optional[List[orders.Order]] = None
        if self._reindex_counter % self._reindex_every == 0:
            new_open_orders = []

        for order in self._open_orders:
            if order.is_open:
                yield order
                if new_open_orders is not None and order.is_open:
                    new_open_orders.append(order)

        if new_open_orders is not None:
            self._open_orders = new_open_orders

    def get_all_orders(self) -> Sequence[orders.Order]:
        return self._orders.values()


@dataclasses.dataclass
class Balance:
    #: The available balance.
    available: Decimal
    #: The total balance (available + reserved).
    total: Decimal


@dataclasses.dataclass
class CreatedOrder:
    #: The order id.
    id: str


@dataclasses.dataclass
class CanceledOrder:
    #: The order id.
    id: str


@dataclasses.dataclass
class OpenOrder:
    #: The order id.
    id: str
    #: The operation.
    operation: OrderOperation
    #: The original amount.
    amount: Decimal
    #: The amount filled.
    amount_filled: Decimal


class Exchange:
    """This class implements a backtesting exchange.

    This backtesting exchange has support for Market, Limit, Stop and Stop Limit orders and it will simulate order
    execution based on summarized trading activity (:class:`basana.BarEvent`).

    :param dispatcher: The event dispatcher.
    :param initial_balances: The initial balance for each currency/symbol/etc.
    :param liquidity_strategy_factory: A callable that returns a new liquidity strategy.
    :param fee_strategy: The fee stragegy to use.
    :param default_pair_info: The default pair information if a specific one was not set using
        :meth:`Exchange.set_pair_info`.
    :param bid_ask_spread: The spread to use for :meth:`Exchange.get_bid_ask`.
    """
    def __init__(
            self,
            dispatcher: dispatcher.EventDispatcher,
            initial_balances: Dict[str, Decimal],
            liquidity_strategy_factory: LiquidityStrategyFactory = liquidity.VolumeShareImpact,
            fee_strategy: fees.FeeStrategy = fees.NoFee(),
            default_pair_info: PairInfo = PairInfo(base_precision=0, quote_precision=2),
            bid_ask_spread: Decimal = Decimal("0.5")
    ):
        self._dispatcher = dispatcher
        self._balances = account_balances.AccountBalances(initial_balances)
        self._liquidity_strategy_factory = liquidity_strategy_factory
        self._liquidity_strategies: Dict[Pair, liquidity.LiquidityStrategy] = {}
        self._fee_strategy = fee_strategy
        self._orders = OrderIndex()
        self._bar_event_source: Dict[Pair, event.FifoQueueEventSource] = {}
        self._pairs_info: Dict[Pair, PairInfo] = {}
        self._default_pair_info = default_pair_info
        self._last_bars: Dict[Pair, bar.Bar] = {}
        self._bid_ask_spread = bid_ask_spread

    async def get_balance(self, symbol: str) -> Balance:
        """Returns the balance for a specific currency/symbol/etc..

        :param symbol: The currency/symbol/etc..
        """
        available = self._balances.get_available_balance(symbol)
        hold = self._balances.get_balance_on_hold(symbol)
        return Balance(available=available, total=available + hold)

    async def get_balances(self) -> Dict[str, Balance]:
        """Returns all balances."""
        ret = {}
        for symbol in self._balances.get_symbols():
            available = self._balances.get_available_balance(symbol)
            hold = self._balances.get_balance_on_hold(symbol)
            ret[symbol] = Balance(available=available, total=available + hold)
        return ret

    async def get_bid_ask(self, pair: Pair) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Returns the current bid and ask price, if available.

        This is calculated using the closing price of the last bar, and the bid/ask spread specified during
        initialization.

        :param pair: The trading pair.
        """
        bid = ask = None
        last_price = await self._get_last_price(pair)
        if last_price:
            pair_info = await self.get_pair_info(pair)
            half_spread = core_helpers.truncate_decimal(
                (last_price * self._bid_ask_spread / Decimal("100")) / Decimal(2),
                pair_info.quote_precision
            )
            bid = last_price - half_spread
            ask = last_price + half_spread
        return bid, ask

    async def create_order(self, order_request: requests.ExchangeOrder) -> CreatedOrder:
        # Validate request parameters.
        pair_info = await self.get_pair_info(order_request.pair)
        order_request.validate(pair_info)

        # Check balances.
        required_balances = await self._estimate_required_balances(order_request)
        self._check_available_balance(required_balances)

        # Create and accept the order.
        order = order_request.create_order(uuid.uuid4().hex)
        self._orders.add_order(order)

        # Update/hold balances.
        self._balances.order_accepted(order, required_balances)

        return CreatedOrder(id=order.id)

    async def create_market_order(self, operation: OrderOperation, pair: Pair, amount: Decimal) -> CreatedOrder:
        """Creates a market order.

        A market order is an order to immediately buy or sell at the best available price.
        Generally, this type of order will be executed on the next bar using the open price as a reference, and
        according to the rules defined by the liquidity strategy.
        If the order is not filled on the next bar, due to lack of liquidity or funds, the order will be canceled.

        If the order can't be created an :class:`Error` will be raised.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The base amount to buy/sell.
        """
        return await self.create_order(requests.MarketOrder(operation, pair, amount))

    async def create_limit_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, limit_price: Decimal
    ) -> CreatedOrder:
        """Creates a limit order.

        A limit order is an order to buy or sell at a specific price or better.
        A buy limit order can only be executed at the limit price or lower, and a sell limit order can only be executed
        at the limit price or higher.

        If the order can't be created an :class:`Error` will be raised.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The base amount to buy/sell.
        :param limit_price: The limit price.
        """
        return await self.create_order(requests.LimitOrder(operation, pair, amount, limit_price))

    async def create_stop_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal
    ) -> CreatedOrder:
        """Creates a stop order.

        A stop order, also referred to as a stop-loss order, is an order to buy or sell once the price reaches a
        specified price, known as the stop price.
        When the stop price is reached, a stop order becomes a market order.

        * A buy stop order is entered at a stop price above the current market price. Investors generally use a buy
          stop order to limit a loss or to protect a profit on an instrument that they have sold short.
        * A sell stop order is entered at a stop price below the current market price. Investors generally use a sell
          stop order to limit a loss or to protect a profit on an instrument that they own.

        If the order can't be created an :class:`Error` will be raised.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The base amount to buy/sell.
        :param stop_price: The stop price.
        """
        return await self.create_order(requests.StopOrder(operation, pair, amount, stop_price))

    async def create_stop_limit_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal, limit_price: Decimal
    ) -> CreatedOrder:
        """Creates a stop limit order.

        A stop-limit order is an order to buy or sell that combines the features of a stop order and a limit order.
        Once the stop price is reached, a stop-limit order becomes a limit order that will be executed at a specified
        price (or better).

        If the order can't be created an :class:`Error` will be raised.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The base amount to buy/sell.
        :param stop_price: The stop price.
        :param limit_price: The limit price.
        """
        return await self.create_order(requests.StopLimitOrder(operation, pair, amount, stop_price, limit_price))

    async def cancel_order(self, order_id: str) -> CanceledOrder:
        """Cancels an order.

        If the order doesn't exist, or its not open, an :class:`Error` will be raised.

        :param order_id: The order id.
        """
        order = self._orders.get_order(order_id)
        if order is None:
            raise Error("Order not found")
        if not order.is_open:
            raise Error("Order {} is in {} state and can't be canceled".format(order_id, order.state))
        order.cancel()
        # Update balances to release any pending hold.
        self._balances.order_updated(order, {})
        return CanceledOrder(id=order_id)

    async def get_order_info(self, order_id: str) -> OrderInfo:
        """Returns information about an order.

        If the order doesn't exist, or its not open, an :class:`Error` will be raised.

        :param order_id: The order id.
        """
        order = self._orders.get_order(order_id)
        if not order:
            raise Error("Order not found")
        return order.get_order_info()

    async def get_open_orders(self, pair: Optional[Pair] = None) -> List[OpenOrder]:
        """Returns open orders.

        :param pair: If set, only open orders matching this pair will be returned, otherwise all open orders will be
            returned.
        """
        return [
            OpenOrder(
                id=order.id,
                operation=order.operation,
                amount=order.amount,
                amount_filled=order.amount_filled
            )
            for order in self._orders.get_open_orders()
            if pair is None or order.pair == pair
        ]

    def add_bar_source(self, bar_source: event.EventSource):
        """Adds an event source that produces :class:`basana.BarEvent` instances.

        These will be used to drive the backtest.

        :param bar_source: An event source that produces :class:`basana.BarEvent` instances.
        """
        self._dispatcher.subscribe(bar_source, self._on_bar_event)

    def subscribe_to_bar_events(self, pair: Pair, event_handler: BarEventHandler):
        """Registers an async callable that will be called when a new bar is available.

        :param pair: The trading pair.
        :param event_handler: An async callable that receives a basana.BarEvent.
        """
        # Get/create the event source for the given pair.
        event_source = self._bar_event_source.get(pair)
        if event_source is None:
            event_source = event.FifoQueueEventSource()
            self._bar_event_source[pair] = event_source
        self._dispatcher.subscribe(event_source, cast(dispatcher.EventHandler, event_handler))

    async def get_pair_info(self, pair: Pair) -> PairInfo:
        """Returns information about a trading pair.

        :param pair: The trading pair.
        """
        return self._get_pair_info(pair)

    def set_pair_info(self, pair: Pair, pair_info: PairInfo):
        """Set information about a trading pair.

        :param pair: The trading pair.
        :param pair_info: The pair information.
        """
        self._pairs_info[pair] = pair_info

    def _get_pair_info(self, pair: Pair) -> PairInfo:
        ret = self._pairs_info.get(pair)
        if ret is None:
            ret = self._default_pair_info
        return ret

    def _round_balance_updates(self, pair: Pair, balance_updates: Dict[str, Decimal]) -> Dict[str, Decimal]:
        ret = copy.copy(balance_updates)
        pair_info = self._get_pair_info(pair)

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

        return bt_helpers.remove_empty_amounts(ret)

    def _round_fees(self, pair: Pair, fees: Dict[str, Decimal]) -> Dict[str, Decimal]:
        ret = copy.copy(fees)
        pair_info = self._get_pair_info(pair)
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

        return bt_helpers.remove_empty_amounts(ret)

    def _process_order(
            self, order: orders.Order, bar_event: bar.BarEvent, liquidity_strategy: liquidity.LiquidityStrategy
    ):
        def order_not_filled():
            order.not_filled()
            # Update balances to release any pending hold if the order is no longer open.
            if not order.is_open:
                self._balances.order_updated(order, {})
                logger.debug(logs.StructuredMessage("Order not filled", order_id=order.id, order_state=order.state))

        prev_state = order.state
        balance_updates = order.get_balance_updates(bar_event.bar, liquidity_strategy)
        assert order.state == prev_state, "The order state should not change inside get_balance_updates"

        # If there are no balance updates then there is nothing left to do.
        if not balance_updates:
            order_not_filled()
            return

        # Sanity checks. Base and quote amounts should be there.
        base_sign = bt_helpers.get_base_sign_for_operation(order.operation)
        assert_has_value(balance_updates, order.pair.base_symbol, base_sign)
        assert_has_value(balance_updates, order.pair.quote_symbol, -base_sign)

        # If base/quote amounts were removed after rounding then there is nothing left to do.
        balance_updates = self._round_balance_updates(order.pair, balance_updates)
        logger.debug(logs.StructuredMessage("Processing order", order_id=order.id, balance_updates=balance_updates))
        if order.pair.base_symbol not in balance_updates or order.pair.quote_symbol not in balance_updates:
            order_not_filled()
            return

        # Get fees, round them, and combine them with the balance updates.
        fees = self._fee_strategy.calculate_fees(order, balance_updates)
        fees = self._round_fees(order.pair, fees)
        logger.debug(logs.StructuredMessage("Processing order", order_id=order.id, fees=fees))
        final_updates = bt_helpers.add_amounts(balance_updates, fees)
        final_updates = bt_helpers.remove_empty_amounts(final_updates)

        # Check if we're short on any balance.
        balances_short = False
        for symbol, balance_update in final_updates.items():
            available_balance = self._balances.get_available_balance(symbol) + \
                                self._balances.get_balance_on_hold_for_order(order.id, symbol)
            final_balance = available_balance + balance_update
            if final_balance < Decimal(0):
                balances_short = True
                logger.debug(logs.StructuredMessage(
                    "Balance short processing order", order_id=order.id, symbol=symbol, short=final_balance
                ))
                break

        # Update, or fail.
        if not balances_short:
            # Update the liquidity strategy.
            liquidity_strategy.take_liquidity(abs(balance_updates[bar_event.bar.pair.base_symbol]))
            # Update the order.
            order.add_fill(bar_event.when, balance_updates, fees)
            # Update balances.
            self._balances.order_updated(order, final_updates)
            logger.debug(logs.StructuredMessage(
                "Order updated", order_id=order.id, final_updates=final_updates, order_state=order.state
            ))
        else:
            order_not_filled()

    def _process_orders(self, bar_event: bar.BarEvent):
        if (liquidity_strategy := self._liquidity_strategies.get(bar_event.bar.pair)) is None:
            liquidity_strategy = self._liquidity_strategy_factory()
        liquidity_strategy.on_bar(bar_event.bar)
        for order in filter(lambda o: o.pair == bar_event.bar.pair, self._orders.get_open_orders()):
            self._process_order(order, bar_event, liquidity_strategy)

    async def _on_bar_event(self, event: event.Event):
        assert isinstance(event, bar.BarEvent), f"{event} is not an instance of bar.BarEvent"

        self._last_bars[event.bar.pair] = event.bar
        self._process_orders(event)
        # Forward the event to the right source, if any.
        event_source = self._bar_event_source.get(event.bar.pair)
        if event_source:
            event_source.push(event)

    def _check_available_balance(self, required_balance: Dict[str, Decimal]):
        for symbol, required in required_balance.items():
            assert required > Decimal(0), f"Invalid required balance {required} for {symbol}"
            available_balance = self._balances.get_available_balance(symbol)
            if available_balance < required:
                raise errors.Error("Not enough {} available. {} are required and {} are available".format(
                    symbol, required, available_balance
                ))

    async def _get_last_price(self, pair: Pair) -> Optional[Decimal]:
        last_bar = self._last_bars.get(pair)
        return last_bar.close if last_bar else None

    async def _estimate_required_balances(self, order_request: requests.ExchangeOrder) -> Dict[str, Decimal]:
        # Build a dictionary of balance updates suitable for calculating fees.
        base_sign = bt_helpers.get_base_sign_for_operation(order_request.operation)
        estimated_balance_updates = {
            order_request.pair.base_symbol: order_request.amount * base_sign
        }
        estimated_fill_price = order_request.get_estimated_fill_price()
        if not estimated_fill_price:
            estimated_fill_price = await self._get_last_price(order_request.pair)
        if estimated_fill_price:
            estimated_balance_updates[order_request.pair.quote_symbol] = \
                order_request.amount * estimated_fill_price * -base_sign
        estimated_balance_updates = self._round_balance_updates(order_request.pair, estimated_balance_updates)

        # Calculate fees.
        fees = {}
        if len(estimated_balance_updates) == 2:
            order = order_request.create_order("temporary")
            fees = self._fee_strategy.calculate_fees(order, estimated_balance_updates)
            fees = self._round_fees(order_request.pair, fees)
        estimated_balance_updates = bt_helpers.add_amounts(estimated_balance_updates, fees)

        # Return only negative balance updates as required balances.
        return {symbol: -amount for symbol, amount in estimated_balance_updates.items() if amount < Decimal(0)}

    def _get_all_orders(self) -> Sequence[orders.Order]:
        return self._orders.get_all_orders()

    def _get_dispatcher(self) -> dispatcher.EventDispatcher:
        return self._dispatcher
