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
from typing import cast, Callable, Dict, List, Optional, Sequence, Tuple
import dataclasses
import logging
import uuid

from basana.backtesting import account_balances, config, errors, fees, lending, loan_mgr, liquidity, \
    orders, order_mgr, prices, requests
from basana.core import bar, dispatcher, enums, event, logs
from basana.core.pair import Pair, PairInfo
from basana.backtesting.lending import base as lending_base


logger = logging.getLogger(__name__)

BarEventHandler = bar.BarEventHandler
Error = errors.Error
LiquidityStrategyFactory = Callable[[], liquidity.LiquidityStrategy]
OrderEvent = order_mgr.OrderEvent
OrderEventHandler = order_mgr.OrderEventHandler
OrderInfo = orders.OrderInfo
OrderOperation = enums.OrderOperation


@dataclasses.dataclass
class Balance:
    #: The available balance.
    available: Decimal
    #: The total balance (available + hold - borrowed).
    total: Decimal = dataclasses.field(init=False)
    #: The balance on hold (reserved for open sell orders).
    hold: Decimal
    #: The balance borrowed.
    borrowed: Decimal

    def __post_init__(self):
        self.total = self.available + self.hold - self.borrowed


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
    """
    This class implements a backtesting exchange.

    This backtesting exchange has support for Market, Limit, Stop and Stop Limit orders and it will simulate order
    execution based on summarized trading activity (:class:`basana.BarEvent`).

    :param dispatcher: The event dispatcher.
    :param initial_balances: The initial balance for each currency/symbol/etc.
    :param liquidity_strategy_factory: A callable that returns a new liquidity strategy.
    :param fee_strategy: The stragegy to use to calculate fees.
    :param default_pair_info: The default pair information if a specific one was not set using
        :meth:`Exchange.set_pair_info`.
    :param bid_ask_spread: The spread to use for :meth:`Exchange.get_bid_ask`.
    :param lending_strategy: The strategy to use for managing loans.
    """
    def __init__(
            self,
            dispatcher: dispatcher.BacktestingDispatcher,
            initial_balances: Dict[str, Decimal],
            liquidity_strategy_factory: LiquidityStrategyFactory = liquidity.VolumeShareImpact,
            fee_strategy: fees.FeeStrategy = fees.NoFee(),
            default_pair_info: Optional[PairInfo] = PairInfo(base_precision=0, quote_precision=2),
            bid_ask_spread: Decimal = Decimal("0.5"),
            lending_strategy: lending.LendingStrategy = lending.NoLoans()
    ):
        self._dispatcher = dispatcher
        self._balances = account_balances.AccountBalances(initial_balances)
        self._bar_event_source: Dict[Pair, event.FifoQueueEventSource] = {}
        self._config = config.Config(None, default_pair_info)
        self._prices = prices.Prices(bid_ask_spread, self._config)
        self._loan_mgr = loan_mgr.LoanManager(
            lending_strategy,
            lending_base.ExchangeContext(
                dispatcher=dispatcher,
                account_balances=self._balances,
                prices=self._prices,
                config=self._config
            )
        )
        self._order_mgr = order_mgr.OrderManager(
            order_mgr.ExchangeContext(
                dispatcher=dispatcher, account_balances=self._balances, prices=self._prices,
                fee_strategy=fee_strategy, liquidity_strategy_factory=liquidity_strategy_factory,
                loan_mgr=self._loan_mgr, config=self._config
            )
        )

    async def get_balance(self, symbol: str) -> Balance:
        """
        Returns the balance for a specific currency/symbol/etc..

        :param symbol: The currency/symbol/etc..
        """
        return self._get_balance(symbol)

    async def get_balances(self) -> Dict[str, Balance]:
        """
        Returns all balances.
        """
        ret = {}
        for symbol in self._balances.get_symbols():
            ret[symbol] = self._get_balance(symbol)
        return ret

    async def get_bid_ask(self, pair: Pair) -> Tuple[Decimal, Decimal]:
        """
        Returns the last bid and ask price.

        This is calculated using the closing price of the last bar, and the bid/ask spread specified during
        initialization.

        :param pair: The trading pair.
        """
        return self._prices.get_bid_ask(pair)

    async def create_order(self, order_request: requests.ExchangeOrder) -> CreatedOrder:
        # Validate request parameters.
        pair_info = await self.get_pair_info(order_request.pair)
        order_request.validate(pair_info)

        order = order_request.create_order(uuid.uuid4().hex)
        self._order_mgr.add_order(order)
        logger.debug(logs.StructuredMessage("Request accepted", order_id=order.id))
        return CreatedOrder(id=order.id)

    async def create_market_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, auto_borrow: bool = False,
            auto_repay: bool = False
    ) -> CreatedOrder:
        """
        Creates a market order.

        A market order is an order to immediately buy or sell at the best available price.
        Generally, this type of order will be executed on the next bar using the open price as a reference, and
        according to the rules defined by the liquidity strategy.
        If the order is not filled on the next bar, due to lack of liquidity or funds, the order will be canceled.

        If the order can't be created an :class:`Error` will be raised.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The base amount to buy/sell.
        :param auto_borrow: Automatically borrow missing funds.
        :param auto_repay: Automatically repay open loans once the order gets filled.
        """
        return await self.create_order(requests.MarketOrder(
            operation, pair, amount, auto_borrow=auto_borrow, auto_repay=auto_repay
        ))

    async def create_limit_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, limit_price: Decimal,
            auto_borrow: bool = False, auto_repay: bool = False
    ) -> CreatedOrder:
        """
        Creates a limit order.

        A limit order is an order to buy or sell at a specific price or better.
        A buy limit order can only be executed at the limit price or lower, and a sell limit order can only be executed
        at the limit price or higher.

        If the order can't be created an :class:`Error` will be raised.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The base amount to buy/sell.
        :param limit_price: The limit price.
        :param auto_borrow: Automatically borrow missing funds.
        :param auto_repay: Automatically repay open loans once the order gets filled.
        """
        return await self.create_order(requests.LimitOrder(
            operation, pair, amount, limit_price, auto_borrow=auto_borrow, auto_repay=auto_repay
        ))

    async def create_stop_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal,
            auto_borrow: bool = False, auto_repay: bool = False
    ) -> CreatedOrder:
        """
        Creates a stop order.

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
        :param auto_borrow: Automatically borrow missing funds.
        :param auto_repay: Automatically repay open loans once the order gets filled.
        """
        return await self.create_order(requests.StopOrder(
            operation, pair, amount, stop_price, auto_borrow=auto_borrow, auto_repay=auto_repay
        ))

    async def create_stop_limit_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal, limit_price: Decimal,
            auto_borrow: bool = False, auto_repay: bool = False
    ) -> CreatedOrder:
        """
        Creates a stop limit order.

        A stop-limit order is an order to buy or sell that combines the features of a stop order and a limit order.
        Once the stop price is reached, a stop-limit order becomes a limit order that will be executed at a specified
        price (or better).

        If the order can't be created an :class:`Error` will be raised.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The base amount to buy/sell.
        :param stop_price: The stop price.
        :param limit_price: The limit price.
        :param auto_borrow: Automatically borrow missing funds.
        :param auto_repay: Automatically repay open loans once the order gets filled.
        """
        return await self.create_order(requests.StopLimitOrder(
            operation, pair, amount, stop_price, limit_price, auto_borrow=auto_borrow, auto_repay=auto_repay
        ))

    async def cancel_order(self, order_id: str) -> CanceledOrder:
        """
        Cancels an order.

        If the order doesn't exist, or its not open, an :class:`Error` will be raised.

        :param order_id: The order id.
        """
        self._order_mgr.cancel_order(order_id)
        return CanceledOrder(id=order_id)

    async def get_order_info(self, order_id: str) -> OrderInfo:
        """
        Returns information about an order.

        If the order doesn't exist, or its not open, an :class:`Error` will be raised.

        :param order_id: The order id.
        """
        order = self._order_mgr.get_order(order_id)
        if not order:
            raise errors.NotFound("Order not found")
        return order.get_order_info()

    async def get_open_orders(self, pair: Optional[Pair] = None) -> List[OpenOrder]:
        """
        Returns open orders.

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
            for order in self._order_mgr.get_open_orders()
            if pair is None or order.pair == pair
        ]

    async def get_orders(self, pair: Optional[Pair] = None, is_open: Optional[bool] = None) -> List[OrderInfo]:
        """
        Returns orders filtered by various conditions.

        :param pair: If set, only orders matching this pair will be returned.
        :param is_open: If set, only open or closed orders will be returned.
        """

        orders = self._order_mgr.get_all_orders()
        if pair:
            orders = filter(lambda order: order.pair == pair, orders)
        if is_open is not None:
            orders = filter(lambda order: order.is_open == is_open, orders)
        return [order.get_order_info() for order in orders]

    def add_bar_source(self, bar_source: event.EventSource):
        """
        Adds an event source that produces :class:`basana.BarEvent` instances.

        These will be used to drive the backtest.

        :param bar_source: An event source that produces :class:`basana.BarEvent` instances.
        """
        self._dispatcher.subscribe(bar_source, self._on_bar_event)

    def subscribe_to_bar_events(self, pair: Pair, event_handler: BarEventHandler):
        """
        Registers an async callable that will be called when a new bar is available.

        :param pair: The trading pair.
        :param event_handler: An async callable that receives a basana.BarEvent.
        """
        # Get/create the event source for the given pair.
        event_source = self._bar_event_source.get(pair)
        if event_source is None:
            event_source = event.FifoQueueEventSource()
            self._bar_event_source[pair] = event_source
        self._dispatcher.subscribe(event_source, cast(dispatcher.EventHandler, event_handler))

    def subscribe_to_order_events(self, event_handler: OrderEventHandler):
        """
        Registers an async callable that will be called when an order is accepted or updated.

        :param event_handler: The event handler.
        """
        self._order_mgr.subscribe_to_order_events(event_handler)

    async def get_pair_info(self, pair: Pair) -> PairInfo:
        """
        Returns information about a trading pair.

        :param pair: The trading pair.
        """
        return self._get_pair_info(pair)

    def set_pair_info(self, pair: Pair, pair_info: PairInfo):
        """
        Set information about a trading pair.

        :param pair: The trading pair.
        :param pair_info: The pair information.
        """
        self._config.set_pair_info(pair, pair_info)

    def set_symbol_precision(self, symbol: str, precision: int):
        """
        Set precision for a symbol.

        :param symbol: The symbol.
        :param precision: The precision.
        """
        self._config.set_symbol_info(symbol, config.SymbolInfo(precision=precision))

    async def create_loan(self, symbol: str, amount: Decimal) -> lending.LoanInfo:
        """
        Creates a loan.

        :param symbol: The symbol to borrow.
        :param amount: The amount to borrow.
        """

        return self._loan_mgr.create_loan(symbol, amount)

    async def get_loans(
            self, borrowed_symbol: Optional[str] = None, is_open: Optional[bool] = None
    ) -> List[lending.LoanInfo]:
        """
        Returns loans filtered by various conditions.

        :param borrowed_symbol: If set, only loans matching this borrowed symbol will be returned.
        :param is_open: If set, only open or closed loans will be returned.
        """
        return self._loan_mgr.get_loans(borrowed_symbol=borrowed_symbol, is_open=is_open)

    async def get_loan(self, loan_id: str) -> lending.LoanInfo:
        """
        Returns information about a loan.

        :param loan_id: The loan id.
        """
        loan_info = self._loan_mgr.get_loan(loan_id)
        if not loan_info:
            raise errors.NotFound("Loan not found")
        return loan_info

    async def repay_loan(self, loan_id: str):
        """
        Repays a loan fully.

        :param loan_id: The loan id.
        """
        return self._loan_mgr.repay_loan(loan_id)

    def _get_pair_info(self, pair: Pair) -> PairInfo:
        return self._config.get_pair_info(pair)

    async def _on_bar_event(self, event: event.Event):
        assert isinstance(event, bar.BarEvent), f"{event} is not an instance of bar.BarEvent"

        self._prices.on_bar_event(event)
        self._order_mgr.on_bar_event(event)

        # Forward the event if necessary.
        event_source = self._bar_event_source.get(event.bar.pair)
        if event_source:
            event_source.push(event)

    def _get_all_orders(self) -> Sequence[orders.Order]:
        return list(self._order_mgr.get_all_orders())

    def _get_dispatcher(self) -> dispatcher.BacktestingDispatcher:
        return self._dispatcher

    def _get_balance(self, symbol: str) -> Balance:
        available = self._balances.get_available_balance(symbol)
        hold = self._balances.get_balance_on_hold(symbol)
        borrowed = self._balances.get_borrowed_balance(symbol)
        return Balance(
            available=available, hold=hold, borrowed=borrowed
        )
