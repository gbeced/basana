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
from typing import cast, Callable, Dict, Generator, Iterable, List, Optional
import dataclasses
import decimal
import logging

from basana.backtesting import account_balances, config, errors, fees, helpers, lending, loan_mgr, liquidity, prices
from basana.backtesting.orders import Order
from basana.backtesting.value_map import ValueMap, ValueMapDict
from basana.core import bar, dispatcher, helpers as core_helpers, logs
from basana.core.enums import OrderOperation
from basana.core.pair import Pair


logger = logging.getLogger(__name__)

LiquidityStrategyFactory = Callable[[], liquidity.LiquidityStrategy]


@dataclasses.dataclass
class ExchangeContext:
    dispatcher: dispatcher.EventDispatcher
    account_balances: account_balances.AccountBalances
    prices: prices.Prices
    fee_strategy: fees.FeeStrategy
    liquidity_strategy_factory: LiquidityStrategyFactory
    loan_mgr: loan_mgr.LoanManager
    config: config.Config


class OrderManager:
    def __init__(self, exchange_ctx: ExchangeContext):
        self._ctx = exchange_ctx
        self._liquidity_strategies: Dict[Pair, liquidity.LiquidityStrategy] = {}
        self._orders = helpers.ExchangeObjectContainer[Order]()
        self._holds_by_order: Dict[str, ValueMap] = {}

    def on_bar_event(self, bar_event: bar.BarEvent):
        if (liquidity_strategy := self._liquidity_strategies.get(bar_event.bar.pair)) is None:
            liquidity_strategy = self._ctx.liquidity_strategy_factory()
        liquidity_strategy.on_bar(bar_event.bar)
        for order in filter(lambda o: o.pair == bar_event.bar.pair, self._orders.get_open()):
            self._process_order(order, bar_event, liquidity_strategy)

    def add_order(self, order: Order):
        try:
            # When an order gets accepted we need to hold any required balance that will be debited as the order gets
            # filled.
            if required_balances := self._estimate_required_balances(order):
                if order.auto_borrow:
                    self._borrow(required_balances, order)

                self._ctx.account_balances.update(hold_updates=required_balances)
                self._holds_by_order[order.id] = required_balances

            self._orders.add(order)

        except errors.NotEnoughBalance as e:
            logger.debug(logs.StructuredMessage(
                "Not enough balance to accept order", order=order.get_debug_info(), error=str(e)
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
        self._order_closed(order)

    def _update_balances(self, order: Order, balance_updates: ValueMapDict):
        # If we have holds associated with the order, it may be time to release some/all of those.
        hold_updates = {}
        order_holds = self._holds_by_order.get(order.id, ValueMap())
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
            self._ctx.account_balances.update(balance_updates=balance_updates, hold_updates=hold_updates)

        # Update holds by order.
        if order_holds:
            if order.is_open:
                order_holds += hold_updates
            else:
                del self._holds_by_order[order.id]

    def _borrow(self, required_balances: ValueMap, order: Order):
        post_hold = {
            symbol: self._ctx.account_balances.get_available_balance(symbol) - required_amount
            for symbol, required_amount in required_balances.items()
        }
        balances_short = {
            symbol: -amount for symbol, amount in post_hold.items() if amount < Decimal(0)
        }

        loan_ids: List[str] = []
        try:
            # Create a loan for every balance that we're short on.
            for symbol, amount in balances_short.items():
                logger.debug(logs.StructuredMessage(
                    "Borrowing for order", order_id=order.id, symbol=symbol, amount=amount
                ))
                loan = self._ctx.loan_mgr.create_loan(symbol, amount)
                loan_ids.append(loan.id)
        except Exception as e:
            logger.debug(logs.StructuredMessage(
                "Failed to borrow", order_id=order.id, symbol=symbol, amount=amount, error=str(e)
            ))
            # If there are any errors borrowing money we'll rollback everything before propagating the exception.
            for loan_id in loan_ids:
                logger.debug(logs.StructuredMessage("Canceling loan", order_id=order.id, loan_id=loan_id))
                self._ctx.loan_mgr.cancel_loan(loan_id)
            raise

    def _repay_loans(self, symbol: str):
        candidate_loans = [
            loan for loan in self._ctx.loan_mgr.get_loans(is_open=True)
            if loan.borrowed_symbol == symbol
        ]
        # Try to cancel bigger loans first.
        candidate_loans.sort(key=lambda loan: loan.borrowed_amount, reverse=True)
        for loan in candidate_loans:
            try:
                self._ctx.loan_mgr.repay_loan(loan.id)
                loan = cast(lending.LoanInfo, self._ctx.loan_mgr.get_loan(loan.id))
                logger.debug(logs.StructuredMessage("Repayed loan", loan=dataclasses.asdict(loan)))
            except errors.NotEnoughBalance:
                pass

    def _order_closed(self, order: Order):
        # The order is closed and there might be balances on hold that have to be released.
        self._update_balances(order, {})
        # If the order has auto_repay set and is filled, either fully or partially, then we need to cancel any open
        # loans matching the order's base/quote currency.
        if order.auto_repay and order.amount_filled:
            if order.operation == OrderOperation.BUY:
                self._repay_loans(order.pair.base_symbol)
            else:
                self._repay_loans(order.pair.quote_symbol)

    def _process_order(
            self, order: Order, bar_event: bar.BarEvent, liquidity_strategy: liquidity.LiquidityStrategy
    ):
        def order_not_filled():
            order.not_filled()
            logger.debug(logs.StructuredMessage("Order not filled", order_id=order.id, order_state=order.state))
            if not order.is_open:
                self._order_closed(order)

        # Calculate balance updates for the current bar.
        logger.debug(logs.StructuredMessage(
            "Processing order", order=order.get_debug_info(),
            bar={
                "open": bar_event.bar.open, "high": bar_event.bar.high, "low": bar_event.bar.low,
                "close": bar_event.bar.close, "volume": bar_event.bar.volume,
            }
        ))
        prev_state = order.state
        balance_updates = ValueMap(order.get_balance_updates(bar_event.bar, liquidity_strategy))
        assert order.state == prev_state, "The order state should not change inside get_balance_updates"
        self._round_balance_updates(balance_updates, order.pair)
        logger.debug(logs.StructuredMessage(
            "Order balance updates", order_id=order.id, balance_updates=balance_updates
        ))
        if order.pair.base_symbol not in balance_updates or order.pair.quote_symbol not in balance_updates:
            order_not_filled()
            return

        # Get fees, round them, and combine them with the balance updates.
        fees = ValueMap(self._ctx.fee_strategy.calculate_fees(order, balance_updates))
        self._round_fees(fees, order.pair)
        logger.debug(logs.StructuredMessage(
            "Order fees", order_id=order.id, fees=fees
        ))

        try:
            # Update balances. This may fail if there is not enough balance, so we do this first.
            final_updates = balance_updates + fees
            final_updates.prune()
            self._update_balances(order, final_updates)
            # Update the liquidity strategy.
            liquidity_strategy.take_liquidity(abs(balance_updates[bar_event.bar.pair.base_symbol]))
            # Update the order and release any pending balance on hold if the order is now closed.
            order.add_fill(bar_event.when, balance_updates, fees)
            logger.debug(logs.StructuredMessage(
                "Order updated", order_id=order.id, final_updates=final_updates, order_state=order.state
            ))

            if not order.is_open:
                self._order_closed(order)

        except errors.NotEnoughBalance as e:
            logger.debug(logs.StructuredMessage(
                "Balance short processing order", order=order.get_debug_info(), error=str(e)
            ))
            order_not_filled()

    def _round_balance_updates(self, balance_updates: ValueMap, pair: Pair):
        pair_info = self._ctx.config.get_pair_info(pair)

        # For the base amount we truncate instead of rounding to avoid exceeding available liquidity.
        if (base_amount := balance_updates.get(pair.base_symbol)) is not None:
            balance_updates[pair.base_symbol] = core_helpers.truncate_decimal(base_amount, pair_info.base_precision)

        # For the quote amount we simply round.
        if (quote_amount := balance_updates.get(pair.quote_symbol)) is not None:
            balance_updates[pair.quote_symbol] = core_helpers.round_decimal(quote_amount, pair_info.quote_precision)

        balance_updates.prune()

    def _round_fees(self, fees: ValueMap, pair: Pair):
        pair_info = self._ctx.config.get_pair_info(pair)
        precisions = {
            pair.base_symbol: pair_info.base_precision,
            pair.quote_symbol: pair_info.quote_precision,
        }
        for symbol, precision in precisions.items():
            if (amount := fees.get(symbol)) is not None:
                fees[symbol] = core_helpers.round_decimal(amount, precision, rounding=decimal.ROUND_UP)

        fees.prune()

    def _estimate_required_balances(self, order: Order) -> ValueMap:
        # Get an estimated fill price. If the order can't provide one, use the last price available.
        estimated_fill_price = order.calculate_estimated_fill_price()
        if not estimated_fill_price:
            try:
                estimated_fill_price = self._ctx.prices.get_price(order.pair)
            except errors.NoPrice:
                pass

        # Build a dictionary of balance updates suitable for calculating fees.
        base_sign = helpers.get_base_sign_for_operation(order.operation)
        estimated_balance_updates = ValueMap({
            order.pair.base_symbol: order.amount * base_sign
        })
        if estimated_fill_price:
            estimated_balance_updates[order.pair.quote_symbol] = order.amount * estimated_fill_price * -base_sign
        self._round_balance_updates(estimated_balance_updates, order.pair)

        # Calculate fees.
        fees = ValueMap()
        if len(estimated_balance_updates) == 2:
            fees += self._ctx.fee_strategy.calculate_fees(order, estimated_balance_updates)
            self._round_fees(fees, order.pair)
        estimated_balance_updates += fees

        # Return only negative balance updates as required balances.
        return ValueMap({
            symbol: -amount for symbol, amount in estimated_balance_updates.items()
            if amount < Decimal(0)
        })
