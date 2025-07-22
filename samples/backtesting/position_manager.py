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

from collections import defaultdict
from decimal import Decimal
from typing import cast, Dict, Optional
import asyncio
import copy
import dataclasses
import datetime
import logging

from basana.core.logs import StructuredMessage
import basana as bs
import basana.backtesting.exchange as backtesting_exchange


@dataclasses.dataclass
class PositionInfo:
    pair: bs.Pair
    pair_info: bs.PairInfo
    initial: Decimal
    initial_avg_price: Decimal
    target: Decimal
    order: backtesting_exchange.OrderInfo

    def __post_init__(self):
        # Both initial and initial_avg_price should be set to 0, or none of them.
        assert (self.initial == Decimal(0)) is (self.initial_avg_price == Decimal(0)), \
                f"initial={self.initial}, initial_avg_price={self.initial_avg_price}"

    @property
    def current(self) -> Decimal:
        delta = self.order.amount_filled if self.order.operation == bs.OrderOperation.BUY else -self.order.amount_filled
        return self.initial + delta

    @property
    def avg_price(self) -> Decimal:
        # If the current position is 0, then the average price is 0.
        current = self.current
        if current == Decimal(0):
            return Decimal(0)

        # If the current position is not 0, then the order will have a fill price.
        order_fill_price = cast(Decimal, self.order.fill_price)

        # If we're going from a neutral position to a non-neutral position, the order fill price is returned.
        if self.initial == 0:
            ret = order_fill_price
        # If we are closing the position, going from a non-neutral position to a neutral position, the initial average
        # price is returned.
        elif self.target == 0:
            ret = self.initial_avg_price
        # Going from long to short, or the other way around.
        elif self.initial * self.target < 0:
            # If we are on the target side, the order fill price is returned.
            if current * self.target > 0:
                ret = order_fill_price
            # If we're still on the initial side, the initial average price is returned.
            else:
                ret = self.initial_avg_price
        # Rebalancing on the same side.
        else:
            assert self.initial * self.target > 0
            # Reducing the position.
            if self.target > 0 and self.order.operation == bs.OrderOperation.SELL \
                    or self.target < 0 and self.order.operation == bs.OrderOperation.BUY:
                ret = self.initial_avg_price
            # Increasing the position.
            else:
                ret = (abs(self.initial) * self.initial_avg_price + self.order.amount_filled * order_fill_price) \
                    / (abs(self.initial) + self.order.amount_filled)

        return ret

    @property
    def order_open(self) -> bool:
        return self.order.is_open

    @property
    def target_reached(self) -> bool:
        return self.current == self.target

    def calculate_unrealized_pnl_pct(self, bid: Decimal, ask: Decimal) -> Decimal:
        pnl_pct = Decimal(0)
        current = self.current
        avg_price = self.avg_price
        if current and avg_price:
            exit_price = bid if current > 0 else ask
            pnl = (exit_price - avg_price) * current
            pnl_pct = pnl / abs(avg_price * current) * Decimal(100)
        return pnl_pct


class PositionManager:
    # Responsible for managing orders and tracking positions in response to trading signals.
    def __init__(
            self, exchange: backtesting_exchange.Exchange, position_amount: Decimal, quote_symbol: str,
            stop_loss_pct: Decimal, borrowing_disabled: bool = False
    ):
        assert position_amount > 0
        assert stop_loss_pct > 0

        self._exchange = exchange
        self._position_amount = position_amount
        self._quote_symbol = quote_symbol
        self._positions: Dict[bs.Pair, PositionInfo] = {}
        self._pos_mutex: Dict[bs.Pair, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._stop_loss_pct = stop_loss_pct
        self._borrowing_disabled = borrowing_disabled
        self._last_check_loss: Optional[datetime.datetime] = None

    async def get_position_info(self, pair: bs.Pair) -> Optional[PositionInfo]:
        pos_info = self._positions.get(pair)
        if pos_info and pos_info.order_open:
            async with self._pos_mutex[pair]:
                pos_info.order = await self._exchange.get_order_info(pos_info.order.id)
        return copy.deepcopy(pos_info)

    async def check_loss(self):
        positions = []
        coros = [self.get_position_info(pair) for pair in self._positions.keys()]
        if coros:
            positions.extend(await asyncio.gather(*coros))

        # Check unrealized PnL for all non-neutral positions.
        non_neutral = [pos_info for pos_info in positions if pos_info.current != Decimal(0)]
        if not non_neutral:
            return

        # Get bid and ask prices and calculate unrealized PnL.
        bids_and_asks = await asyncio.gather(*[self._exchange.get_bid_ask(pos_info.pair) for pos_info in non_neutral])
        for pos_info, (bid, ask) in zip(non_neutral, bids_and_asks):
            pnl_pct = pos_info.calculate_unrealized_pnl_pct(bid, ask)
            logging.info(StructuredMessage(
                f"Position for {pos_info.pair}", current=pos_info.current, target=pos_info.target,
                order_open=pos_info.order_open,
                avg_price=bs.round_decimal(pos_info.avg_price, pos_info.pair_info.quote_precision),
                pnl_pct=bs.round_decimal(pnl_pct, 2)
            ))
            if pnl_pct <= self._stop_loss_pct * -1:
                logging.info(f"Stop loss for {pos_info.pair}")
                await self.switch_position(pos_info.pair, bs.Position.NEUTRAL, force=True)

    async def switch_position(self, pair: bs.Pair, target_position: bs.Position, force: bool = False):
        current_pos_info = await self.get_position_info(pair)

        # Unless force is set, we can ignore the request if we're already there.
        if not force and any([
                current_pos_info is None and target_position == bs.Position.NEUTRAL,
                (
                    current_pos_info is not None
                    and signed_to_position(current_pos_info.target) == target_position
                    and current_pos_info.target_reached
                )
        ]):
            return

        # Exclusive access to the position since we're going to modify it.
        async with self._pos_mutex[pair]:
            # Cancel the previous order.
            if current_pos_info and current_pos_info.order_open:
                logging.info(StructuredMessage("Canceling order", order_ids=current_pos_info.order.id))
                await self._exchange.cancel_order(current_pos_info.order.id)
                current_pos_info.order = await self._exchange.get_order_info(current_pos_info.order.id)

            (bid, ask), pair_info = await asyncio.gather(
                self._exchange.get_bid_ask(pair),
                self._exchange.get_pair_info(pair),
            )

            # 1. Calculate the target balance.
            # If the target position is neutral, the target balance is 0, otherwise we need to convert
            # self._position_amount, which is expressed in self._quote_symbol units, into base units.
            if target_position == bs.Position.NEUTRAL:
                target = Decimal(0)
            else:
                if pair.quote_symbol == self._quote_symbol:
                    target = self._position_amount / ((bid + ask) / 2)
                else:
                    quote_bid, quote_ask = await self._exchange.get_bid_ask(
                        bs.Pair(pair.base_symbol, self._quote_symbol)
                    )
                    target = self._position_amount / ((quote_bid + quote_ask) / 2)

                if target_position == bs.Position.SHORT:
                    target *= -1
                target = bs.truncate_decimal(target, pair_info.base_precision)

            # 2. Calculate the difference between the target balance and our current balance.
            current = Decimal(0) if current_pos_info is None else current_pos_info.current
            delta = target - current
            logging.info(StructuredMessage("Switch position", pair=pair, current=current, target=target, delta=delta))
            if delta == 0:
                return

            # 3. Create the order.
            order_size = abs(delta)
            operation = bs.OrderOperation.BUY if delta > 0 else bs.OrderOperation.SELL
            logging.info(StructuredMessage(
                "Creating market order", operation=operation, pair=pair, order_size=order_size
            ))
            created_order = await self._exchange.create_market_order(
                operation, pair, order_size, auto_borrow=True, auto_repay=True
            )
            logging.info(StructuredMessage("Order created", id=created_order.id))

            # 4. Keep track of the position.
            initial_avg_price = Decimal(0) if current_pos_info is None else current_pos_info.avg_price
            pos_info = PositionInfo(
                pair=pair, pair_info=pair_info, initial=current, initial_avg_price=initial_avg_price, target=target,
                order=created_order
            )
            self._positions[pair] = pos_info

    async def on_trading_signal(self, trading_signal: bs.TradingSignal):
        pairs = list(trading_signal.get_pairs())
        logging.info(StructuredMessage("Trading signal", pairs=pairs))

        try:
            coros = []
            for pair, target_position in pairs:
                if self._borrowing_disabled and target_position == bs.Position.SHORT:
                    target_position = bs.Position.NEUTRAL
                coros.append(self.switch_position(pair, target_position))
            await asyncio.gather(*coros)
        except Exception as e:
            logging.exception(e)

    async def on_bar_event(self, bar_event: bs.BarEvent):
        bar = bar_event.bar
        logging.info(StructuredMessage(bar.pair, close=bar.close))
        if self._last_check_loss is None or self._last_check_loss < bar_event.when:
            self._last_check_loss = bar_event.when
            await self.check_loss()

    async def on_order_event(self, order_event: backtesting_exchange.OrderEvent):
        order = order_event.order
        logging.info(StructuredMessage(
            "Order updated", id=order.id, is_open=order.is_open, amount=order.amount,
            amount_filled=order.amount_filled, avg_fill_price=order.fill_price
        ))


def signed_to_position(signed):
    if signed > 0:
        return bs.Position.LONG
    elif signed < 0:
        return bs.Position.SHORT
    else:
        return bs.Position.NEUTRAL
