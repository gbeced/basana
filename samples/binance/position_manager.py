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
from typing import Dict, Optional
import asyncio
import dataclasses
import datetime
import json
import logging

from basana.core.logs import StructuredMessage
from basana.external.binance import exchange, spot
import basana as bs


@dataclasses.dataclass
class PositionInfo:
    pair: bs.Pair
    initial: Decimal
    initial_avg_price: Decimal
    target: Decimal
    order: spot.OrderInfo

    @property
    def current(self) -> Decimal:
        delta = self.order.amount_filled if self.order.operation == bs.OrderOperation.BUY else -self.order.amount_filled
        return self.initial + delta

    @property
    def avg_price(self) -> Decimal:
        order_fill_price = Decimal(0) if self.order.fill_price is None else self.order.fill_price
        ret = order_fill_price

        if self.initial == 0:
            ret = order_fill_price
        # Transition from long to short, or viceversa, and already on the target side.
        elif self.initial * self.target < 0 and self.current * self.target > 0:
            ret = order_fill_price
        # Rebalancing on the same side.
        elif self.initial * self.target > 0:
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


class SpotAccountPositionManager:
    # Responsible for managing orders and tracking positions in response to trading signals.
    def __init__(
            self, exchange: exchange.Exchange, position_amount: Decimal, quote_symbol: str,
            stop_loss_pct: Decimal, checkpoint_fname: str
    ):
        assert position_amount > 0
        assert stop_loss_pct > 0

        self._exchange = exchange
        self._position_amount = position_amount
        self._quote_symbol = quote_symbol
        self._positions: Dict[bs.Pair, PositionInfo] = {}
        self._stop_loss_pct = stop_loss_pct
        self._checkpoint_fname = checkpoint_fname
        self._last_check_loss: Optional[datetime.datetime] = None

    def save(self):
        with open(self._checkpoint_fname, "w") as f:
            json_dict = {
                str(pair): dataclasses.asdict(pos_info) for pair, pos_info in self._positions.items()
            }
            for pos_info in json_dict.values():
                pos_info["order"] = pos_info["order"].json

            json.dump(json_dict, f, default=str)

    def load(self):
        with open(self._checkpoint_fname) as f:
            json_dict = json.load(f)
            json_dict = {
                bs.Pair(*pair.split("/")): PositionInfo(
                    pair=bs.Pair(*pair.split("/")),
                    initial=Decimal(pos_info["initial"]),
                    initial_avg_price=Decimal(pos_info["initial_avg_price"]),
                    target=Decimal(pos_info["target"]),
                    order=spot.OrderInfo(pos_info["order"], []),
                ) for pair, pos_info in json_dict.items()
            }
            self._positions = json_dict

    async def cancel_open_orders(self, pair: bs.Pair):
        open_orders = await self._exchange.spot_account.get_open_orders(pair)
        await asyncio.gather(*[
            self._exchange.spot_account.cancel_order(pair, order_id=open_order.id)
            for open_order in open_orders
        ])

    async def get_position_info(self, pair: bs.Pair) -> Optional[PositionInfo]:
        pos_info = self._positions.get(pair)
        if pos_info and pos_info.order_open:
            pos_info.order = await self._exchange.spot_account.get_order_info(pair, order_id=pos_info.order.id)
            self.save()
        return pos_info

    async def check_loss(self):
        pairs = [pos_info.pair for pos_info in self._positions.values() if pos_info.current != 0]
        # For every pair get position information along with bid and ask prices.
        coros = [self.get_position_info(pair) for pair in pairs]
        coros.extend(self._exchange.get_bid_ask(pair) for pair in pairs)
        res = await asyncio.gather(*coros)
        midpoint = int(len(res) / 2)
        all_pos_info = res[0:midpoint]
        all_bid_ask = res[midpoint:]

        # Log each position an check PnL.
        for pos_info, (bid, ask) in zip(all_pos_info, all_bid_ask):
            pnl_pct = pos_info.calculate_unrealized_pnl_pct(bid, ask)
            logging.info(StructuredMessage(
                f"Position for {pos_info.pair}", current=pos_info.current, target=pos_info.target,
                avg_price=pos_info.avg_price, pnl_pct=pnl_pct, order_open=pos_info.order_open
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

        # Cancel the previous order.
        if current_pos_info and current_pos_info.order_open:
            await self._exchange.spot_account.cancel_order(pair, order_id=current_pos_info.order.id)
            current_pos_info.order = await self._exchange.spot_account.get_order_info(
                pair, order_id=current_pos_info.order.id
            )

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
                quote_bid, quote_ask = await self._exchange.get_bid_ask(bs.Pair(pair.base_symbol, self._quote_symbol))
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
        logging.info(StructuredMessage("Creating market order", operation=operation, pair=pair, order_size=order_size))
        created_order = await self._exchange.spot_account.create_market_order(operation, pair, order_size)
        order = await self._exchange.spot_account.get_order_info(pair, order_id=created_order.id)

        # 4. Keep track of the position.
        initial_avg_price = Decimal(0) if current_pos_info is None else current_pos_info.avg_price
        pos_info = PositionInfo(
            pair=pair, initial=current, initial_avg_price=initial_avg_price, target=target, order=order
        )
        self._positions[pair] = pos_info
        self.save()

    async def on_trading_signal(self, trading_signal: bs.TradingSignal):
        pairs = list(trading_signal.get_pairs())
        logging.info(StructuredMessage("Trading signal", pairs=pairs))

        try:
            coros = []
            for pair, target_position in pairs:
                # No borrowing with spot account.
                if target_position == bs.Position.SHORT:
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


def signed_to_position(signed):
    if signed > 0:
        return bs.Position.LONG
    elif signed < 0:
        return bs.Position.SHORT
    else:
        return bs.Position.NEUTRAL
