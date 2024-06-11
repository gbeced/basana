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

# Bars can be downloaded using this command:
# python -m basana.external.binance.tools.download_bars -c BTC/USDT -p 1d -s 2021-01-01 -e 2021-12-31 \
# -o binance_btcusdt_day.csv

from decimal import Decimal
from typing import Dict, Optional
import asyncio
import dataclasses
import datetime
import logging

from basana.backtesting import charts, margin
from basana.core.logs import StructuredMessage
from basana.external.binance import csv
import basana as bs
import basana.backtesting.exchange as backtesting_exchange

from . import bbands


@dataclasses.dataclass
class PositionInfo:
    pair: bs.Pair
    initial: Decimal
    initial_avg_price: Decimal
    target: Decimal
    order: backtesting_exchange.OrderInfo

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
            ret = self.order.fill_price
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


class PositionManager:
    # Responsible for managing orders and tracking positions in response to trading signals.
    def __init__(
            self, exchange: backtesting_exchange.Exchange, position_amount: Decimal, quote_symbol: str,
            stop_loss_pct: Decimal
    ):
        assert position_amount > 0
        assert stop_loss_pct > 0

        self._exchange = exchange
        self._position_amount = position_amount
        self._quote_symbol = quote_symbol
        self._positions: Dict[bs.Pair, PositionInfo] = {}
        self._stop_loss_pct = stop_loss_pct

    async def cancel_open_orders(self, pair: bs.Pair):
        open_orders = await self._exchange.get_open_orders(pair)
        await asyncio.gather(*[
            self._exchange.cancel_order(open_order.id)
            for open_order in open_orders
        ])

    async def get_position_info(self, pair: bs.Pair) -> Optional[PositionInfo]:
        pos_info = self._positions.get(pair)
        if pos_info and pos_info.order_open:
            pos_info.order = await self._exchange.get_order_info(pos_info.order.id)
        return pos_info

    async def log_positions(self):
        coros = [self.get_position_info(pair) for pair in self._positions.keys()]
        coros.extend(self._exchange.get_bid_ask(pair) for pair in self._positions.keys())
        res = await asyncio.gather(*coros)
        midpoint = int(len(res) / 2)
        all_pos_info = res[0:midpoint]
        all_bid_ask = res[midpoint:]

        for pos_info, (bid, ask) in zip(all_pos_info, all_bid_ask):
            pnl_pct = pos_info.calculate_unrealized_pnl_pct(bid, ask)
            logging.info(StructuredMessage(
                f"Position for {pos_info.pair}", current=pos_info.current, target=pos_info.target,
                avg_price=pos_info.avg_price, pnl_pct=pnl_pct, order_open=pos_info.order_open
            ))
            if pnl_pct < self._stop_loss_pct * -1:
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
        created_order = await self._exchange.create_market_order(
            operation, pair, order_size, auto_borrow=True, auto_repay=True
        )
        order = await self._exchange.get_order_info(created_order.id)

        # 4. Keep track of the position.
        initial_avg_price = Decimal(0) if current_pos_info is None else current_pos_info.avg_price
        pos_info = PositionInfo(
            pair=pair, initial=current, initial_avg_price=initial_avg_price, target=target, order=order
        )
        self._positions[pair] = pos_info

    async def on_trading_signal(self, trading_signal: bs.TradingSignal):
        logging.info(StructuredMessage(
            "Trading signal", pair=trading_signal.pair, target_position=trading_signal.position
        ))

        try:
            await self.switch_position(trading_signal.pair, trading_signal.position)
        except Exception as e:
            logging.exception(e)

    async def on_bar_event(self, bar_event: bs.BarEvent):
        bar = bar_event.bar
        logging.info(StructuredMessage(bar.pair, close=bar.close))
        await self.log_positions()


def signed_to_position(signed):
    if signed > 0:
        return bs.Position.LONG
    elif signed < 0:
        return bs.Position.SHORT
    else:
        return bs.Position.NEUTRAL


async def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s")

    event_dispatcher = bs.backtesting_dispatcher()
    quote_symbol = "USDT"
    pair = bs.Pair("BTC", quote_symbol)
    lending_strategy = margin.MarginLoans(quote_symbol, default_conditions=margin.MarginLoanConditions(
        interest_symbol=quote_symbol, interest_percentage=Decimal("7"),
        interest_period=datetime.timedelta(days=365), min_interest=Decimal("0.01"),
        margin_requirement=Decimal("0.5")
    ))
    exchange = backtesting_exchange.Exchange(
        event_dispatcher,
        initial_balances={"BTC": Decimal(0), quote_symbol: Decimal(1200)},
        lending_strategy=lending_strategy,
    )
    exchange.set_pair_info(pair, bs.PairInfo(8, 2))
    exchange.set_symbol_precision(quote_symbol, 2)

    # Connect the strategy to the bar events from the exchange.
    strategy = bbands.Strategy(event_dispatcher, 30, 2)
    exchange.subscribe_to_bar_events(pair, strategy.on_bar_event)

    # Connect the position manager to the strategy signals and to bar events.
    position_mgr = PositionManager(exchange, Decimal(1000), quote_symbol, Decimal(5))
    strategy.subscribe_to_trading_signals(position_mgr.on_trading_signal)
    exchange.subscribe_to_bar_events(pair, position_mgr.on_bar_event)

    # Load bars from CSV file.
    exchange.add_bar_source(csv.BarSource(pair, "binance_btcusdt_day.csv", "1d"))

    # Setup chart.
    chart = charts.LineCharts(exchange)
    chart.add_pair(pair)
    chart.add_pair_indicator(
        "Upper", pair, lambda _: strategy.bb[-1].ub if len(strategy.bb) and strategy.bb[-1] else None
    )
    chart.add_pair_indicator(
        "Central", pair, lambda _: strategy.bb[-1].cb if len(strategy.bb) and strategy.bb[-1] else None
    )
    chart.add_pair_indicator(
        "Lower", pair, lambda _: strategy.bb[-1].lb if len(strategy.bb) and strategy.bb[-1] else None
    )
    chart.add_portfolio_value("USDT")

    # Run the backtest.
    await event_dispatcher.run()

    # Log balances.
    balances = await exchange.get_balances()
    for currency, balance in balances.items():
        logging.info("%s balance: %s", currency, balance.available)

    chart.show()


if __name__ == "__main__":
    asyncio.run(main())
