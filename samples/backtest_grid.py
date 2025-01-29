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

import asyncio
# Bars can be downloaded using this command:
# python -m basana.external.binance.tools.download_bars -c BTC/USDT -p 1d -s 2021-01-01 -e 2021-12-31 \
# -o binance_btcusdt_day.csv
import dataclasses
import datetime
import logging
from decimal import Decimal
from typing import Dict, Optional, List

import basana as bs
import basana.backtesting.exchange as backtesting_exchange
from basana import OrderOperation, Pair
from basana.backtesting import charts, lending
from basana.core.event_sources.trading_signal import BaseTradingSignal
from basana.core.logs import StructuredMessage
from basana.external.binance import csv
from samples.configs.grid_config import Config
from samples.strategies import bbands


@dataclasses.dataclass
class PositionInfo:
    """
    PositionInfo class
    """
    pair: Pair
    signal_position: bs.Position


class Point:
    def __init__(self, pair: Pair):
        """
        Init Point instance

        @param pair:
        """
        self._value = 0.0001 if pair.quote_symbol.lower() != "JPY".lower() else 0.01

    @property
    def value(self):
        return self._value


class GridPositionManager:
    """
    GridPositionManager class
    """

    # Responsible for managing orders and tracking positions in response to trading signals.
    def __init__(
            self, exchange: backtesting_exchange.Exchange, position_amount: Decimal, grid_size: int, grid_step: int,
            hedging: bool, martingale_sizes: bool, borrowing_disabled: bool = False
    ):
        """
        Init GridPositionManager instance

        Manage a grid system

        @param exchange: Exchange instance
        @param position_amount: Amount of base value to trade
        @param grid_size: Grid number of lines above and below the start price
        @param grid_step: Space in Pips between grid lines
        @param hedging: True to take hedging orders
        @param martingale_sizes: True to increase position sizes in grid
        @param borrowing_disabled: Disable loans on platform (ie: do not use short trades), default= False
        """
        assert position_amount > 0

        # configuration
        self._exchange = exchange
        self._position_amount = position_amount
        self._grid_size = grid_size
        self._grid_step = grid_step
        self._hedging = hedging
        self._martingale_sizes = martingale_sizes
        self._borrowing_disabled = borrowing_disabled

        # storage lists
        self._positions: Dict[bs.Pair, PositionInfo] = {}
        self._buy_orders_ids: List[str] = []
        self._sell_orders_ids: List[str] = []

    async def on_trading_signal(self, trading_signal: BaseTradingSignal):
        """
        Handle trading signals

        @param trading_signal: TradingSignal instance
        """
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

    async def switch_position(self, pair: bs.Pair, target_position: bs.Position, force: bool = False):
        """
        Handle change in trading position (SHORT, LONG, NEUTRAL)

        @param pair: Pair instance
        @param target_position: New position
        @param force: True if user want to execute even if old position == new position
        @return:
        """
        current_pos_info = await self.get_position_info(pair)

        # Unless force is set, we can ignore the request if we're already there.
        if not force and any([
            # current_pos_info is None and target_position == bs.Position.NEUTRAL,
            (
                    current_pos_info is not None
                    and current_pos_info == target_position
            )
        ]):
            return
        self._positions[pair] = PositionInfo(pair, target_position)

        if target_position != bs.Position.NEUTRAL:
            return

        # SIGNAL NEUTRAL (BBands cross the middle)
        buy_order_ids = [order.id for order in await self._exchange.get_open_orders(pair) if
                         order.operation == OrderOperation.BUY]

        if len(buy_order_ids) == 0:

            (bid, ask), pair_info = await asyncio.gather(
                self._exchange.get_bid_ask(pair),
                self._exchange.get_pair_info(pair),
            )
            order_size = bs.truncate_decimal(Decimal(self._position_amount), pair_info.base_precision)
            grid_step_from_pips = Decimal(self._grid_step * Point(pair).value)

            # place initial buy limit orders
            for i in range(self._grid_size):
                price = bs.truncate_decimal(Decimal(bid - (grid_step_from_pips * (i + 1))), pair_info.quote_precision)
                if self._martingale_sizes is True:
                    order_size = bs.truncate_decimal(Decimal(self._position_amount * (i + 1)), pair_info.base_precision)
                operation = bs.OrderOperation.BUY
                logging.info(
                    StructuredMessage("Creating BUY limit order", operation=operation, pair=pair, order_size=order_size,
                                      limit_price=price))
                created_order = await self._exchange.create_limit_order(
                    operation, pair, order_size, price, auto_borrow=True, auto_repay=True
                )
                self._buy_orders_ids.append(created_order.id)

            # place initial sell limit orders
            for i in range(self._grid_size):
                price = bs.truncate_decimal(Decimal(bid + (grid_step_from_pips * (i + 1))), pair_info.quote_precision)
                if self._martingale_sizes is True:
                    order_size = bs.truncate_decimal(Decimal(self._position_amount * (i + 1)), pair_info.base_precision)
                operation = bs.OrderOperation.SELL
                logging.info(
                    StructuredMessage("Creating SELL limit order", operation=operation, pair=pair,
                                      order_size=order_size,
                                      limit_price=price))
                created_order = await self._exchange.create_limit_order(
                    operation, pair, order_size, price, auto_borrow=True, auto_repay=True
                )
                self._sell_orders_ids.append(created_order.id)

    async def get_position_info(self, pair: bs.Pair) -> Optional[PositionInfo]:
        """
        Return position info of pair

        @param pair: Pair instance
        @return:
        """
        pos_info = self._positions.get(pair)
        return pos_info

    async def on_bar_event(self, bar_event: bs.BarEvent):
        """
        Handle Bar events

        @param bar_event: BarEvent instance
        @return:
        """
        bar = bar_event.bar
        pair = bar.pair
        grid_step_from_pips = Decimal(self._grid_step * Point(pair).value)

        logging.info(StructuredMessage(pair, close=bar.close))

        filled_orders = await self._exchange.get_orders(pair, is_open=False)
        for order_info in filled_orders:
            pair_info = await self._exchange.get_pair_info(pair)

            # Buy order filled...
            if order_info.operation == OrderOperation.BUY and order_info.id in self._buy_orders_ids:
                self._buy_orders_ids.remove(order_info.id)
                if self._hedging is True and order_info.limit_price is not None:
                    price = Decimal(
                        order_info.fill_price if order_info.fill_price is not None else order_info.limit_price)
                    new_sell_price = bs.truncate_decimal(price + grid_step_from_pips, pair_info.quote_precision)
                    order_size = bs.truncate_decimal(Decimal(self._position_amount), pair_info.base_precision)
                    operation = bs.OrderOperation.SELL
                    logging.info(
                        StructuredMessage("Creating SELL limit order", operation=operation, pair=pair,
                                          order_size=order_size, limit_price=new_sell_price))
                    created_order = await self._exchange.create_limit_order(
                        operation, pair, order_size, new_sell_price, auto_borrow=True, auto_repay=True
                    )
                    self._sell_orders_ids.append(created_order.id)

            # Sell order filled...
            elif order_info.operation == OrderOperation.SELL and order_info.id in self._sell_orders_ids:
                self._sell_orders_ids.remove(order_info.id)
                if self._hedging is True and order_info.limit_price is not None:
                    price = Decimal(
                        order_info.fill_price if order_info.fill_price is not None else order_info.limit_price)
                    new_buy_price = bs.truncate_decimal(price - grid_step_from_pips, pair_info.quote_precision)
                    order_size = bs.truncate_decimal(Decimal(self._position_amount), pair_info.base_precision)
                    operation = bs.OrderOperation.BUY
                    logging.info(
                        StructuredMessage("Creating BUY limit order", operation=operation, pair=pair,
                                          order_size=order_size, limit_price=new_buy_price))
                    created_order = await self._exchange.create_limit_order(
                        operation, pair, order_size, new_buy_price, auto_borrow=True, auto_repay=True
                    )
                    self._buy_orders_ids.append(created_order.id)

        # if no sell orders are opened...
        sell_open_order_ids = [order.id for order in await self._exchange.get_open_orders(pair) if
                               order.operation == OrderOperation.SELL]
        if len(sell_open_order_ids) == 0:
            # cancel all open buy orders
            await self.cancel_open_orders(pair)

    async def cancel_open_orders(self, pair: bs.Pair):
        """
        Cancel all orders

        @param pair: Pair instance
        @return:
        """
        open_orders = await self._exchange.get_open_orders(pair)
        await asyncio.gather(*[
            self._exchange.cancel_order(open_order.id)
            for open_order in open_orders
        ])


async def main():
    logging.basicConfig(level=logging.ERROR, format="[%(asctime)s %(levelname)s] %(message)s")

    event_dispatcher = bs.backtesting_dispatcher()

    # We'll be opening short positions, so we need to set a lending strategy when initializing the exchange.
    lending_strategy = lending.MarginLoans(Config.PAIR.quote_symbol, default_conditions=lending.MarginLoanConditions(
        interest_symbol=Config.PAIR.quote_symbol, interest_percentage=Decimal(Config.loan_interest_percentage),
        interest_period=datetime.timedelta(days=Config.loan_interest_period),
        min_interest=Decimal(Config.loan_min_interest),
        margin_requirement=Decimal(Config.loan_margin_requirement)
    ))
    exchange = backtesting_exchange.Exchange(
        event_dispatcher,
        initial_balances={Config.PAIR.quote_symbol: Decimal(Config.initial_balance)},
        lending_strategy=lending_strategy,
    )
    exchange.set_symbol_precision(Config.PAIR.base_symbol, Config.base_symbol_precision)
    exchange.set_symbol_precision(Config.PAIR.quote_symbol, Config.quote_symbol_precision)
    exchange.add_bar_source(csv.BarSource(Config.PAIR, Config.bars_csv_path, Config.bars_period))

    # Connect the strategy to the bar events from the exchange.
    strategy = bbands.Strategy(event_dispatcher, period=Config.BBAND_PERIOD, std_dev=Config.BBAND_STANDARD_DEVIATION)
    exchange.subscribe_to_bar_events(Config.PAIR, strategy.on_bar_event)

    # Connect the position manager to the strategy signals and to bar events.
    position_mgr = GridPositionManager(
        exchange, Config.DEFAULT_POSITION_SIZE, Config.GRID_SIZE, Config.GRID_STEP_SIZE, Config.HEDGING,
        Config.MARTINGALE_SIZES
    )
    # no signals
    strategy.subscribe_to_trading_signals(position_mgr.on_trading_signal)
    exchange.subscribe_to_bar_events(Config.PAIR, position_mgr.on_bar_event)

    # Setup chart.
    chart = charts.LineCharts(exchange)
    chart.add_pair(Config.PAIR)
    chart.add_pair_indicator(
        "Upper", Config.PAIR, lambda _: strategy.bb[-1].ub if len(strategy.bb) and strategy.bb[-1] else None
    )
    chart.add_pair_indicator(
        "Central", Config.PAIR, lambda _: strategy.bb[-1].cb if len(strategy.bb) and strategy.bb[-1] else None
    )
    chart.add_pair_indicator(
        "Lower", Config.PAIR, lambda _: strategy.bb[-1].lb if len(strategy.bb) and strategy.bb[-1] else None
    )
    chart.add_balance(Config.PAIR.base_symbol)
    chart.add_balance(Config.PAIR.quote_symbol)
    chart.add_portfolio_value(Config.PAIR.quote_symbol)

    # Run the backtest.
    await event_dispatcher.run()

    # Log balances.
    balances = await exchange.get_balances()
    for currency, balance in balances.items():
        logging.info(StructuredMessage(f"{currency} balance", available=balance.available))

    chart.show()


if __name__ == "__main__":
    asyncio.run(main())
