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
# python -m basana.external.binance.tools.download_bars -c BTC/USDT -p 1m -s 2021-01-01 -e 2021-01-31 > \
# binance_btcusdt_min.csv

from decimal import Decimal
import asyncio
import logging

from basana.external.binance import csv
import basana as bs
import basana.backtesting.exchange as backtesting_exchange
import bbands


class PositionManager:
    def __init__(self, exchange: backtesting_exchange.Exchange, position_amount: Decimal):
        assert position_amount > 0
        self._exchange = exchange
        self._position_amount = position_amount

    async def on_trading_signal(self, trading_signal: bs.TradingSignal):
        logging.info("Trading signal: operation=%s pair=%s", trading_signal.operation, trading_signal.pair)
        try:
            # Calculate the order size.
            balances = await self._exchange.get_balances()
            if trading_signal.operation == bs.OrderOperation.BUY:
                _, ask = await self._exchange.get_bid_ask(trading_signal.pair)
                balance = balances[trading_signal.pair.quote_symbol]
                order_size = min(self._position_amount, balance.available) / ask
            else:
                balance = balances[trading_signal.pair.base_symbol]
                order_size = balance.available
            pair_info = await self._exchange.get_pair_info(trading_signal.pair)
            order_size = bs.truncate_decimal(order_size, pair_info.base_precision)
            if not order_size:
                return

            logging.info(
                "Creating %s market order for %s: amount=%s", trading_signal.operation, trading_signal.pair, order_size
            )
            await self._exchange.create_market_order(trading_signal.operation, trading_signal.pair, order_size)
        except Exception as e:
            logging.error(e)


async def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s")

    event_dispatcher = bs.backtesting_dispatcher()
    pair = bs.Pair("BTC", "USDT")
    exchange = backtesting_exchange.Exchange(
        event_dispatcher,
        initial_balances={"BTC": Decimal(0), "USDT": Decimal(10000)}
    )
    exchange.set_pair_info(pair, bs.PairInfo(8, 2))

    # Connect the strategy to the bar events from the exchange.
    strategy = bbands.Strategy(event_dispatcher, 20, 1.5)
    exchange.subscribe_to_bar_events(pair, strategy.on_bar_event)

    # Connect the position manager to the strategy signals.
    position_mgr = PositionManager(exchange, Decimal(1000))
    strategy.subscribe_to_trading_signals(position_mgr.on_trading_signal)

    # Load bars from CSV files.
    exchange.add_bar_source(csv.BarSource(pair, "binance_btcusdt_min.csv", "1m"))

    # Run the backtest.
    await event_dispatcher.run()

    # Log balances.
    balances = await exchange.get_balances()
    for currency, balance in balances.items():
        logging.info("%s balance: %s", currency, balance.available)


if __name__ == "__main__":
    asyncio.run(main())
