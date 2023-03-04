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

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

# Strategy based on Bollinger bands.
# https://www.investopedia.com/articles/trading/07/bollinger.asp
# Bars can be downloaded using this command:
# python -m basana.external.bitstamp.tools.download_bars -c btcusd -p day -s 2015-01-01 -e 2015-12-31 \
#   > data/bitstamp_btcusd_day_2015.csv

from decimal import Decimal
import asyncio
import datetime
import logging

from talipp.indicators import BB

from basana.external.bitstamp import csv
import basana as bs
import basana.backtesting.exchange as backtesting_exchange


class TradingSignal(bs.Event):
    def __init__(self, when: datetime.datetime, operation: bs.OrderOperation, pair: bs.Pair):
        super().__init__(when)
        self.operation = operation
        self.pair = pair


class BBands_SignalSource(bs.FifoQueueEventSource):
    def __init__(self, period: int, std_dev_multiplier: float):
        super().__init__()
        self._bb = BB(period, std_dev_multiplier)
        self._prev_value = None

    async def on_bar_event(self, bar_event: bs.BarEvent):
        # Feed the technical indicator.
        value = bar_event.bar.close
        self._bb.add_input_value(float(value))

        previous_value = self._prev_value
        self._prev_value = value

        # Is indicator ready ?
        if len(self._bb) < 2:
            return

        # Price moved below lower band ?
        if self._bb[-2].lb <= previous_value and self._bb[-1].lb > value:
            self.push(TradingSignal(bar_event.when, bs.OrderOperation.BUY, bar_event.bar.pair))
        # Price moved above upper band ?
        elif self._bb[-2].ub >= previous_value and self._bb[-1].ub < value:
            self.push(TradingSignal(bar_event.when, bs.OrderOperation.SELL, bar_event.bar.pair))


# The strategy is responsible for placing orders in response to trading signals.
class Strategy:
    def __init__(self, exchange: backtesting_exchange.Exchange, position_pct: Decimal):
        assert position_pct > 0 and position_pct <= 1
        self._exchange = exchange
        self._position_pct = position_pct

    async def on_trading_signal(self, trading_signal: TradingSignal):
        logging.info("Trading signal: operation=%s pair=%s", trading_signal.operation, trading_signal.pair)
        try:
            # Calculate the order size.
            if trading_signal.operation == bs.OrderOperation.BUY:
                balance, (_, ask) = await asyncio.gather(
                    self._exchange.get_balance(trading_signal.pair.quote_symbol),
                    self._exchange.get_bid_ask(trading_signal.pair)
                )
                order_size = balance.available * self._position_pct / ask
            else:
                balance = await self._exchange.get_balance(trading_signal.pair.base_symbol)
                order_size = balance.available
            order_size = bs.truncate_decimal(order_size, 8)
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
    pair = bs.Pair("BTC", "USD")
    exchange = backtesting_exchange.Exchange(event_dispatcher, initial_balances={"USD": Decimal(10000)})
    exchange.set_pair_info(pair, bs.PairInfo(8, 2))

    # Load bars from CSV files.
    for year in range(2015, 2022):
        exchange.add_bar_source(
            csv.BarSource(pair, f"data/bitstamp_btcusd_day_{year}.csv", csv.BarPeriod.DAY)
        )

    # Bollinger bands will be used to generate trading signals.
    signal_source = BBands_SignalSource(23, 3.1)
    exchange.subscribe_to_bar_events(pair, signal_source.on_bar_event)

    strategy = Strategy(exchange, Decimal("0.95"))
    event_dispatcher.subscribe(signal_source, strategy.on_trading_signal)

    # Run the backtest.
    await event_dispatcher.run()

    # Calculate the portfolio value in USD.
    portfolio_value = Decimal(0)
    balances = await exchange.get_balances()
    for currency, balance in balances.items():
        if currency == "USD":
            price = Decimal(1)
        else:
            _, price = await exchange.get_bid_ask(bs.Pair(currency, "USD"))
        portfolio_value += balance.available * price
    logging.info("Final portfolio value in USD: %s", round(portfolio_value, 2))


if __name__ == "__main__":
    asyncio.run(main())
