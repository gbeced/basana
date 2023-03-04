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

# Strategy based on RSI.
# Bars can be downloaded using this command:
# python -m basana.external.bitstamp.tools.download_bars -c btcusd -p day -s 2015-01-01 -e 2021-12-31 \
#   > bitstamp_btcusd_day.csv

from decimal import Decimal
import asyncio
import datetime
import logging

from talipp.indicators import RSI

from basana.external.bitstamp import csv
import basana as bs
import basana.backtesting.exchange as backtesting_exchange


class TradingSignal(bs.Event):
    def __init__(self, when: datetime.datetime, operation: bs.OrderOperation, pair: bs.Pair):
        super().__init__(when)
        self.operation = operation
        self.pair = pair


class RSI_SignalSource(bs.FifoQueueEventSource):
    def __init__(self, period: int, overbought_level: Decimal, oversold_level: Decimal):
        super().__init__()
        self._overbought_level = overbought_level
        self._oversold_level = oversold_level
        self._rsi = RSI(period=period)

    async def on_bar_event(self, bar_event: bs.BarEvent):
        self._rsi.add_input_value(float(bar_event.bar.close))

        # Is RSI ready ?
        if len(self._rsi) < 2:
            return

        # RSI crossed below oversold level
        if self._rsi[-2] >= self._oversold_level and self._rsi[-1] < self._oversold_level:
            self.push(TradingSignal(bar_event.when, bs.OrderOperation.BUY, bar_event.bar.pair))
        # RSI crossed above overbought level
        elif self._rsi[-2] <= self._overbought_level and self._rsi[-1] > self._overbought_level:
            self.push(TradingSignal(bar_event.when, bs.OrderOperation.SELL, bar_event.bar.pair))


# The strategy is responsible for placing orders in response to trading signals.
class Strategy:
    def __init__(self, exchange: backtesting_exchange.Exchange, position_pct: Decimal):
        assert position_pct > 0 and position_pct <= 1
        self._exchange = exchange
        self._position_pct = position_pct

    async def calculate_price(self, trading_signal: TradingSignal):
        bid, ask = await self._exchange.get_bid_ask(trading_signal.pair)
        return {
            bs.OrderOperation.BUY: ask,
            bs.OrderOperation.SELL: bid,
        }[trading_signal.operation]

    async def cancel_open_orders(self, pair: bs.Pair, order_operation: bs.OrderOperation):
        await asyncio.gather(*[
            self._exchange.cancel_order(open_order.id)
            for open_order in await self._exchange.get_open_orders(pair)
            if open_order.operation == order_operation
        ])

    async def on_trading_signal(self, trading_signal: TradingSignal):
        logging.info("Trading signal: operation=%s pair=%s", trading_signal.operation, trading_signal.pair)
        try:
            # Cancel any open orders in the opposite direction.
            await self.cancel_open_orders(
                trading_signal.pair,
                bs.OrderOperation.BUY if trading_signal.operation == bs.OrderOperation.SELL else bs.OrderOperation.SELL
            )

            # Calculate the order price and size.
            balance_symbol = trading_signal.pair.quote_symbol if trading_signal.operation == bs.OrderOperation.BUY \
                else trading_signal.pair.base_symbol
            balance, price, pair_info = await asyncio.gather(
                self._exchange.get_balance(balance_symbol),
                self.calculate_price(trading_signal),
                self._exchange.get_pair_info(trading_signal.pair)
            )
            if trading_signal.operation == bs.OrderOperation.BUY:
                order_size = balance.available * self._position_pct / price
            else:
                order_size = balance.available
            order_size = bs.truncate_decimal(order_size, pair_info.base_precision)
            if not order_size:
                return

            logging.info(
                "Creating %s limit order for %s. Price: %s", trading_signal.operation, trading_signal.pair, price
            )
            await self._exchange.create_limit_order(
                trading_signal.operation, trading_signal.pair, order_size, price
            )
        except Exception as e:
            logging.error(e)


async def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s")

    event_dispatcher = bs.backtesting_dispatcher()
    pair = bs.Pair("BTC", "USD")
    exchange = backtesting_exchange.Exchange(event_dispatcher, initial_balances={"USD": Decimal(10000)})
    exchange.set_pair_info(pair, bs.PairInfo(8, 2))

    # Load bars from CSV files.
    exchange.add_bar_source(csv.BarSource(pair, "bitstamp_btcusd_day.csv", csv.BarPeriod.DAY))

    # RSI will be used to generate trading signals.
    signal_source = RSI_SignalSource(25, Decimal(79), Decimal(39))
    exchange.subscribe_to_bar_events(pair, signal_source.on_bar_event)

    strategy = Strategy(exchange, Decimal("0.95"))
    event_dispatcher.subscribe(signal_source, strategy.on_trading_signal)

    # Run the backtes
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
