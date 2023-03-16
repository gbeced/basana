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
# python -m basana.external.bitstamp.tools.download_bars -c BTC/USD -p 1m -s 2021-01-01 -e 2021-01-31 > \
# bitstamp_btcusd_min.csv

from decimal import Decimal
import asyncio
import logging

from basana.external.bitstamp import csv
import basana as bs
import basana.backtesting.exchange as backtesting_exchange
import rsi


class PositionManager:
    def __init__(self, exchange: backtesting_exchange.Exchange, position_amount: Decimal):
        assert position_amount > 0
        self._exchange = exchange
        self._position_amount = position_amount

    async def calculate_price(self, trading_signal: bs.TradingSignal):
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

    async def on_trading_signal(self, trading_signal: bs.TradingSignal):
        logging.info("Trading signal: operation=%s pair=%s", trading_signal.operation, trading_signal.pair)
        try:
            # Cancel any open orders in the opposite direction.
            await self.cancel_open_orders(
                trading_signal.pair,
                bs.OrderOperation.BUY if trading_signal.operation == bs.OrderOperation.SELL else bs.OrderOperation.SELL
            )

            # Calculate the order price and size.
            balances, price, pair_info = await asyncio.gather(
                self._exchange.get_balances(),
                self.calculate_price(trading_signal),
                self._exchange.get_pair_info(trading_signal.pair)
            )
            if trading_signal.operation == bs.OrderOperation.BUY:
                balance = balances[trading_signal.pair.quote_symbol]
                order_size = min(self._position_amount, balance.available) / price
            else:
                balance = balances[trading_signal.pair.base_symbol]
                order_size = balance.available
            order_size = bs.truncate_decimal(order_size, pair_info.base_precision)
            if not order_size:
                return

            logging.info(
                "Creating %s limit order for %s: amount: %s price: %s", trading_signal.operation, trading_signal.pair,
                order_size, price
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
    exchange = backtesting_exchange.Exchange(
        event_dispatcher,
        initial_balances={"BTC": Decimal(0), "USD": Decimal(10000)}
    )
    exchange.set_pair_info(pair, bs.PairInfo(8, 2))

    # Connect the strategy to the bar events from the exchange.
    strategy = rsi.Strategy(event_dispatcher, 20, 30, 70)
    exchange.subscribe_to_bar_events(pair, strategy.on_bar_event)

    # Connect the position manager to the strategy signals.
    position_mgr = PositionManager(exchange, Decimal(1000))
    strategy.subscribe_to_trading_signals(position_mgr.on_trading_signal)

    # Load bars from CSV files.
    exchange.add_bar_source(csv.BarSource(pair, "bitstamp_btcusd_min.csv", "1d"))

    # Run the backtest.
    await event_dispatcher.run()

    # Log balances.
    balances = await exchange.get_balances()
    for currency, balance in balances.items():
        logging.info("%s balance: %s", currency, balance.available)


if __name__ == "__main__":
    asyncio.run(main())
