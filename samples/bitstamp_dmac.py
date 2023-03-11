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

# Strategy based on Dual Moving Average Crossover signals.

from decimal import Decimal
import asyncio
import logging


from basana.external.bitstamp import exchange as bitstamp_exchange
import basana as bs
import dmac


# The strategy is responsible for placing orders in response to trading signals.
class Strategy:
    def __init__(self, exchange: bitstamp_exchange.Exchange, position_amount: Decimal):
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
                bs.OrderOperation.BUY if trading_signal.operation == bs.OrderOperation.SELL
                else bs.OrderOperation.SELL
            )

            # Calculate the order price and size.
            balances, price, pair_info = await asyncio.gather(
                self._exchange.get_balances(),
                self.calculate_price(trading_signal),
                self._exchange.get_pair_info(trading_signal.pair)
            )
            if trading_signal.operation == bs.OrderOperation.BUY:
                balance = balances[trading_signal.pair.quote_symbol]
                order_size = min(balance.available, self._position_amount) / price
            else:
                balance = balances[trading_signal.pair.base_symbol]
                order_size = balance.available
            order_size = bs.truncate_decimal(order_size, pair_info.base_precision)
            if not order_size:
                return

            logging.info(
                "Creating %s limit order for %s: amount=%s price=%s",
                trading_signal.operation, trading_signal.pair, order_size, price
            )
            await self._exchange.create_limit_order(
                trading_signal.operation, trading_signal.pair, order_size, price
            )
        except Exception as e:
            logging.error(e)


async def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s")

    event_dispatcher = bs.realtime_dispatcher()
    api_key = "YOUR_API_KEY"
    api_secret = "YOUR_API_SECRET"
    exchange = bitstamp_exchange.Exchange(event_dispatcher, api_key=api_key, api_secret=api_secret)
    strategy = Strategy(exchange, Decimal(30))

    pairs = [
        bs.Pair("BTC", "USD"),
        bs.Pair("ETH", "USD"),
    ]
    for pair in pairs:
        # Connect the signal source with the bar events from the exchange.
        signal_source = dmac.SignalSource(event_dispatcher, 5, 9)
        exchange.subscribe_to_bar_events(pair, 60, signal_source.on_bar_event, flush_delay=1)

        # Connect the strategy with the signal source.
        signal_source.subscribe_to_trading_signals(strategy.on_trading_signal)

    await event_dispatcher.run()


if __name__ == "__main__":
    asyncio.run(main())
