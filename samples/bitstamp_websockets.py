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
import logging

from basana.external.bitstamp import exchange as bitstamp_exchange
import basana as bs


async def on_bar_event(bar_event: bs.BarEvent):
    logging.info(
        "Bar event: pair=%s open=%s high=%s low=%s close=%s volume=%s",
        bar_event.bar.pair, bar_event.bar.open, bar_event.bar.high, bar_event.bar.low, bar_event.bar.close,
        bar_event.bar.volume
    )


async def on_order_book_event(order_book_event: bitstamp_exchange.OrderBookEvent):
    logging.info(
        "Order book event: pair=%s bid=%s ask=%s",
        order_book_event.order_book.pair, order_book_event.order_book.bids[0].price,
        order_book_event.order_book.asks[0].price
    )


async def on_trade_event(trade_event: bitstamp_exchange.TradeEvent):
    logging.info(
        "Trade event: pair=%s price=%s amount=%s",
        trade_event.trade.pair, trade_event.trade.price, trade_event.trade.amount
    )


async def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s")
    event_dispatcher = bs.realtime_dispatcher()
    exchange = bitstamp_exchange.Exchange(event_dispatcher)

    pairs = [
        bs.Pair("BTC", "USD"),
        bs.Pair("ETH", "USD"),
    ]
    for pair in pairs:
        exchange.subscribe_to_bar_events(pair, 60, on_bar_event)
        exchange.subscribe_to_order_book_events(pair, on_order_book_event)
        exchange.subscribe_to_public_trade_events(pair, on_trade_event)

    await event_dispatcher.run()


if __name__ == "__main__":
    asyncio.run(main())
