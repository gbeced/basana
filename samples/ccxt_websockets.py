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

from basana.external.ccxt.exchange import Exchange, TradeEvent, PartialOrderBookEvent
import basana as bs


async def on_bar_event(bar_event: bs.BarEvent):
    bar = bar_event.bar
    logging.info(
        (
            f"Bar event: pair={bar.pair} begin={bar.begin} open={bar.open} high={bar.high} "
            f"low={bar.low} close={bar.close} volume={bar.volume}"
        )
    )


async def on_order_book_event(order_book_event: PartialOrderBookEvent):
    order_book = order_book_event.order_book
    logging.info(
        "Order book event: pair=%s bid=%s ask=%s",
        order_book.pair, order_book.bids[0].price, order_book.asks[0].price
    )


async def on_trade_event(trade_event: TradeEvent):
    trade = trade_event.trade
    logging.info(
        "Trade event: id=%s pair=%s price=%s amount=%s",
        trade.id, trade.pair, trade.price, trade.amount
    )


async def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s")
    event_dispatcher = bs.realtime_dispatcher()

    try:
        exchange_id = "binance"
        pair = {
            "binance": bs.Pair("BTC", "USDT"),
            "kraken": bs.Pair("ETH", "USD"),
            "bitfinex": bs.Pair("XRP", "USD"),
        }[exchange_id]
        exchange = Exchange(event_dispatcher, exchange_id)
        exchange.subscribe_to_bar_events(pair, "1m", on_bar_event)
        exchange.subscribe_to_order_book_events(pair, on_order_book_event)
        exchange.subscribe_to_public_trade_events(pair, on_trade_event)

        await event_dispatcher.run()
    finally:
        await exchange.close()


if __name__ == "__main__":
    asyncio.run(main())
