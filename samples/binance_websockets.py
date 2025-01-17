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

from basana.external.binance import exchange as binance_exchange
import basana as bs


async def on_bar_event(bar_event: bs.BarEvent):
    logging.info(
        "Bar event: pair=%s open=%s high=%s low=%s close=%s volume=%s",
        bar_event.bar.pair, bar_event.bar.open, bar_event.bar.high, bar_event.bar.low, bar_event.bar.close,
        bar_event.bar.volume
    )


async def on_order_book_event(order_book_event: binance_exchange.OrderBookEvent):
    logging.info(
        "Order book event: pair=%s bid=%s ask=%s",
        order_book_event.order_book.pair, order_book_event.order_book.bids[0].price,
        order_book_event.order_book.asks[0].price
    )


async def on_trade_event(trade_event: binance_exchange.TradeEvent):
    logging.info(
        "Trade event: pair=%s price=%s amount=%s",
        trade_event.trade.pair, trade_event.trade.price, trade_event.trade.amount
    )


async def on_order_event(event):
    logging.info(
        "Order event: id=%s status=%s amount_filled=%s fees=%s",
        event.order_update.id, event.order_update.status, event.order_update.amount_filled, event.order_update.fees
    )


async def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s")
    event_dispatcher = bs.realtime_dispatcher()
    exchange = binance_exchange.Exchange(
        event_dispatcher,
        # api_key="YOUR_API_KEY",
        # api_secret="YOUR_API_SECRET"
    )

    pairs = [
        bs.Pair("BTC", "USDT"),
        bs.Pair("ETH", "USDT"),
    ]
    for pair in pairs:
        exchange.subscribe_to_bar_events(pair, "1m", on_bar_event)
        exchange.subscribe_to_order_book_events(pair, on_order_book_event)
        exchange.subscribe_to_trade_events(pair, on_trade_event)

    # Uncomment the following lines if you want to subscribe to order events. This requires the API key and secret to
    # be set.
    # exchange.spot_account.subscribe_to_order_events(on_order_event)
    # exchange.cross_margin_account.subscribe_to_order_events(on_order_event)
    # for pair in pairs:
    #     exchange.isolated_margin_account.subscribe_to_order_events(pair, on_order_event)

    await event_dispatcher.run()


if __name__ == "__main__":
    asyncio.run(main())
