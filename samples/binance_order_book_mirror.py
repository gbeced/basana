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

from decimal import Decimal
from typing import List, Optional
import abc
import asyncio
import bisect
import logging

from basana.core.logs import StructuredMessage
from basana.external.binance import exchange as binance_exchange
import basana as bs


class OrderBook:
    def __init__(self):
        self.asks = []  # list of (price, amount), sorted ascending by price
        self.bids = []  # list of (price, amount), sorted descending by price
        self.last_update_id = 0

    @classmethod
    def from_exchange(cls, obook: binance_exchange.PartialOrderBook):
        ret = OrderBook()
        ret.asks = sorted([(ask.price, ask.volume) for ask in obook.asks])
        ret.bids = sorted([(bid.price, bid.volume) for bid in obook.bids], reverse=True)
        ret.last_update_id = obook.last_update_id
        return ret

    def update_from_diff(self, diff: binance_exchange.OrderBookDiff):
        if self.last_update_id - diff.first_update_id < -1:
            raise Exception("Order book snapshot is too old")

        if self.last_update_id < diff.final_update_id:
            for bid in diff.bids:
                self.update(price=bid.price, amount=bid.volume, is_bid=True)
            for ask in diff.asks:
                self.update(price=ask.price, amount=ask.volume, is_bid=False)
            self.last_update_id = diff.final_update_id

    def update(self, price: Decimal, amount: Decimal, is_bid: bool):
        """
        Update the order book with a new price and amount.

        :param price: The price level to update.
        :param amount: The new amount at the price level. If the amount is 0, the price level should be removed.
        :param is_bid: True if the price level is a bid, False if it is an ask.
        """

        if is_bid:
            # Bids are sorted descending order.
            pos = bisect_descending(self.bids, price)
            prices = self.bids
        else:
            # Asks are sorted ascending order.
            pos = bisect.bisect_left(self.asks, (price, 0))
            prices = self.asks

        if pos < len(prices) and prices[pos][0] == price:
            # The price level already exists.
            if amount == 0:
                final_amount = 0
            else:
                final_amount = amount

            if final_amount > 0:
                prices[pos] = (price, final_amount)
            else:
                del prices[pos]
        elif amount:
            # The price level has to be created.
            prices.insert(pos, (price, amount))

    @property
    def ready(self) -> bool:
        return bool(self.bids) and bool(self.asks)


class UpdaterState(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_order_book_diff_event(
        self, updater: "OrderBookUpdater", diff_event: binance_exchange.OrderBookDiffEvent
    ):
        raise NotImplementedError()


class OrderBookUpdater:
    MAX_DEPTH = 5000

    def __init__(self, pair: bs.Pair, exchange: binance_exchange.Exchange, interval: int = 100, check_depth: int = 20):
        self.order_book = OrderBook()
        self._pair = pair
        self._exchange = exchange
        self._state = Initializing()
        self._check_task = None
        self._check_interval = 1
        self._check_depth = check_depth

        exchange.subscribe_to_order_book_diff_events(pair, self._on_order_book_diff_event, interval=interval)

    async def _on_order_book_diff_event(self, diff_event: binance_exchange.OrderBookDiffEvent):
        await self._state.on_order_book_diff_event(self, diff_event)
        if self.order_book.ready:
            logging.info(StructuredMessage(
                self._pair, bid=self.order_book.bids[0][0], ask=self.order_book.asks[0][0],
                last_update_id=self.order_book.last_update_id
            ))

        if self._check_interval and self._check_task is None:
            self._check_task = asyncio.create_task(self._check())

    def switch_state(self, state: UpdaterState):
        self._state = state

    async def _check(self):
        while True:
            await asyncio.sleep(self._check_interval)

            try:
                # If we are lucky and retrieve the same version that we have locally, check the top levels.
                snapshot = await self._exchange.get_order_book(self._pair, self._check_depth)
                if snapshot.last_update_id == self.order_book.last_update_id:
                    logging.info(StructuredMessage(
                        "Checking order book", last_update_id=snapshot.last_update_id
                    ))

                    snapshot_bids = [(bid.price, bid.volume) for bid in snapshot.bids]
                    snapshot_asks = [(ask.price, ask.volume) for ask in snapshot.asks]
                    local_bids = self.order_book.bids[:self._check_depth]
                    local_asks = self.order_book.asks[:self._check_depth]
                    if local_bids != snapshot_bids or local_asks != snapshot_asks:
                        logging.error(StructuredMessage(
                            "Order book mismatch", last_update_id=snapshot.last_update_id,
                            local_bids=local_bids, snapshot_bids=snapshot_bids,
                            local_asks=local_asks, snapshot_asks=snapshot_asks
                        ))
                        self.switch_state(Initializing())
            except Exception as e:
                logging.exception(StructuredMessage("Error checking order book", error=str(e)))


class Initializing(UpdaterState):
    def __init__(self):
        self._buffer: List[binance_exchange.OrderBookDiff] = []
        self._fetch_task: Optional[asyncio.Task] = None

    async def on_order_book_diff_event(
            self, updater: OrderBookUpdater, diff_event: binance_exchange.OrderBookDiffEvent
    ):
        # Buffer diffs to be processed once the snapshot is fetched.
        self._buffer.append(diff_event.order_book_diff)
        # Fetch the snapshot if not doing so already.
        if self._fetch_task is None:
            self._fetch_task = asyncio.create_task(self._fetch_snapshot(updater))

    async def _fetch_snapshot(self, updater: OrderBookUpdater):
        try:
            snapshot = await updater._exchange.get_order_book(updater._pair, OrderBookUpdater.MAX_DEPTH)

            # If the lastUpdateId from the snapshot is strictly less than the first_update_id from the first diff in
            # the queue, then re-fetch the snapshot.
            if snapshot.last_update_id < self._buffer[0].first_update_id:
                raise Exception("Order book snapshot is too old")

            # Discard any diff where final_update_id is <= last_update_id of the snapshot.
            for i in range(0, len(self._buffer)):
                if self._buffer[i].final_update_id > snapshot.last_update_id:
                    break
            else:
                i += 1  # No diffs to apply.

            # Build an order book from the snapshot and update it with pending diffs.
            order_book = OrderBook.from_exchange(snapshot)
            for i in range(i, len(self._buffer)):
                order_book.update_from_diff(self._buffer[i])

            # Set the new order book and switch to updating state.
            updater.order_book = order_book
            updater.switch_state(Updating())

        except Exception as e:
            logging.exception(StructuredMessage("Error fetching and processing order book", error=str(e)))
            if len(self._buffer):
                self._fetch_task = asyncio.create_task(self._fetch_snapshot(updater))
            else:
                # Wait until there is a diff available.
                self._fetch_task = None


class Updating(UpdaterState):
    async def on_order_book_diff_event(
            self, updater: OrderBookUpdater, diff_event: binance_exchange.OrderBookDiffEvent
    ):
        diff = diff_event.order_book_diff
        try:
            updater.order_book.update_from_diff(diff)
        except Exception as e:
            logging.exception(StructuredMessage("Error processing diff", error=str(e)))
            updater.switch_state(Initializing())


def bisect_descending(a, x):
    lo = 0
    hi = len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        if a[mid][0] > x:
            lo = mid + 1
        else:
            hi = mid
    return lo


async def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s")
    event_dispatcher = bs.realtime_dispatcher()
    exchange = binance_exchange.Exchange(event_dispatcher)
    pair = bs.Pair("BTC", "USDT")
    _ = OrderBookUpdater(pair, exchange)

    await event_dispatcher.run()


if __name__ == "__main__":
    asyncio.run(main())
