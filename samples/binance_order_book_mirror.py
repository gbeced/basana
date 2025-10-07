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
from typing import List, Optional, Tuple
import abc
import asyncio
import bisect
import datetime
import logging

from basana.core.logs import StructuredMessage
from basana.external.binance import exchange as binance_exchange
import basana as bs


class OrderBook:
    """
    A class representing an exchange order book that maintains sorted lists of bids and asks.
    The order book can be initialized from exchange data and updated via diffs from a WebSocket stream
    while maintaining proper synchronization. It ensures price levels are properly maintained by
    inserting, updating or removing entries based on incoming data.
    """
    def __init__(
            self, bids: List[Tuple[Decimal, Decimal]] = [], asks: List[Tuple[Decimal, Decimal]] = []
    ):
        self.bids = sorted(bids, reverse=True)
        self.asks = sorted(asks)
        self.last_update_id = 0
        self.last_updated: Optional[datetime.datetime] = None
        self._last_bid: Optional[Decimal] = self.bids[-1][0] if self.bids else None
        self._last_ask: Optional[Decimal] = self.asks[-1][0] if self.asks else None

    @classmethod
    def from_exchange(cls, obook: binance_exchange.PartialOrderBook) -> "OrderBook":
        """
        Creates an OrderBook instance from a Binance exchange partial order book.

        :param obook: Partial order book data from Binance exchange.
        """
        ret = OrderBook(
            bids=[(bid.price, bid.volume) for bid in obook.bids],
            asks=[(ask.price, ask.volume) for ask in obook.asks]
        )
        ret.last_update_id = obook.last_update_id
        return ret

    def update_from_diff(self, diff_event: binance_exchange.OrderBookDiffEvent):
        """
        Updates the order book based on a diff received from Binance WebSocket stream.
        This method ensures the order book stays synchronized by only applying diffs that are
        sequential and within the current price bounds to avoid gaps in the order book.

        :param diff: The OrderBookDiff containing bids and asks updates from Binance.
        :raises Exception: If the order book snapshot is too old compared to the diff
        """

        diff = diff_event.order_book_diff
        update_id_diff = diff.first_update_id - self.last_update_id
        if update_id_diff > 1:
            raise Exception("Order book snapshot is too old")
        elif update_id_diff >= 0:
            # Update only within our current bounds, otherwise there might be gaps.
            for bid in filter(lambda bid: self._last_bid is not None and bid.price >= self._last_bid, diff.bids):
                self.update(price=bid.price, amount=bid.volume, is_bid=True)
            for ask in filter(lambda ask: self._last_ask is not None and ask.price <= self._last_ask, diff.asks):
                self.update(price=ask.price, amount=ask.volume, is_bid=False)
            self.last_update_id = diff.final_update_id
            self.last_updated = diff_event.when

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


class UpdaterState(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_order_book_diff_event(
        self, updater: "OrderBookUpdater", diff_event: binance_exchange.OrderBookDiffEvent
    ):
        raise NotImplementedError()

    async def on_enter_state(self, updater: "OrderBookUpdater"):
        pass

    async def on_exit_state(self, updater: "OrderBookUpdater"):
        pass

    async def on_sync_required(self, updater: "OrderBookUpdater"):
        pass


class OrderBookUpdater:
    """
    An order book updater that maintains a local copy of Binance order book by processing
    diffs received from a WebSocket stream and periodically checking consistency against
    snapshots fetched via REST API. It handles initialization, updating, and state transitions to ensure
    the local order book remains accurate and synchronized with the exchange.

    :param pair: The trading pair for the order book.
    :param exchange: The Binance exchange instance to interact with.
    :param max_depth: The maximum depth of the order book to maintain (5000 is the maximum depth supported).
    :param diff_interval_ms: The interval in milliseconds for receiving order book diffs (100 or 1000 ms).
    :param check_interval_ms: The interval in milliseconds for checking order book consistency via snapshots.
    :param check_depth: The depth of the order book to check for consistency.
    :param re_sync_threshold: The threshold (as a fraction of max_depth) below which a re-sync is triggered.
    """
    def __init__(
            self, pair: bs.Pair, exchange: binance_exchange.Exchange, max_depth: int = 5000,
            diff_interval_ms: int = 100, check_interval_ms: int = 1000, check_depth: int = 20,
            re_sync_threshold: float = 0.2
    ):
        assert diff_interval_ms in (100, 1000)
        assert max_depth <= 5000
        assert re_sync_threshold > 0 and re_sync_threshold < 1

        self.order_book = OrderBook()
        self._pair = pair
        self._exchange = exchange
        self._max_depth = max_depth
        self._state = Initializing()
        self._check_interval_ms = check_interval_ms
        self._check_depth = check_depth
        self._switch_mutex = asyncio.Lock()
        self._re_sync_threshold = round(max_depth * re_sync_threshold)

        exchange.subscribe_to_order_book_diff_events(pair, self._on_order_book_diff_event, interval=diff_interval_ms)

    async def _on_order_book_diff_event(self, diff_event: binance_exchange.OrderBookDiffEvent):
        logging.info(StructuredMessage(
            "Order book diff", first_update_id=diff_event.order_book_diff.first_update_id,
            final_update_id=diff_event.order_book_diff.final_update_id
        ))

        await self._state.on_order_book_diff_event(self, diff_event)

        msg_kwargs = dict(
            bids=len(self.order_book.bids),
            asks=len(self.order_book.asks),
            last_update_id=self.order_book.last_update_id
        )
        if self.order_book.bids:
            msg_kwargs["bid"] = self.order_book.bids[0][0]
        if self.order_book.asks:
            msg_kwargs["ask"] = self.order_book.asks[0][0]
        logging.info(StructuredMessage(self._pair, **msg_kwargs))

        if len(self.order_book.bids) <= self._re_sync_threshold or len(self.order_book.asks) <= self._re_sync_threshold:
            await self._state.on_sync_required(self)

    async def switch_state(self, state: UpdaterState, order_book: Optional[OrderBook] = None):
        async with self._switch_mutex:
            logging.info(StructuredMessage(
                "Switch state", current=self._state.__class__.__name__, new=state.__class__.__name__
            ))

            await self._state.on_exit_state(self)

            if order_book:
                assert order_book.last_update_id >= self.order_book.last_update_id
                self.order_book = order_book
                logging.info(StructuredMessage(
                    "New orderbook set", last_update_id=self.order_book.last_update_id,
                    top_bid=self.order_book.bids[0][0], last_bid=self.order_book.bids[-1][0],
                    top_ask=self.order_book.asks[0][0], last_ask=self.order_book.asks[-1][0],
                ))
            self._state = state

            await self._state.on_enter_state(self)


class Initializing(UpdaterState):
    def __init__(self):
        self._buffer: List[binance_exchange.OrderBookDiffEvent] = []
        self._fetch_task: Optional[asyncio.Task] = None

    async def on_order_book_diff_event(
            self, updater: OrderBookUpdater, diff_event: binance_exchange.OrderBookDiffEvent
    ):
        # Buffer diffs to be processed once the snapshot is fetched.
        self._buffer.append(diff_event)
        # Fetch the snapshot if not doing so already.
        if self._fetch_task is None:
            self._fetch_task = asyncio.create_task(self._fetch_snapshot(updater))

    async def _fetch_snapshot(self, updater: OrderBookUpdater):
        try:
            snapshot = await updater._exchange.get_order_book(updater._pair, updater._max_depth)

            # If the lastUpdateId from the snapshot is strictly less than the first_update_id from the first diff in
            # the queue, then re-fetch the snapshot.
            if snapshot.last_update_id < self._buffer[0].order_book_diff.first_update_id:
                raise Exception("Order book snapshot is too old")

            # Discard any diff where final_update_id is <= last_update_id of the snapshot.
            for i in range(0, len(self._buffer)):
                if self._buffer[i].order_book_diff.final_update_id > snapshot.last_update_id:
                    break
            else:
                i += 1  # No diffs to apply.

            # Build an order book from the snapshot and update it with pending diffs.
            order_book = OrderBook.from_exchange(snapshot)
            for i in range(i, len(self._buffer)):
                order_book.update_from_diff(self._buffer[i])

            # Switch to updating state using the new orderbook.
            await updater.switch_state(Updating(), order_book=order_book)

        except Exception as e:
            logging.exception(StructuredMessage("Error fetching and processing order book", error=str(e)))
            if len(self._buffer):
                self._fetch_task = asyncio.create_task(self._fetch_snapshot(updater))
            else:
                # Wait until there is a diff available.
                self._fetch_task = None


class Updating(UpdaterState):
    def __init__(self):
        self._snapshot: Optional[binance_exchange.PartialOrderBook] = None
        self._fetch_task: Optional[asyncio.Task] = None

    async def on_enter_state(self, updater: "OrderBookUpdater"):
        if updater._check_interval_ms:
            self._fetch_task = asyncio.create_task(self._fetch_snapshot(updater))

    async def on_order_book_diff_event(
            self, updater: OrderBookUpdater, diff_event: binance_exchange.OrderBookDiffEvent
    ):
        try:
            updater.order_book.update_from_diff(diff_event)
            await self._check_order_book_consistency(updater)
        except Exception as e:
            logging.exception(StructuredMessage("Error processing diff", error=str(e)))
            await updater.switch_state(Initializing())

    async def on_exit_state(self, updater: "OrderBookUpdater"):
        if self._fetch_task:
            self._fetch_task.cancel()

    async def on_sync_required(self, updater: "OrderBookUpdater"):
        logging.warning("Sync required")
        await updater.switch_state(Initializing())

    async def _fetch_snapshot(self, updater: OrderBookUpdater):
        while True:
            try:
                await asyncio.sleep(updater._check_interval_ms / 1000)
                snapshot = await updater._exchange.get_order_book(updater._pair, limit=updater._check_depth)
                self._snapshot = snapshot
                logging.info(StructuredMessage("Fetched order book", last_update_id=self._snapshot.last_update_id))
                await self._check_order_book_consistency(updater)
            except Exception as e:
                logging.exception(StructuredMessage("Error fetching order book", error=str(e)))

    async def _check_order_book_consistency(self, updater: OrderBookUpdater):
        if self._snapshot is None:
            return

        if self._snapshot.last_update_id == updater.order_book.last_update_id:
            logging.info(StructuredMessage("Checking order book", last_update_id=self._snapshot.last_update_id))

            snapshot_bids = [(bid.price, bid.volume) for bid in self._snapshot.bids]
            snapshot_asks = [(ask.price, ask.volume) for ask in self._snapshot.asks]
            local_bids = updater.order_book.bids[:updater._check_depth]
            local_asks = updater.order_book.asks[:updater._check_depth]
            if local_bids != snapshot_bids or local_asks != snapshot_asks:
                logging.error(StructuredMessage(
                    "Order book mismatch", last_update_id=self._snapshot.last_update_id,
                    local_bids=local_bids, snapshot_bids=snapshot_bids,
                    local_asks=local_asks, snapshot_asks=snapshot_asks
                ))
                await updater.switch_state(Initializing())


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
