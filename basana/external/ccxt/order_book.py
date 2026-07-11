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

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, List, Optional

from . import helpers
from .watch_event_source import WatchEventSource
from basana.core import dt, event
from basana.core.pair import Pair


@dataclass
class Entry:
    #: The price.
    price: Decimal

    #: The volume.
    volume: Decimal


class PartialOrderBook:
    """An order book."""
    def __init__(self, pair: Pair, raw: dict):
        #: The trading pair.
        self.pair: Pair = pair
        #: The raw value returned by CCXT.
        self.raw: dict = raw

    @property
    def bids(self) -> List[Entry]:
        """Returns the top bid entries."""
        return [
            Entry(price=helpers.to_decimal(entry[0]), volume=helpers.to_decimal(entry[1]))
            for entry in self.raw["bids"]
        ]

    @property
    def asks(self) -> List[Entry]:
        """Returns the top ask entries."""
        return [
            Entry(price=helpers.to_decimal(entry[0]), volume=helpers.to_decimal(entry[1]))
            for entry in self.raw["asks"]
        ]


class PartialOrderBookEvent(event.Event):
    """
    An event for partial order book updates.

    :param when: The datetime when the event occurred. It must have timezone information set.
    :param order_book: The updated order book.
    """
    def __init__(self, when, order_book: PartialOrderBook):
        super().__init__(when)
        #: The order book.
        self.order_book: PartialOrderBook = order_book


class WatchOrderBookEventSource(WatchEventSource):
    def __init__(
            self, cli: Any, pair: Pair, limit: Optional[int] = None,
            params: Optional[Dict[str, Any]] = None
    ):
        super().__init__(cli, pair, "watchOrderBook", "unWatchOrderBook")
        self._limit = limit
        self._params = dict(params or {})

    async def watch(self):
        order_book = await self._cli.watch_order_book(
            self._symbol, self._limit, params=self._params
        )
        self._handle_order_book(order_book)

    async def unwatch(self):
        await self._cli.un_watch_order_book(self._symbol, params=self._params)

    def _handle_order_book(self, order_book: dict):
        timestamp = order_book.get("timestamp")
        if timestamp is not None:
            when = helpers.timestamp_to_datetime(int(timestamp))
        else:
            when = dt.utc_now(monotonic=True)
        self.push(PartialOrderBookEvent(
            when, PartialOrderBook(self._pair, order_book)
        ))


PartialOrderBookEventHandler = Callable[[PartialOrderBookEvent], Awaitable[Any]]