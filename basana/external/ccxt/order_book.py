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
import asyncio
import logging

from . import helpers
from basana.core import dt, event, logs
from basana.core.pair import Pair


logger = logging.getLogger(__name__)


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


class WatchOrderBookEventSource(event.FifoQueueEventSource, event.Producer):
    def __init__(
            self, cli: Any, pair: Pair, limit: Optional[int] = None,
            params: Optional[Dict[str, Any]] = None
    ):
        if not cli.has.get("watchOrderBook"):
            raise NotImplementedError("The exchange does not support watchOrderBook")

        super().__init__(producer=self)
        self._cli = cli
        self._pair = pair
        self._symbol = helpers.pair_to_symbol(pair)
        self._limit = limit
        self._params = dict(params or {})

    async def initialize(self):
        await self._cli.load_markets()

    async def main(self):
        while True:
            try:
                order_book = await self._cli.watch_order_book(
                    self._symbol, self._limit, params=self._params
                )
                self._handle_order_book(order_book)
            except Exception as e:
                logger.error(logs.StructuredMessage(
                    "Error watching order book", pair=self._pair, error=e
                ))
            await asyncio.sleep(0)

    async def finalize(self):
        if not self._cli.has.get("unWatchOrderBook"):
            return

        try:
            await self._cli.un_watch_order_book(self._symbol, params=self._params)
        except Exception as error:
            logger.error(logs.StructuredMessage(
                "Error unwatching order book", pair=self._pair, error=error
            ))

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