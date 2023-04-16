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

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, List, Optional
import asyncio
import datetime
import logging

import aiohttp

from . import client, helpers
from basana.core import dt, event, logs, token_bucket, websockets as core_ws
from basana.core.pair import Pair


logger = logging.getLogger(__name__)


@dataclass
class Entry:
    #: The price.
    price: Decimal

    #: The volume.
    volume: Decimal


class OrderBook:
    """An order book."""
    def __init__(self, pair: Pair, json: dict):
        #: The trading pair.
        self.pair: Pair = pair
        #: The JSON representation.
        self.json: dict = json

    @property
    def bids(self) -> List[Entry]:
        """Returns the top bid entries."""
        return [
            Entry(price=Decimal(entry[0]), volume=Decimal(entry[1])) for entry in self.json["bids"]
        ]

    @property
    def asks(self) -> List[Entry]:
        """Returns the top ask entries."""
        return [
            Entry(price=Decimal(entry[0]), volume=Decimal(entry[1])) for entry in self.json["asks"]
        ]


class OrderBookEvent(event.Event):
    """An event for order book updates.

    :param when: The datetime when the event occurred. It must have timezone information set.
    :param order_book: The updated order book.
    """
    def __init__(self, when: datetime.datetime, order_book: OrderBook):
        super().__init__(when)
        #: The order book.
        self.order_book: OrderBook = order_book


class PollOrderBook(event.FifoQueueEventSource, event.Producer):
    def __init__(
            self, pair: Pair, interval: float, limit: Optional[int] = None,
            session: Optional[aiohttp.ClientSession] = None, tb: Optional[token_bucket.TokenBucketLimiter] = None,
            config_overrides: dict = {}
    ):
        assert interval > 0, "Invalid interval"

        super().__init__(producer=self)
        self.pair = pair
        self._interval = interval
        self._limit = limit
        self._client = client.APIClient(session=session, tb=tb, config_overrides=config_overrides)

    async def _fetch_and_push(self, order_book_symbol: str):
        order_book_json = await self._client.get_order_book(order_book_symbol, limit=self._limit)
        self.push(OrderBookEvent(
            dt.utc_now(),
            OrderBook(self.pair, order_book_json)
        ))

    async def on_error(self, error: Any):
        logger.error(logs.StructuredMessage("Error polling order book", channel=self.pair, error=error))

    async def main(self):
        order_book_symbol = helpers.pair_to_order_book_symbol(self.pair)
        while True:
            try:
                await self._fetch_and_push(order_book_symbol)
            except Exception as e:
                await self.on_error(e)
            await asyncio.sleep(self._interval)


# Generate OrderBookEvent events from websocket messages.
class WebSocketEventSource(core_ws.ChannelEventSource):
    def __init__(self, pair: Pair, producer: event.Producer):
        super().__init__(producer=producer)
        self._pair = pair

    async def push_from_message(self, message: dict):
        event = message["data"]
        self.push(OrderBookEvent(
            dt.utc_now(),  # The event doesn't include a timestamp.
            OrderBook(self._pair, event)
        ))


def get_channel(pair: Pair, depth: int) -> str:
    assert depth in [5, 10, 20], "Invalid depth"
    return "{}@depth{}".format(helpers.pair_to_order_book_symbol(pair).lower(), depth)
