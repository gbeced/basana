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
from typing import Any, Awaitable, Callable, List
import datetime
import logging

from . import helpers
from basana.core import dt, event, websockets as core_ws
from basana.core.pair import Pair


logger = logging.getLogger(__name__)


@dataclass
class Entry:
    #: The price.
    price: Decimal

    #: The volume.
    volume: Decimal


class OrderBookDiff:
    """
    An order book diff as described in
    https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#diff-depth-stream
    """
    def __init__(self, pair: Pair, json: dict):
        assert json["e"] == "depthUpdate", "Invalid event type: {}".format(json["e"])

        #: The trading pair.
        self.pair: Pair = pair
        #: The JSON representation.
        self.json: dict = json

    @property
    def datetime(self) -> datetime.datetime:
        """The update datetime."""
        return helpers.timestamp_to_datetime(int(self.json["E"]))

    @property
    def first_update_id(self) -> int:
        """The first update id."""
        return self.json["U"]

    @property
    def final_update_id(self) -> int:
        """The final update id."""
        return self.json["u"]

    @property
    def bids(self) -> List[Entry]:
        """Bids to be updated."""
        return [
            Entry(price=Decimal(entry[0]), volume=Decimal(entry[1])) for entry in self.json["b"]
        ]

    @property
    def asks(self) -> List[Entry]:
        """Asks to be updated."""
        return [
            Entry(price=Decimal(entry[0]), volume=Decimal(entry[1])) for entry in self.json["a"]
        ]


class OrderBookDiffEvent(event.Event):
    """
    An event for order book diffs.

    :param when: The datetime when the event occurred. It must have timezone information set.
    :param order_book_diff: The order book diff.
    """
    def __init__(self, when: datetime.datetime, order_book_diff: OrderBookDiff):
        super().__init__(when)
        #: The order book diff.
        self.order_book_diff: OrderBookDiff = order_book_diff


# Generate OrderBookDiffEvent from websocket messages.
class WebSocketEventSource(core_ws.ChannelEventSource):
    def __init__(self, pair: Pair, producer: event.Producer):
        super().__init__(producer=producer)
        self._pair = pair

    async def push_from_message(self, message: dict):
        event = message["data"]
        self.push(OrderBookDiffEvent(
            dt.utc_now(),
            OrderBookDiff(self._pair, event)
        ))


def get_channel(pair: Pair, interval: int) -> str:
    assert interval in [100, 1000], "Invalid interval"
    return "{}@depth@{}ms".format(helpers.pair_to_symbol(pair).lower(), interval)


OrderBookDiffEventHandler = Callable[[OrderBookDiffEvent], Awaitable[Any]]
