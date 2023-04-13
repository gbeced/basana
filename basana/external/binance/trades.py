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

from decimal import Decimal
import datetime
import logging

from . import helpers
from basana.core import event, websockets as core_ws
from basana.core.pair import Pair


logger = logging.getLogger(__name__)


class Trade:
    def __init__(self, pair: Pair, json: dict):
        assert json["e"] == "trade"

        #: The trading pair.
        self.pair: Pair = pair
        #: The JSON representation.
        self.json: dict = json

    @property
    def id(self) -> str:
        """The trade id."""
        return str(self.json["t"])

    @property
    def datetime(self) -> datetime.datetime:
        """The trade datetime."""
        return helpers.timestamp_to_datetime(int(self.json["T"]))

    @property
    def price(self) -> Decimal:
        """The price."""
        return Decimal(self.json["p"])

    @property
    def amount(self) -> Decimal:
        """The amount."""
        return Decimal(self.json["q"])

    @property
    def buy_order_id(self) -> str:
        """The buyer order id."""
        return str(self.json["b"])

    @property
    def sell_order_id(self) -> str:
        """The seller order id."""
        return str(self.json["a"])


class TradeEvent(event.Event):
    """An event for new trades.

    :param when: The datetime when the event occurred. It must have timezone information set.
    :param trade: The trade.
    """

    def __init__(self, when: datetime.datetime, trade: Trade):
        super().__init__(when)
        #: The trade.
        self.trade: Trade = trade


# Generate TradeEvent events from websocket messages.
class WebSocketEventSource(core_ws.ChannelEventSource):
    def __init__(self, pair: Pair, producer: event.Producer):
        super().__init__(producer=producer)
        self._pair: Pair = pair

    async def push_from_message(self, message: dict):
        event = message["data"]
        self.push(TradeEvent(
            helpers.timestamp_to_datetime(int(event["E"])),
            Trade(self._pair, event)
        ))


def get_channel(pair: Pair) -> str:
    return "{}@trade".format(helpers.pair_to_order_book_symbol(pair).lower())
