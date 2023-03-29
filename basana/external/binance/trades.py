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
from basana.core import dt, event, websockets as core_ws
from basana.core.pair import Pair


logger = logging.getLogger(__name__)


class Trade:
    def __init__(self, pair: Pair, json: dict):
        assert json["e"] == "trade"

        self.pair = pair
        self.json = json

    @property
    def id(self) -> str:
        return str(self.json["t"])

    @property
    def datetime(self) -> datetime.datetime:
        timestamp = self.json["T"] / 1e3
        return datetime.datetime.utcfromtimestamp(timestamp).replace(tzinfo=datetime.timezone.utc)

    @property
    def price(self) -> Decimal:
        return Decimal(self.json["p"])

    @property
    def amount(self) -> Decimal:
        return Decimal(self.json["q"])

    @property
    def buy_order_id(self) -> str:
        return str(self.json["b"])

    @property
    def sell_order_id(self) -> str:
        return str(self.json["a"])


class TradeEvent(event.Event):
    def __init__(self, trade: Trade):
        super().__init__(dt.utc_now())
        self.trade = trade


# Generate TradeEvent events from websocket messages.
class WebSocketEventSource(core_ws.ChannelEventSource):
    def __init__(self, pair: Pair, producer: event.Producer):
        super().__init__(producer=producer)
        self._pair = pair

    async def push_from_message(self, message: dict):
        self.push(TradeEvent(Trade(self._pair, message["data"])))


def get_channel(pair: Pair) -> str:
    return "{}@trade".format(helpers.pair_to_order_book_symbol(pair).lower())
