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

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

from decimal import Decimal
import datetime

from . import helpers
from basana.core import dt, event, websockets as core_ws
from basana.core.enums import OrderOperation
from basana.core.pair import Pair


class Trade:
    def __init__(self, pair: Pair, json: dict):
        self.pair = pair
        self.json = json

    @property
    def id(self) -> str:
        return str(self.json["id"])

    @property
    def datetime(self) -> datetime.datetime:
        timestamp = int(self.json["microtimestamp"]) / 1e6
        return datetime.datetime.utcfromtimestamp(timestamp).replace(tzinfo=datetime.timezone.utc)

    @property
    def amount(self) -> Decimal:
        return Decimal(self.json["amount_str"])

    @property
    def price(self) -> Decimal:
        return Decimal(self.json["price_str"])

    @property
    def type(self) -> OrderOperation:
        return helpers.order_type_to_order_operation(int(self.json["type"]))

    @property
    def buy_order_id(self) -> str:
        return str(self.json["buy_order_id"])

    @property
    def sell_order_id(self) -> str:
        return str(self.json["sell_order_id"])


class TradeEvent(event.Event):
    def __init__(self, trade: Trade):
        super().__init__(dt.utc_now())
        self.trade = trade


# Generate Trade events from websocket messages.
class WebSocketEventSource(core_ws.ChannelEventSource):
    def __init__(self, pair: Pair, producer: event.Producer):
        super().__init__(producer=producer)
        self._pair = pair

    async def push_from_message(self, message: dict):
        self.push(TradeEvent(Trade(self._pair, message["data"])))


def get_public_channel(pair: Pair) -> str:
    return "live_trades_{}".format(helpers.pair_to_currency_pair(pair))


def get_private_channel(pair: Pair) -> str:
    return "private-my_trades_{}".format(helpers.pair_to_currency_pair(pair))
