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

from . import helpers
from basana.core import dt, event, websockets as core_ws
from basana.core.enums import OrderOperation
from basana.core.pair import Pair


class Order:
    def __init__(self, pair: Pair, json: dict):
        #: The trading pair.
        self.pair: Pair = pair
        #: The JSON representation.
        self.json: dict = json

    @property
    def id(self) -> str:
        """The order id."""
        return str(self.json["id"])

    @property
    def datetime(self) -> datetime.datetime:
        timestamp = int(self.json["microtimestamp"]) / 1e6
        return datetime.datetime.utcfromtimestamp(timestamp).replace(tzinfo=datetime.timezone.utc)

    @property
    def amount(self) -> Decimal:
        """The order amount."""
        return Decimal(self.json["amount_str"])

    @property
    def price(self) -> Decimal:
        """The order price."""
        return Decimal(self.json["price_str"])

    @property
    def type(self) -> OrderOperation:
        # TODO: Deprecate this property.
        return self.operation

    @property
    def operation(self) -> OrderOperation:
        """The operation."""
        return helpers.order_type_to_order_operation(int(self.json["order_type"]))


class OrderEvent(event.Event):
    """An event for order updates.

    :param when: The datetime when the event occurred. It must have timezone information set.
    :param type: The type of event. One of order_created, order_changed or order_deleted.
    :param order: The order.
    """

    def __init__(self, when: datetime.datetime, type: str, order: Order):
        super().__init__(when)
        #: The event type. One of order_created, order_changed or order_deleted.
        self.type: str = type
        #: The order.
        self.order: Order = order


# Generate Order events from websocket messages.
class WebSocketEventSource(core_ws.ChannelEventSource):
    def __init__(self, pair: Pair, producer: event.Producer):
        super().__init__(producer=producer)
        self._pair = pair

    async def push_from_message(self, message: dict):
        self.push(OrderEvent(dt.utc_now(), message["event"], Order(self._pair, message["data"])))


def get_public_channel(pair: Pair) -> str:
    return "live_orders_{}".format(helpers.pair_to_currency_pair(pair))


def get_private_channel(pair: Pair) -> str:
    return "private-my_orders_{}".format(helpers.pair_to_currency_pair(pair))
