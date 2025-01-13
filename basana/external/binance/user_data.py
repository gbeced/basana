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
from typing import Any, Awaitable, Callable, Dict, Optional
import datetime

from . import helpers
from basana.core import event, websockets as core_ws
from basana.core.enums import OrderOperation


class OrderUpdate:
    def __init__(self, json: dict):
        assert json["e"] == "executionReport"

        #: The JSON representation.
        self.json: dict = json

    @property
    def id(self) -> str:
        """The order id."""
        return str(self.json["i"])

    @property
    def symbol(self) -> str:
        """Symbol."""
        return str(self.json["s"])

    @property
    def client_order_id(self) -> str:
        """The client order id."""
        return self.json["c"]

    @property
    def operation(self) -> OrderOperation:
        """The operation."""
        return helpers.side_to_order_operation(self.json["S"])

    @property
    def type(self) -> str:
        """
        The type of order.

        Check **Order types** in
        https://developers.binance.com/docs/binance-spot-api-docs/enums#order-types-ordertypes-type.
        """
        return self.json["o"]

    @property
    def time_in_force(self) -> Optional[str]:
        """
        The time in force.

        Check **Time in force** in
        https://developers.binance.com/docs/binance-spot-api-docs/enums#time-in-force-timeinforce.
        """
        return self.json.get("f")

    @property
    def amount(self) -> Decimal:
        """The amount."""
        return Decimal(self.json["q"])

    @property
    def quote_amount(self) -> Optional[Decimal]:
        """The amount set in quote units."""
        return helpers.get_optional_decimal(self.json, "Q", True)

    @property
    def limit_price(self) -> Optional[Decimal]:
        """The limit price."""
        return helpers.get_optional_decimal(self.json, "p", True)

    @property
    def stop_price(self) -> Optional[Decimal]:
        """The stop price."""
        return helpers.get_optional_decimal(self.json, "P", True)

    @property
    def order_list_id(self) -> Optional[str]:
        """The order list id."""
        ret = self.json.get("g")
        ret = None if ret in [None, -1] else str(ret)
        return ret

    @property
    def status(self) -> str:
        """
        The status.

        Check **Order status** in https://developers.binance.com/docs/binance-spot-api-docs/enums#order-status-status.
        """
        return self.json["X"]

    @property
    def is_open(self) -> bool:
        """True if the order is open, False otherwise."""
        return helpers.order_status_is_open(self.status)

    @property
    def amount_filled(self) -> Decimal:
        """The cumulative amount filled."""
        return Decimal(self.json["z"])

    @property
    def quote_amount_filled(self) -> Decimal:
        """The cumulative amount filled in quote units."""
        return Decimal(self.json["Z"])

    @property
    def fees(self) -> Dict[str, Decimal]:
        """The fees."""
        ret = {}
        if commision_asset := self.json.get("N"):
            ret[commision_asset] = Decimal(self.json["n"])
        return ret


class Event(event.Event):
    def __init__(self, when: datetime.datetime, json: dict):
        super().__init__(when)
        #: The JSON representation.
        self.json: dict = json


class OrderEvent(Event):
    """
    An event for order updates.

    :param when: The datetime when the event occurred. It must have timezone information set.
    :param order_update: The order update.
    """

    def __init__(self, when: datetime.datetime, json: dict):
        super().__init__(when, json)

        #: The order update.
        self.order_update: OrderUpdate = OrderUpdate(json)


# Generate events from websocket messages.
class WebSocketEventSource(core_ws.ChannelEventSource):
    def __init__(self, producer: event.Producer):
        super().__init__(producer=producer)

    async def push_from_message(self, message: dict):
        json = message["data"]
        # Push the event.
        event_cls = {
            "executionReport": OrderEvent,
        }.get(json["e"], Event)
        event = event_cls(helpers.timestamp_to_datetime(int(json["E"])), json)
        self.push(event)


OrderEventHandler = Callable[[OrderEvent], Awaitable[Any]]
UserDataEventHandler = Callable[[Event], Awaitable[Any]]
