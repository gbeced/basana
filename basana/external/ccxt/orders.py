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
from typing import Any, Awaitable, Callable, Dict, List, Optional
import datetime
import functools

from . import helpers
from .watch_event_source import WatchEventSource
from basana.core import dt, event
from basana.core.enums import OrderOperation
from basana.core.pair import Pair


class Order:
    def __init__(self, pair: Pair, raw: dict):
        #: The trading pair.
        self.pair: Pair = pair
        #: The raw value returned by CCXT.
        self.raw: dict = raw

        # Sort key, for internal use.
        for key in ["lastUpdateTimestamp", "lastTradeTimestamp", "timestamp"]:
            if (value := raw.get(key)) is not None:
                self._sort_key = helpers.timestamp_to_datetime(int(value))
                break
        else:
            self._sort_key = dt.utc_now(monotonic=True)

    @property
    def id(self) -> str:
        """The order id."""
        return str(self.raw["id"])

    @property
    def client_order_id(self) -> Optional[str]:
        """The client order id."""
        return helpers.optional_client_order_id(self.raw)

    @property
    def operation(self) -> OrderOperation:
        """The operation."""
        return helpers.side_to_order_operation(self.raw["side"])

    @property
    def type(self) -> str:
        """The type of order."""
        return self.raw["type"]

    @property
    def is_open(self) -> bool:
        """True if the order is open, False otherwise."""
        return helpers.order_status_is_open(self.raw["status"])

    @property
    def amount(self) -> Decimal:
        """The amount."""
        return helpers.to_decimal(self.raw["amount"])

    @property
    def amount_filled(self) -> Decimal:
        """The amount filled."""
        return helpers.to_decimal(self.raw.get("filled", 0))

    @property
    def amount_remaining(self) -> Decimal:
        """The amount remaining to be filled."""
        return helpers.to_decimal(self.raw.get("remaining", 0))

    @property
    def limit_price(self) -> Optional[Decimal]:
        """The limit price."""
        return helpers.optional_decimal(self.raw.get("price"))

    @property
    def stop_price(self) -> Optional[Decimal]:
        """The stop price."""
        return helpers.optional_decimal(self.raw.get("stopPrice"))

    @property
    def quote_amount_filled(self) -> Decimal:
        """The amount filled in quote units."""
        return helpers.to_decimal(self.raw.get("cost", 0))

    @property
    def fill_price(self) -> Optional[Decimal]:
        """The fill price."""
        if self.amount_filled == 0:
            return None
        return self.quote_amount_filled / self.amount_filled


class OrderEvent(event.Event):
    """An event for order updates.

    :param when: The datetime when the event occurred. It must have timezone information set.
    :param order: The order.
    """

    def __init__(self, when: datetime.datetime, order: Order):
        super().__init__(when)
        #: The order.
        self.order: Order = order


class WatchOrdersEventSource(WatchEventSource):
    def __init__(self, cli: Any, pair: Pair, params: Optional[Dict[str, Any]] = None):
        super().__init__(cli, pair, "watchOrders", "unWatchOrders")
        self._params = dict(params or {})

    async def watch(self):
        orders = await self._cli.watch_orders(self._symbol, params=self._params)
        self._handle_orders(orders)

    async def unwatch(self):
        await self._cli.un_watch_orders(self._symbol, params=self._params)

    def _handle_orders(self, raw_orders: List[dict]):
        orders = list(map(functools.partial(Order, self._pair), raw_orders))
        orders.sort(key=lambda order: order._sort_key)
        for order in orders:
            self.push(OrderEvent(dt.utc_now(monotonic=True), order))


OrderEventHandler = Callable[[OrderEvent], Awaitable[Any]]