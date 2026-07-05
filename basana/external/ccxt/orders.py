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
import asyncio
import datetime
import logging

from . import helpers
from basana.core import dt, event, logs
from basana.core.enums import OrderOperation
from basana.core.pair import Pair


logger = logging.getLogger(__name__)


class Order:
    def __init__(self, pair: Pair, raw: dict):
        #: The trading pair.
        self.pair: Pair = pair
        #: The raw value returned by CCXT.
        self.raw: dict = raw

    @property
    def id(self) -> str:
        """The order id."""
        return str(self.raw["id"])

    @property
    def client_order_id(self) -> Optional[str]:
        """The client order id."""
        return self.raw.get("clientOrderId")

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


class WatchOrdersEventSource(event.FifoQueueEventSource, event.Producer):
    def __init__(self, cli: Any, pair: Pair, params: Optional[Dict[str, Any]] = None):
        if not cli.has.get("watchOrders"):
            raise NotImplementedError("The exchange does not support watchOrders")

        super().__init__(producer=self)
        self._cli = cli
        self._pair = pair
        self._symbol = helpers.pair_to_symbol(pair)
        self._params = dict(params or {})

    async def initialize(self):
        await self._cli.load_markets()

    async def main(self):
        while True:
            try:
                orders = await self._cli.watch_orders(self._symbol, params=self._params)
                self._handle_orders(orders)
            except Exception as e:
                logger.error(logs.StructuredMessage(
                    "Error watching orders", pair=self._pair, error=e
                ))
            await asyncio.sleep(0)

    async def finalize(self):
        if not self._cli.has.get("unWatchOrders"):
            return

        try:
            await self._cli.un_watch_orders(self._symbol, params=self._params)
        except Exception as error:
            logger.error(logs.StructuredMessage(
                "Error unwatching orders", pair=self._pair, error=error
            ))

    def _handle_orders(self, orders: List[dict]):
        def sort_key(order: dict) -> int:
            ret = order.get("lastTradeTimestamp")
            return 0 if ret is None else ret

        orders.sort(key=sort_key)
        for order in orders:
            timestamp = order.get("lastTradeTimestamp")
            if timestamp is not None:
                when = helpers.timestamp_to_datetime(int(timestamp))
            else:
                when = dt.utc_now(monotonic=True)
            self.push(OrderEvent(when, Order(self._pair, order)))


OrderEventHandler = Callable[[OrderEvent], Awaitable[Any]]