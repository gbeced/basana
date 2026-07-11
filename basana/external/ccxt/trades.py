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

from . import helpers
from .watch_event_source import WatchEventSource
from basana.core import event
from basana.core.pair import Pair


class Trade:
    def __init__(self, pair: Pair, raw: dict):
        #: The trading pair.
        self.pair: Pair = pair
        #: The raw value returned by CCXT.
        self.raw: dict = raw

    @property
    def id(self) -> str:
        """The trade id."""
        return str(self.raw["id"])

    @property
    def datetime(self) -> datetime.datetime:
        """The trade datetime."""
        return helpers.timestamp_to_datetime(int(self.raw["timestamp"]))

    @property
    def price(self) -> Decimal:
        """The price."""
        return helpers.to_decimal(self.raw["price"])

    @property
    def amount(self) -> Decimal:
        """The amount."""
        return helpers.to_decimal(self.raw["amount"])

    @property
    def buy_order_id(self) -> Optional[str]:
        """The buyer order id."""
        buy_order_id = self.raw.get("info", {}).get("b")
        return None if buy_order_id is None else str(buy_order_id)

    @property
    def sell_order_id(self) -> Optional[str]:
        """The seller order id."""
        sell_order_id = self.raw.get("info", {}).get("a")
        return None if sell_order_id is None else str(sell_order_id)


class TradeEvent(event.Event):
    """An event for new trades.

    :param when: The datetime when the event occurred. It must have timezone information set.
    :param trade: The trade.
    """

    def __init__(self, when: datetime.datetime, trade: Trade):
        super().__init__(when)
        #: The trade.
        self.trade: Trade = trade


class WatchTradesEventSource(WatchEventSource):
    def __init__(self, cli: Any, pair: Pair, params: Optional[Dict[str, Any]] = None):
        super().__init__(cli, pair, "watchTrades", "unWatchTrades")
        self._params = dict(params or {})

    async def watch(self):
        trades = await self._cli.watch_trades(self._symbol, params=self._params)
        self._handle_trades(trades)

    async def unwatch(self):
        await self._cli.un_watch_trades(self._symbol)

    def _handle_trades(self, trades: List[dict]):
        trades.sort(key=lambda trade: int(trade["timestamp"]))
        for trade in trades:
            when = helpers.timestamp_to_datetime(int(trade["timestamp"]))
            self.push(TradeEvent(when, Trade(self._pair, trade)))


TradeEventHandler = Callable[[TradeEvent], Awaitable[Any]]