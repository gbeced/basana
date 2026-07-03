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
from basana.core import event, logs
from basana.core.pair import Pair


logger = logging.getLogger(__name__)


class Trade:
    def __init__(self, pair: Pair, json: dict):
        #: The trading pair.
        self.pair: Pair = pair
        #: The JSON representation.
        self.json: dict = json

    @property
    def id(self) -> str:
        """The trade id."""
        return str(self.json["id"])

    @property
    def datetime(self) -> datetime.datetime:
        """The trade datetime."""
        return helpers.timestamp_to_datetime(int(self.json["timestamp"]))

    @property
    def price(self) -> Decimal:
        """The price."""
        return helpers.to_decimal(self.json["price"])

    @property
    def amount(self) -> Decimal:
        """The amount."""
        return helpers.to_decimal(self.json["amount"])

    @property
    def buy_order_id(self) -> Optional[str]:
        """The buyer order id."""
        buy_order_id = self.json.get("info", {}).get("b")
        return None if buy_order_id is None else str(buy_order_id)

    @property
    def sell_order_id(self) -> Optional[str]:
        """The seller order id."""
        sell_order_id = self.json.get("info", {}).get("a")
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


class WatchTradesEventSource(event.FifoQueueEventSource, event.Producer):
    def __init__(self, cli: Any, pair: Pair, params: Optional[Dict[str, Any]] = None):
        if not cli.has.get("watchTrades"):
            raise NotImplementedError("The exchange does not support watchTrades")

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
                trades = await self._cli.watch_trades(self._symbol, params=self._params)
                self._handle_trades(trades)
            except Exception as e:
                logger.error(logs.StructuredMessage(
                    "Error watching trades", pair=self._pair, error=e
                ))
            # Yield to the event loop to allow other tasks to run.
            await asyncio.sleep(0)

    async def finalize(self):
        if not self._cli.has.get("unWatchTrades"):
            return

        try:
            await self._cli.un_watch_trades(self._symbol)
        except Exception as error:
            logger.error(logs.StructuredMessage(
                "Error unwatching trades", pair=self._pair, error=error
            ))

    def _handle_trades(self, trades: List[dict]):
        for trade in trades:
            when = helpers.timestamp_to_datetime(int(trade["timestamp"]))
            self.push(TradeEvent(when, Trade(self._pair, trade)))


TradeEventHandler = Callable[[TradeEvent], Awaitable[Any]]