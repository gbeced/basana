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
from typing import Tuple

import basana as bs
import basana.backtesting.exchange as backtesting_exchange

from samples.core import position_manager as core

# Re-export shared types for callers and tests that import from this module.
OrderInfo = core.OrderInfo
PositionInfo = core.PositionInfo


class Exchange(core.Exchange):
    """Wraps a backtesting exchange for use with :class:`~samples.core.position_manager.PositionManager`."""

    def __init__(self, exchange: backtesting_exchange.Exchange):
        self._exchange = exchange

    async def get_bid_ask(self, pair: bs.Pair) -> Tuple[Decimal, Decimal]:
        return await self._exchange.get_bid_ask(pair)

    async def get_pair_info(self, pair: bs.Pair) -> bs.PairInfo:
        return await self._exchange.get_pair_info(pair)

    async def get_order_info(self, pair: bs.Pair, order_id: str) -> core.OrderInfo:
        order = await self._exchange.get_order_info(order_id)
        return core.OrderInfo(
            id=order.id, operation=order.operation, is_open=order.is_open,
            amount_filled=order.amount_filled, fill_price=order.fill_price,
        )

    async def cancel_order(self, pair: bs.Pair, order_id: str) -> None:
        await self._exchange.cancel_order(order_id)

    async def create_market_order(
            self, operation: bs.OrderOperation, pair: bs.Pair, amount: Decimal
    ) -> str:
        created_order = await self._exchange.create_market_order(
            operation, pair, amount, auto_borrow=True, auto_repay=True
        )
        return created_order.id


class PositionManager(core.PositionManager):
    # Responsible for managing orders and tracking positions in response to trading signals.
    def __init__(
            self, exchange: backtesting_exchange.Exchange, position_amount: Decimal, quote_symbol: str,
            stop_loss_pct: Decimal, borrowing_disabled: bool = False
    ):
        super().__init__(
            Exchange(exchange), position_amount, quote_symbol, stop_loss_pct,
            borrowing_disabled=borrowing_disabled,
        )

    async def on_order_event(self, order_event: backtesting_exchange.OrderEvent):
        order = order_event.order
        await self.on_order_update(
            order.pair, order.id, order.is_open, order.amount, order.amount_filled, order.fill_price,
        )
