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

from basana.external.binance import exchange, spot
import basana as bs


from samples.core import position_manager as core

# Re-export shared types for callers and tests that import from this module.
OrderInfo = core.OrderInfo
PositionInfo = core.PositionInfo


class SpotExchange(core.Exchange):
    """Wraps a Binance spot account for use with :class:`~samples.core.position_manager.PositionManager`."""

    def __init__(self, exchange: exchange.Exchange):
        self._exchange = exchange

    async def get_bid_ask(self, pair: bs.Pair) -> Tuple[Decimal, Decimal]:
        return await self._exchange.get_bid_ask(pair)

    async def get_pair_info(self, pair: bs.Pair) -> bs.PairInfo:
        return await self._exchange.get_pair_info(pair)

    async def get_order_info(self, pair: bs.Pair, order_id: str) -> OrderInfo:
        order = await self._exchange.spot_account.get_order_info(pair, order_id=order_id)
        return OrderInfo(
            id=order.id, operation=order.operation, is_open=order.is_open,
            amount_filled=order.amount_filled, fill_price=order.fill_price,
        )

    async def cancel_order(self, pair: bs.Pair, order_id: str) -> None:
        await self._exchange.spot_account.cancel_order(pair, order_id=order_id)

    async def create_market_order(
            self, operation: bs.OrderOperation, pair: bs.Pair, amount: Decimal
    ) -> str:
        created_order = await self._exchange.spot_account.create_market_order(operation, pair, amount)
        return created_order.id


class SpotAccountPositionManager(core.PositionManager):
    # Responsible for managing orders and tracking positions in response to trading signals.
    def __init__(
            self, exchange: exchange.Exchange, position_amount: Decimal, quote_symbol: str,
            stop_loss_pct: Decimal
    ):
        super().__init__(
            SpotExchange(exchange), position_amount, quote_symbol, stop_loss_pct,
        )
        self._binance_exchange = exchange

    async def on_order_event(self, order_event: spot.OrderEvent):
        order_update = order_event.order_update
        pair = await self._binance_exchange.symbol_to_pair(order_update.symbol)
        await self.on_order_update(
            pair, order_update.id, order_update.is_open, order_update.amount, order_update.amount_filled,
            order_update.fill_price,
        )
