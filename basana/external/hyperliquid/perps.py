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
from typing import Any, Awaitable, Callable, List, Optional
import dataclasses
import logging

from basana.core.enums import OrderOperation
from . import client, websockets


logger = logging.getLogger(__name__)

FillEventHandler = Callable[[dict], Awaitable[Any]]


@dataclasses.dataclass(frozen=True)
class Position:
    """An open perpetuals position."""

    #: The coin (e.g. ``"ETH"``).
    coin: str
    #: Size — positive for long, negative for short.
    size: Decimal
    #: Average entry price in USD.
    entry_price: Decimal
    #: Unrealized P&L in USD.
    unrealized_pnl: Decimal
    #: Liquidation price in USD, or ``None`` if not applicable.
    liquidation_price: Optional[Decimal]
    #: Leverage multiplier.
    leverage: Decimal
    #: Margin used in USD.
    margin_used: Decimal


@dataclasses.dataclass(frozen=True)
class OrderInfo:
    """A placed or open order."""

    #: Order ID returned by the exchange.
    oid: int
    coin: str
    is_buy: bool
    size: Decimal
    limit_price: Optional[Decimal]
    filled: Decimal
    status: str


class Account:
    """Hyperliquid perpetuals account.

    Provides order placement, position queries, and real-time fill subscriptions.

    :param api_client: The REST API client.
    :param ws_client: The WebSocket client.
    """

    def __init__(self, api_client: client.APIClient, ws_client: websockets.WebSocketClient):
        self._cli = api_client
        self._ws = ws_client

    # ------------------------------------------------------------------
    # Account state
    # ------------------------------------------------------------------

    async def get_positions(self) -> List[Position]:
        """Return all open perpetuals positions."""
        state = await self._cli.get_user_state()
        positions = []
        for p in state.get("assetPositions", []):
            pos = p.get("position", {})
            if pos.get("szi") == "0":
                continue
            positions.append(Position(
                coin=pos["coin"],
                size=Decimal(pos["szi"]),
                entry_price=Decimal(pos["entryPx"]) if pos.get("entryPx") else Decimal(0),
                unrealized_pnl=Decimal(pos.get("unrealizedPnl", "0")),
                liquidation_price=Decimal(pos["liquidationPx"]) if pos.get("liquidationPx") else None,
                leverage=Decimal(str(pos.get("leverage", {}).get("value", 1))),
                margin_used=Decimal(pos.get("marginUsed", "0")),
            ))
        return positions

    async def get_balance(self) -> Decimal:
        """Return the account equity (total margin balance) in USD."""
        state = await self._cli.get_user_state()
        return Decimal(state.get("marginSummary", {}).get("accountValue", "0"))

    async def get_open_orders(self) -> List[OrderInfo]:
        """Return all open orders."""
        raw = await self._cli.get_open_orders()
        return [
            OrderInfo(
                oid=o["oid"],
                coin=o["coin"],
                is_buy=o["side"] == "B",
                size=Decimal(o["sz"]),
                limit_price=Decimal(o["limitPx"]) if o.get("limitPx") else None,
                filled=Decimal(o.get("filledSz", "0")),
                status=o.get("orderType", "unknown"),
            )
            for o in raw
        ]

    # ------------------------------------------------------------------
    # Order management
    # ------------------------------------------------------------------

    async def market_open(self, coin: str, operation: OrderOperation, size: Decimal, slippage: float = 0.01) -> OrderInfo:
        """Open a position at market price.

        :param coin: e.g. ``"ETH"``
        :param operation: ``OrderOperation.BUY`` for long, ``OrderOperation.SELL`` for short.
        :param size: Position size in base asset units.
        :param slippage: Maximum acceptable slippage (default 1 %).
        """
        is_buy = operation == OrderOperation.BUY
        result = await self._cli.market_open(coin, is_buy, float(size), slippage)
        return self._parse_order_result(result, coin)

    async def market_close(self, coin: str, size: Optional[Decimal] = None, slippage: float = 0.01) -> OrderInfo:
        """Close a position at market price.

        :param coin: e.g. ``"ETH"``
        :param size: Amount to close. ``None`` closes the entire position.
        :param slippage: Maximum acceptable slippage (default 1 %).
        """
        result = await self._cli.market_close(coin, float(size) if size else None, slippage)
        return self._parse_order_result(result, coin)

    async def limit_order(
        self,
        coin: str,
        operation: OrderOperation,
        size: Decimal,
        limit_price: Decimal,
        reduce_only: bool = False,
    ) -> OrderInfo:
        """Place a limit order.

        :param coin: e.g. ``"ETH"``
        :param operation: ``OrderOperation.BUY`` or ``OrderOperation.SELL``.
        :param size: Size in base asset units.
        :param limit_price: Limit price in USD.
        :param reduce_only: If ``True``, the order can only reduce an existing position.
        """
        is_buy = operation == OrderOperation.BUY
        result = await self._cli.limit_order(coin, is_buy, float(size), float(limit_price), reduce_only)
        return self._parse_order_result(result, coin)

    async def cancel_order(self, coin: str, oid: int) -> None:
        """Cancel order ``oid`` for ``coin``."""
        await self._cli.cancel_order(coin, oid)

    async def set_leverage(self, coin: str, leverage: int, is_cross: bool = True) -> None:
        """Set leverage for ``coin``.

        :param coin: e.g. ``"ETH"``
        :param leverage: Leverage multiplier.
        :param is_cross: ``True`` for cross margin, ``False`` for isolated.
        """
        await self._cli.set_leverage(coin, leverage, is_cross)

    # ------------------------------------------------------------------
    # Real-time events
    # ------------------------------------------------------------------

    def subscribe_to_fill_events(self, handler: FillEventHandler) -> None:
        """Subscribe to real-time fill events via WebSocket.

        :param handler: Async callable receiving a fill event dict.
        """
        if not self._cli.address:
            raise client.Error("Private key required to subscribe to fill events")

        channel = websockets._user_fills_channel(self._cli.address)
        event_source = websockets.RawEventSource(producer=self._ws)
        self._ws.set_channel_event_source(channel, event_source)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_order_result(result: dict, coin: str) -> OrderInfo:
        statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])
        s = statuses[0] if statuses else {}
        filled = s.get("filled", {})
        return OrderInfo(
            oid=filled.get("oid", 0),
            coin=coin,
            is_buy=True,
            size=Decimal(str(filled.get("totalSz", "0"))),
            limit_price=None,
            filled=Decimal(str(filled.get("totalSz", "0"))),
            status="filled" if filled else s.get("error", "unknown"),
        )
