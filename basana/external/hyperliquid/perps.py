# Basana - Hyperliquid connector
#
# Licensed under the Apache License, Version 2.0
#
# Perpetuals account: place/cancel orders, track positions, subscribe to fills.

from decimal import Decimal
from typing import Callable, List, Optional
import dataclasses
import logging

from basana.core import enums
from . import client, websockets

logger = logging.getLogger(__name__)

OrderOperation = enums.OrderOperation


@dataclasses.dataclass
class Position:
    """An open perpetuals position."""

    #: The coin (e.g. "ETH").
    coin: str
    #: Size (positive = long, negative = short).
    size: Decimal
    #: Average entry price.
    entry_price: Decimal
    #: Unrealized P&L in USD.
    unrealized_pnl: Decimal
    #: Liquidation price.
    liquidation_price: Optional[Decimal]
    #: Leverage.
    leverage: Decimal
    #: Margin used in USD.
    margin_used: Decimal


@dataclasses.dataclass
class OrderInfo:
    """A placed or open order."""

    oid: int
    coin: str
    is_buy: bool
    size: Decimal
    limit_price: Optional[Decimal]
    filled: Decimal
    status: str


@dataclasses.dataclass
class FillEvent:
    """A trade fill event received via WebSocket."""

    coin: str
    size: Decimal
    price: Decimal
    side: str  # "B" (buy) or "A" (ask/sell)
    fee: Decimal
    timestamp_ms: int


FillEventHandler = Callable[[FillEvent], None]


class Account:
    """Hyperliquid perpetuals account.

    Provides order placement, position queries, and real-time fill events.

    :param api_client: The REST API client.
    :param ws_manager: The WebSocket manager.
    """

    def __init__(self, api_client: client.APIClient, ws_manager: websockets.WebsocketManager):
        self._cli = api_client
        self._ws = ws_manager

    # ------------------------------------------------------------------
    # Account state
    # ------------------------------------------------------------------

    def get_positions(self) -> List[Position]:
        """Return all open perpetuals positions."""
        state = self._cli.get_user_state()
        positions = []
        for p in state.get("assetPositions", []):
            pos = p.get("position", {})
            if pos.get("szi") == "0":
                continue
            size = Decimal(pos["szi"])
            positions.append(Position(
                coin=pos["coin"],
                size=size,
                entry_price=Decimal(pos["entryPx"]) if pos.get("entryPx") else Decimal(0),
                unrealized_pnl=Decimal(pos.get("unrealizedPnl", "0")),
                liquidation_price=Decimal(pos["liquidationPx"]) if pos.get("liquidationPx") else None,
                leverage=Decimal(str(pos.get("leverage", {}).get("value", 1))),
                margin_used=Decimal(pos.get("marginUsed", "0")),
            ))
        return positions

    def get_balance(self) -> Decimal:
        """Return the account equity (total margin balance) in USD."""
        state = self._cli.get_user_state()
        return Decimal(state.get("marginSummary", {}).get("accountValue", "0"))

    def get_open_orders(self) -> List[OrderInfo]:
        """Return all open orders."""
        raw = self._cli.get_open_orders()
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

    def market_open(self, coin: str, operation: OrderOperation, size: Decimal, slippage: float = 0.01) -> OrderInfo:
        """Open a position at market price.

        :param coin: e.g. "ETH"
        :param operation: OrderOperation.BUY for long, OrderOperation.SELL for short.
        :param size: Position size in base asset units.
        :param slippage: Maximum acceptable slippage (default 1%).
        """
        is_buy = operation == OrderOperation.BUY
        result = self._cli.market_open(coin, is_buy, float(size), slippage)
        return self._parse_order_result(result, coin)

    def market_close(self, coin: str, size: Optional[Decimal] = None, slippage: float = 0.01) -> OrderInfo:
        """Close a position at market price.

        :param coin: e.g. "ETH"
        :param size: Amount to close. If None, closes the entire position.
        :param slippage: Maximum acceptable slippage (default 1%).
        """
        result = self._cli.market_close(coin, float(size) if size else None, slippage)
        return self._parse_order_result(result, coin)

    def limit_order(
        self,
        coin: str,
        operation: OrderOperation,
        size: Decimal,
        limit_price: Decimal,
        reduce_only: bool = False,
    ) -> OrderInfo:
        """Place a limit order.

        :param coin: e.g. "ETH"
        :param operation: OrderOperation.BUY or OrderOperation.SELL.
        :param size: Size in base asset units.
        :param limit_price: Limit price in USD.
        :param reduce_only: If True, the order only reduces an existing position.
        """
        is_buy = operation == OrderOperation.BUY
        result = self._cli.limit_order(coin, is_buy, float(size), float(limit_price), reduce_only)
        return self._parse_order_result(result, coin)

    def cancel_order(self, coin: str, oid: int) -> None:
        """Cancel an open order by ID."""
        self._cli.cancel_order(coin, oid)

    def cancel_all_orders(self, coin: Optional[str] = None) -> None:
        """Cancel all open orders, optionally filtered by coin."""
        self._cli.cancel_all_orders(coin)

    def set_leverage(self, coin: str, leverage: int, is_cross: bool = True) -> None:
        """Set leverage for a coin.

        :param coin: e.g. "ETH"
        :param leverage: Leverage multiplier.
        :param is_cross: True for cross margin, False for isolated.
        """
        self._cli.set_leverage(coin, leverage, is_cross)

    # ------------------------------------------------------------------
    # Real-time events
    # ------------------------------------------------------------------

    def subscribe_to_fill_events(self, handler: FillEventHandler) -> None:
        """Subscribe to real-time fill events via WebSocket.

        :param handler: Async or sync callable receiving a FillEvent.
        """
        if not self._cli.address:
            raise client.Error("Private key required to subscribe to fill events")

        async def _on_fill(data: dict) -> None:
            for fill in data.get("fills", [data]):
                event = FillEvent(
                    coin=fill.get("coin", ""),
                    size=Decimal(str(fill.get("sz", "0"))),
                    price=Decimal(str(fill.get("px", "0"))),
                    side=fill.get("side", ""),
                    fee=Decimal(str(fill.get("fee", "0"))),
                    timestamp_ms=fill.get("time", 0),
                )
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)

        self._ws.subscribe_to_user_fills(self._cli.address, _on_fill)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_order_result(result: dict, coin: str) -> OrderInfo:
        statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])
        s = statuses[0] if statuses else {}
        filled = s.get("filled", {})
        return OrderInfo(
            oid=filled.get("oid", 0),
            coin=coin,
            is_buy=True,  # Determined by caller
            size=Decimal(str(filled.get("totalSz", "0"))),
            limit_price=None,
            filled=Decimal(str(filled.get("totalSz", "0"))),
            status="filled" if filled else s.get("error", "unknown"),
        )


import asyncio  # noqa: E402 (needed for fill handler)
