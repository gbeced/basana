# Basana - Hyperliquid connector
#
# Licensed under the Apache License, Version 2.0
#
# Main Exchange class - mirrors the Basana Binance Exchange interface
# but targets Hyperliquid perpetuals (the primary use case).
#
# Usage:
#   async with aiohttp.ClientSession() as session:
#       exchange = Exchange(
#           dispatcher=d,
#           private_key="0x...",   # optional; omit for read-only
#       )
#       exchange.subscribe_to_bar_events("ETH", "1h", my_handler)
#       await d.run()

from decimal import Decimal
from typing import Callable, Dict, List, Optional, Tuple
import dataclasses
import datetime
import logging

import aiohttp

from basana.core import bar, dispatcher
from basana.core.pair import Pair, PairInfo

from . import client, perps, websockets

logger = logging.getLogger(__name__)

# Re-export common types so callers only need to import from this module.
Error = client.Error
FillEvent = perps.FillEvent
FillEventHandler = perps.FillEventHandler
OrderInfo = perps.OrderInfo
Position = perps.Position

BarEventHandler = bar.BarEventHandler


@dataclasses.dataclass(frozen=True)
class AssetInfo(PairInfo):
    """Information about a Hyperliquid tradeable asset.

    :param sz_decimals: The number of decimal places for order size.
    :param max_leverage: Maximum allowed leverage.
    """

    sz_decimals: int
    max_leverage: int


class Exchange:
    """A Basana-compatible client for `Hyperliquid <https://hyperliquid.xyz/>`_ DEX.

    Supports perpetuals trading, real-time bar/trade/order-book feeds,
    and position management.

    :param dispatcher: The Basana event dispatcher.
    :param private_key: Optional EVM private key (hex, with 0x prefix).
        Required for trading and user-specific subscriptions.
        If omitted, only public market data endpoints are available.
    :param testnet: If True, connects to the Hyperliquid testnet.
    :param session: Optional aiohttp.ClientSession for connection reuse.
    """

    def __init__(
        self,
        dispatcher: dispatcher.EventDispatcher,
        private_key: Optional[str] = None,
        testnet: bool = False,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self._dispatcher = dispatcher
        self._cli = client.APIClient(private_key=private_key, testnet=testnet)
        self._ws_mgr = websockets.WebsocketManager(dispatcher, testnet=testnet, session=session)
        self._perps = perps.Account(self._cli, self._ws_mgr)
        self._asset_info: dict[str, AssetInfo] = {}

    # ------------------------------------------------------------------
    # Market data subscriptions (public)
    # ------------------------------------------------------------------

    def subscribe_to_bar_events(
        self,
        coin: str,
        interval: str,
        event_handler: BarEventHandler,
    ) -> None:
        """Subscribe to OHLCV bar events via WebSocket.

        :param coin: e.g. "ETH", "BTC"
        :param interval: One of "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w"
        :param event_handler: Async callable receiving a BarEvent.
        """
        pair = self._coin_to_pair(coin)

        async def _on_candle(data: dict) -> None:
            # Hyperliquid WS candle payload: {"t": ms, "T": ms, "o": str, "h": str, "l": str, "c": str, "v": str}
            # The WS only delivers closed candles when a new one opens, so no "isClosed" check needed.
            try:
                when = datetime.datetime.fromtimestamp(int(data["T"]) / 1e3, tz=datetime.timezone.utc)
                b = bar.Bar(
                    when,
                    pair,
                    Decimal(str(data["o"])),
                    Decimal(str(data["h"])),
                    Decimal(str(data["l"])),
                    Decimal(str(data["c"])),
                    Decimal(str(data["v"])),
                )
                await event_handler(bar.BarEvent(when, b))
            except (KeyError, ValueError) as e:
                logger.warning("Malformed candle data for %s: %s - %s", coin, e, data)

        self._ws_mgr.subscribe_to_candle_events(coin, interval, _on_candle)

    def subscribe_to_trade_events(
        self,
        coin: str,
        event_handler: Callable,
    ) -> None:
        """Subscribe to real-time trade events.

        :param coin: e.g. "ETH"
        :param event_handler: Async callable receiving a list of trade dicts.
        """
        self._ws_mgr.subscribe_to_trade_events(coin, event_handler)

    def subscribe_to_order_book_events(
        self,
        coin: str,
        event_handler: Callable,
    ) -> None:
        """Subscribe to L2 order book updates.

        :param coin: e.g. "ETH"
        :param event_handler: Async callable receiving the order book dict.
        """
        self._ws_mgr.subscribe_to_order_book_events(coin, event_handler)

    # ------------------------------------------------------------------
    # Market data queries (public, synchronous wrappers)
    # ------------------------------------------------------------------

    def get_mid_price(self, coin: str) -> Decimal:
        """Return the current mid price for a coin."""
        mids = self._cli.get_all_mids()
        if coin not in mids:
            raise Error(f"Unknown coin: {coin}")
        return Decimal(mids[coin])

    def get_bid_ask(self, coin: str) -> Tuple[Decimal, Decimal]:
        """Return the best bid and ask prices for a coin."""
        book = self._cli.get_l2_snapshot(coin)
        levels = book.get("levels", [[], []])
        bid = Decimal(levels[0][0]["px"]) if levels[0] else Decimal(0)
        ask = Decimal(levels[1][0]["px"]) if levels[1] else Decimal(0)
        return bid, ask

    def get_pair_info(self, coin: str) -> AssetInfo:
        """Return metadata for a tradeable asset.

        :param coin: e.g. "ETH", "BTC"
        """
        if coin not in self._asset_info:
            meta = self._cli.get_meta()
            for asset in meta.get("universe", []):
                name = asset["name"]
                self._asset_info[name] = AssetInfo(
                    base_precision=asset.get("szDecimals", 8),
                    quote_precision=6,  # Hyperliquid uses 6dp for USD
                    sz_decimals=asset.get("szDecimals", 8),
                    max_leverage=asset.get("maxLeverage", 50),
                )
        return self._asset_info[coin]

    def list_coins(self) -> List[str]:
        """Return a list of all tradeable perpetuals coins."""
        meta = self._cli.get_meta()
        return [a["name"] for a in meta.get("universe", [])]

    # ------------------------------------------------------------------
    # Perpetuals account
    # ------------------------------------------------------------------

    @property
    def perps_account(self) -> perps.Account:
        """Access perpetuals trading and account management.

        Example::

            order = exchange.perps_account.market_open("ETH", OrderOperation.BUY, Decimal("0.1"))
            positions = exchange.perps_account.get_positions()
        """
        return self._perps

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start WebSocket connections. Called automatically by the event dispatcher."""
        self._ws_mgr.start()

    async def stop(self) -> None:
        """Stop WebSocket connections and clean up."""
        await self._ws_mgr.stop()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _coin_to_pair(coin: str) -> Pair:
        return Pair(coin, "USD")
