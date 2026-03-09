# Basana
#
# Copyright 2026 Christian Pojoni
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
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
import dataclasses
import logging

import aiohttp

from basana.core import bar, dispatcher, event
from basana.core.pair import Pair, PairInfo
from . import client, config, helpers, perps, websockets


logger = logging.getLogger(__name__)

BarEventHandler = Callable[[bar.BarEvent], Awaitable[Any]]
Error = client.Error
OrderInfo = perps.OrderInfo
Position = perps.Position


@dataclasses.dataclass(frozen=True)
class AssetInfo(PairInfo):
    """Information about a Hyperliquid tradeable asset.

    :param sz_decimals: The number of decimal places for order size.
    :param max_leverage: Maximum allowed leverage.
    """

    #: The number of decimal places for order size.
    sz_decimals: int
    #: Maximum allowed leverage.
    max_leverage: int


class BarEventSource(websockets.RawEventSource):
    """Converts raw Hyperliquid candle messages into Basana :class:`~basana.core.bar.BarEvent` objects."""

    def __init__(self, pair: Pair, producer: event.Producer):
        super().__init__(producer=producer)
        self._pair = pair

    async def push_from_message(self, message: dict):
        try:
            begin = helpers.timestamp_to_datetime(int(message["t"]))
            when = helpers.timestamp_to_datetime(int(message["T"]))
            duration = when - begin
            b = bar.Bar(
                begin,
                self._pair,
                Decimal(str(message["o"])),
                Decimal(str(message["h"])),
                Decimal(str(message["l"])),
                Decimal(str(message["c"])),
                Decimal(str(message["v"])),
                duration,
            )
            self.push(bar.BarEvent(when, b))
        except (KeyError, ValueError) as e:
            logger.warning("Malformed candle message for %s: %s — %s", self._pair, e, message)


class Exchange:
    """A client for `Hyperliquid <https://hyperliquid.xyz/>`_ decentralized perpetuals exchange.

    Supports perpetuals trading and real-time bar, trade, and order-book feeds.

    :param dispatcher: The event dispatcher.
    :param private_key: Optional EVM private key (hex string with 0x prefix).
        Required for trading and user-specific WebSocket subscriptions.
        If omitted, only public market data endpoints are available.
    :param session: Optional :class:`aiohttp.ClientSession` for connection reuse.
    :param config_overrides: Optional dict for overriding config settings.
    :param testnet: If ``True``, use Hyperliquid testnet defaults unless overridden.
    """

    def __init__(
        self,
        dispatcher: dispatcher.EventDispatcher,
        private_key: Optional[str] = None,
        session: Optional[aiohttp.ClientSession] = None,
        config_overrides: Optional[dict] = None,
        testnet: bool = False,
    ):
        if config_overrides is None:
            config_overrides = {}
        if testnet:
            merged_overrides = {
                "api": {
                    "http": dict(config.TESTNET_DEFAULTS["api"]["http"]),
                    "websockets": dict(config.TESTNET_DEFAULTS["api"]["websockets"]),
                }
            }
            for section, values in config_overrides.get("api", {}).items():
                merged_overrides["api"].setdefault(section, {}).update(values)
            config_overrides = merged_overrides
        self._dispatcher = dispatcher
        self._cli = client.APIClient(private_key=private_key, config_overrides=config_overrides)
        self._ws = websockets.WebSocketClient(session=session, config_overrides=config_overrides)
        self._perps = perps.Account(self._cli, self._ws, dispatcher)
        self._asset_info: Dict[str, AssetInfo] = {}

    # ------------------------------------------------------------------
    # Market data subscriptions
    # ------------------------------------------------------------------

    def subscribe_to_bar_events(self, coin: str, interval: str, event_handler: BarEventHandler) -> None:
        """Subscribe to OHLCV bar events for ``coin``.

        :param coin: e.g. ``"ETH"``, ``"BTC"``
        :param interval: One of ``"1m"``, ``"5m"``, ``"15m"``, ``"30m"``, ``"1h"``, ``"4h"``, ``"1d"``
        :param event_handler: Async callable receiving a :class:`~basana.core.bar.BarEvent`.
        """
        pair = helpers.coin_to_pair(coin)
        self._register_channel(
            websockets._candle_channel(coin, interval),
            BarEventSource(pair=pair, producer=self._ws),
            event_handler,
        )

    def subscribe_to_trade_events(self, coin: str, event_handler: Callable) -> None:
        """Subscribe to real-time trade events for ``coin``.

        :param coin: e.g. ``"ETH"``
        :param event_handler: Async callable receiving raw trade message dicts.
        """
        self._register_channel(
            websockets._trades_channel(coin),
            websockets.RawEventSource(producer=self._ws),
            event_handler,
        )

    def subscribe_to_order_book_events(self, coin: str, event_handler: Callable) -> None:
        """Subscribe to L2 order book updates for ``coin``.

        :param coin: e.g. ``"ETH"``
        :param event_handler: Async callable receiving raw order book message dicts.
        """
        self._register_channel(
            websockets._l2_book_channel(coin),
            websockets.RawEventSource(producer=self._ws),
            event_handler,
        )

    def _register_channel(
        self,
        channel: str,
        event_source: websockets.RawEventSource,
        event_handler: Callable,
    ) -> None:
        self._ws.set_channel_event_source(channel, event_source)
        self._dispatcher.subscribe(event_source, event_handler)

    # ------------------------------------------------------------------
    # Market data queries
    # ------------------------------------------------------------------

    async def get_mid_price(self, coin: str) -> Decimal:
        """Return the current mid price for ``coin``."""
        mids = await self._cli.get_all_mids()
        if coin not in mids:
            raise Error(f"Unknown coin: {coin}")
        return Decimal(mids[coin])

    async def get_bid_ask(self, coin: str) -> Tuple[Decimal, Decimal]:
        """Return the best bid and ask prices for ``coin``."""
        book = await self._cli.get_l2_snapshot(coin)
        levels = book.get("levels", [[], []])
        bid = Decimal(levels[0][0]["px"]) if levels[0] else Decimal(0)
        ask = Decimal(levels[1][0]["px"]) if levels[1] else Decimal(0)
        return bid, ask

    async def get_pair_info(self, coin: str) -> AssetInfo:
        """Return metadata for a tradeable asset.

        :param coin: e.g. ``"ETH"``, ``"BTC"``
        """
        if coin not in self._asset_info:
            meta = await self._cli.get_meta()
            for asset in meta.get("universe", []):
                name = asset["name"]
                self._asset_info[name] = AssetInfo(
                    base_precision=asset.get("szDecimals", 8),
                    quote_precision=6,
                    sz_decimals=asset.get("szDecimals", 8),
                    max_leverage=asset.get("maxLeverage", 50),
                )
        if coin not in self._asset_info:
            raise Error(f"Unknown coin: {coin}")
        return self._asset_info[coin]

    async def list_coins(self) -> List[str]:
        """Return a list of all tradeable perpetuals coins."""
        meta = await self._cli.get_meta()
        return [a["name"] for a in meta.get("universe", [])]

    # ------------------------------------------------------------------
    # Perpetuals account
    # ------------------------------------------------------------------

    @property
    def perps_account(self) -> perps.Account:
        """Access perpetuals trading and position management."""
        return self._perps

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def main(self):
        """Run the WebSocket client. Called automatically by the event dispatcher."""
        await self._ws.main()
