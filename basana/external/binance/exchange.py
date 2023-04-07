# Basana
#
# Copyright 2022-2023 Gabriel Martin Becedillas Ruiz
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
from typing import cast, Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union
import dataclasses

import aiohttp

from . import client, helpers, order_book, trades, websockets as binance_ws, spot, cross_margin, isolated_margin, klines
from basana.core import bar, dispatcher, enums, event, token_bucket, websockets as core_ws
from basana.core.pair import Pair, PairInfo


@dataclasses.dataclass(frozen=True)
class PairInfoEx(PairInfo):
    permissions: List[str]


BarEventHandler = Callable[[bar.BarEvent], Awaitable[Any]]
Error = client.Error
OrderBookEvent = order_book.OrderBookEvent
OrderBookEventHandler = Callable[[order_book.OrderBookEvent], Awaitable[Any]]
OrderOperation = enums.OrderOperation
TradeEvent = trades.TradeEvent
TradeEventHandler = Callable[[trades.TradeEvent], Awaitable[Any]]


class Exchange:
    def __init__(
            self, dispatcher: dispatcher.EventDispatcher, api_key: Optional[str] = None,
            api_secret: Optional[str] = None, session: Optional[aiohttp.ClientSession] = None,
            tb: Optional[token_bucket.TokenBucketLimiter] = None, config_overrides: dict = {}
    ):
        self._dispatcher = dispatcher
        self._cli = client.APIClient(
            api_key=api_key, api_secret=api_secret, session=session, tb=tb, config_overrides=config_overrides
        )
        self._session = session
        self._tb = tb
        self._config_overrides = config_overrides
        self._websocket: Optional[binance_ws.WebSocketClient] = None
        self._channel_to_event_source: Dict[str, event.EventSource] = {}
        self._pair_info_cache: Dict[Pair, PairInfoEx] = {}

    def subscribe_to_bar_events(
            self, pair: Pair, bar_duration: Union[int, str], event_handler: BarEventHandler,
            skip_first_bar: bool = True, flush_delay: float = 1
    ):
        """Registers an async callable that will be called when a new bar is available.

        :param pair: The trading pair.
        :param interval: The bar interval. One of 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M.
        :type interval: str
        :param event_handler: An async callable that receives a :class:`basana.BarEvent`.
        :param skip_first_bar: Ignored.
        :param flush_delay: Ignored.
        """

        # TODO: Deprecate support for bar_duration as int.
        interval = {
            # Supporting interval as int for backwards compatibility reasons.
            1: "1s",
            60: "1m",
            3 * 60: "3m",
            5 * 60: "5m",
            15 * 60: "15m",
            30 * 60: "30m",
            3600: "1h",
            2 * 3600: "2h",
            4 * 3600: "4h",
            6 * 3600: "6h",
            8 * 3600: "8h",
            12 * 3600: "12h",
            86400: "1d",
            3 * 86400: "3d",
            7 * 86400: "1w",
            31 * 86400: "1M",
            # Once support for interval as int is removed, this should be simplified.
            "1s": "1s",
            "1m": "1m",
            "3m": "3m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1h",
            "2h": "2h",
            "4h": "4h",
            "6h": "6h",
            "8h": "8h",
            "12h": "12h",
            "1d": "1d",
            "3d": "3d",
            "1w": "1w",
            "1M": "1M",
        }.get(bar_duration)
        assert interval, "Invalid bar_duration"

        channel = klines.get_channel(pair, interval)
        self._subscribe_to_ws_channel_events(
            channel,
            lambda ws_cli: klines.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_order_book_events(
            self, pair: Pair, event_handler: OrderBookEventHandler, depth: int = 10
    ):
        channel = order_book.get_channel(pair, depth)
        self._subscribe_to_ws_channel_events(
            channel,
            lambda ws_cli: order_book.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_trade_events(self, pair: Pair, event_handler: TradeEventHandler):
        channel = trades.get_channel(pair)
        self._subscribe_to_ws_channel_events(
            channel,
            lambda ws_cli: trades.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    async def get_pair_info(self, pair: Pair) -> PairInfoEx:
        ret = self._pair_info_cache.get(pair)
        if not ret:
            exchange_info = await self._cli.get_exchange_info(helpers.pair_to_order_book_symbol(pair))
            symbols = exchange_info["symbols"]
            assert len(symbols) == 1, "More than 1 symbol found"
            symbol_info = symbols[0]
            price_filter = get_filter_from_symbol_info(symbol_info, "PRICE_FILTER")
            assert price_filter, f"PRICE_FILTER not found for {pair}"
            lot_size = get_filter_from_symbol_info(symbol_info, "LOT_SIZE")
            assert lot_size, f"LOT_SIZE not found for {pair}"
            ret = PairInfoEx(
                base_precision=get_precision_from_step_size(lot_size["stepSize"]),
                quote_precision=get_precision_from_step_size(price_filter["tickSize"]),
                permissions=symbol_info.get("permissions")
            )
            self._pair_info_cache[pair] = ret
        return ret

    async def get_bid_ask(self, pair: Pair) -> Tuple[Decimal, Decimal]:
        order_book = await self._cli.get_order_book(helpers.pair_to_order_book_symbol(pair), limit=1)
        return Decimal(order_book["bids"][0][0]), Decimal(order_book["asks"][0][0])

    @property
    def spot_account(self) -> spot.Account:
        return spot.Account(self._cli.spot_account)

    @property
    def cross_margin_account(self) -> cross_margin.Account:
        return cross_margin.Account(self._cli.cross_margin_account)

    @property
    def isolated_margin_account(self) -> isolated_margin.Account:
        return isolated_margin.Account(self._cli.isolated_margin_account)

    def _subscribe_to_ws_channel_events(
            self, channel: str, event_src_factory: Callable[[core_ws.WebSocketClient], core_ws.ChannelEventSource],
            event_handler: dispatcher.EventHandler
    ):
        # Get/create the event source for the channel.
        ws_cli = self._get_ws_client()
        event_source = ws_cli.get_channel_event_source(channel)
        if not event_source:
            event_source = event_src_factory(ws_cli)
            ws_cli.set_channel_event_source(channel, event_source)

        # Subscribe the event handler to the event source.
        self._dispatcher.subscribe(event_source, event_handler)

    def _get_ws_client(self) -> binance_ws.WebSocketClient:
        if self._websocket is None:
            self._websocket = binance_ws.WebSocketClient(session=self._session, config_overrides=self._config_overrides)
        return self._websocket


def get_filter_from_symbol_info(symbol_info: dict, filter_type: str) -> Optional[dict]:
    filters = symbol_info["filters"]
    price_filters = [filter for filter in filters if filter["filterType"] == filter_type]
    return None if not price_filters else price_filters[0]


def get_precision_from_step_size(step_size: str) -> int:
    return int(-Decimal(step_size).log10() / Decimal(10).log10())
