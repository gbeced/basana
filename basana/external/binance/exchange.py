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

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

from decimal import Decimal
from typing import cast, Any, Awaitable, Callable, Dict, List, Optional, Tuple
import dataclasses

import aiohttp

from . import client, helpers, order_book, trades, websockets as binance_ws, spot, cross_margin, isolated_margin
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


class RealTimeTradesToBar(bar.RealTimeTradesToBar):
    async def on_trade_event(self, trade_event: trades.TradeEvent):
        self.push_trade(trade_event.trade.datetime, trade_event.trade.price, trade_event.trade.amount)


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
        self._bar_event_source: Dict[Tuple[Pair, int, bool, float], RealTimeTradesToBar] = {}

    def subscribe_to_bar_events(
            self, pair: Pair, bar_duration: int, event_handler: BarEventHandler, skip_first_bar: bool = True,
            flush_delay: float = 1
    ):
        key = (pair, bar_duration, skip_first_bar, flush_delay)
        event_source = self._bar_event_source.get(key)
        if not event_source:
            event_source = RealTimeTradesToBar(
                pair, bar_duration, skip_first_bar=skip_first_bar, flush_delay=flush_delay
            )
            self.subscribe_to_trade_events(pair, event_source.on_trade_event)
            self._bar_event_source[key] = event_source
        self._dispatcher.subscribe(event_source, cast(dispatcher.EventHandler, event_handler))

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
