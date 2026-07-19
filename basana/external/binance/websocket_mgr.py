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

from typing import cast, Callable, Optional

import aiohttp

from . import client, order_book, order_book_diff, trades, user_data, websockets as binance_ws, klines
from basana.core import bar, dispatcher, websockets as core_ws
from basana.core.pair import Pair


class WebsocketManager:
    def __init__(
            self, dispatcher: dispatcher.EventDispatcher, api_client: client.APIClient,
            session: Optional[aiohttp.ClientSession] = None, config_overrides: dict = {}
    ):
        self._dispatcher = dispatcher
        self._cli = api_client
        self._session = session
        self._config_overrides = config_overrides
        self._stream_websocket: Optional[binance_ws.WebSocketStreamClient] = None
        self._api_websocket: Optional[binance_ws.WebSocketAPIClient] = None

    def subscribe_to_bar_events(self, pair: Pair, interval: str, event_handler: bar.BarEventHandler):
        self._subscribe_to_ws_stream_events(
            binance_ws.PublicStreamChannel(klines.get_channel(pair, interval)),
            lambda ws_cli: klines.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_order_book_events(
            self, pair: Pair, event_handler: order_book.PartialOrderBookEventHandler, depth: int = 10,
            interval: int = 1000
    ):
        self._subscribe_to_ws_stream_events(
            binance_ws.PublicStreamChannel(order_book.get_channel(pair, depth, interval)),
            lambda ws_cli: order_book.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_order_book_diff_events(
            self, pair: Pair, event_handler: order_book_diff.OrderBookDiffEventHandler, interval: int = 1000
    ):
        self._subscribe_to_ws_stream_events(
            binance_ws.PublicStreamChannel(order_book_diff.get_channel(pair, interval)),
            lambda ws_cli: order_book_diff.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_trade_events(self, pair: Pair, event_handler: trades.TradeEventHandler):
        self._subscribe_to_ws_stream_events(
            binance_ws.PublicStreamChannel(trades.get_channel(pair)),
            lambda ws_cli: trades.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_user_data_events(
        self, channel: binance_ws.Channel,
        event_src_factory: Callable[[core_ws.WebSocketClient], core_ws.ChannelEventSource],
        event_handler: user_data.UserDataEventHandler,
    ):
        self._subscribe_based_on_channel(channel, event_src_factory, cast(dispatcher.EventHandler, event_handler))

    def subscribe_to_order_events(
        self, channel: binance_ws.Channel,
        event_src_factory: Callable[[core_ws.WebSocketClient], core_ws.ChannelEventSource],
        event_handler: user_data.OrderEventHandler
    ):
        async def forward_if_order_event(event: user_data.Event):
            if isinstance(event, user_data.OrderEvent):
                await event_handler(event)

        self._subscribe_based_on_channel(
            channel, event_src_factory, cast(dispatcher.EventHandler, forward_if_order_event)
        )

    def _subscribe_based_on_channel(
        self, channel: binance_ws.Channel,
        event_src_factory: Callable[[core_ws.WebSocketClient], core_ws.ChannelEventSource],
        event_handler: dispatcher.EventHandler
    ):
        if channel.websocket_protocol == "ws_stream":
            self._subscribe_to_ws_stream_events(
                cast(binance_ws.WSStreamChannel, channel), event_src_factory, event_handler
            )
        else:
            assert channel.websocket_protocol == "ws_api", \
                f"Unsupported websocket protocol {channel.websocket_protocol}"
            self._subscribe_to_ws_api_events(
                cast(binance_ws.WSAPIChannel, channel), event_src_factory, event_handler
            )

    def _subscribe_to_ws_stream_events(
            self, channel: binance_ws.WSStreamChannel,
            event_src_factory: Callable[[core_ws.WebSocketClient], core_ws.ChannelEventSource],
            event_handler: dispatcher.EventHandler
    ):
        # Get/create the client.
        if self._stream_websocket is None:
            self._stream_websocket = binance_ws.WebSocketStreamClient(
                self._dispatcher, self._cli, session=self._session, config_overrides=self._config_overrides
            )
        ws_cli = self._stream_websocket

        # Get/create the event source.
        event_source = ws_cli.get_channel_event_source_ex(channel)
        if not event_source:
            event_source = event_src_factory(ws_cli)
            ws_cli.set_channel_event_source_ex(channel, event_source)

        # Subscribe the event handler to the event source.
        self._dispatcher.subscribe(event_source, event_handler)

    def _subscribe_to_ws_api_events(
            self, channel: binance_ws.WSAPIChannel,
            event_src_factory: Callable[[core_ws.WebSocketClient], core_ws.ChannelEventSource],
            event_handler: dispatcher.EventHandler
    ):
        # Get/create the client.
        if self._api_websocket is None:
            self._api_websocket = binance_ws.WebSocketAPIClient(
                self._dispatcher, self._cli, session=self._session, config_overrides=self._config_overrides
            )
        ws_cli = self._api_websocket

        # Get/create the event source.
        event_source = ws_cli.get_channel_event_source_ex(channel)
        if not event_source:
            event_source = event_src_factory(ws_cli)
            ws_cli.set_channel_event_source_ex(channel, event_source)
 
        # Subscribe the event handler to the event source.
        self._dispatcher.subscribe(event_source, event_handler)
