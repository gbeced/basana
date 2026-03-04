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

from typing import Any, Awaitable, Callable, List, Optional
import json
import logging

import aiohttp

from basana.core import event, websockets as core_ws
from basana.core.config import get_config_value
from basana.external.hyperliquid import config


logger = logging.getLogger(__name__)

ChannelMessageHandler = Callable[[dict], Awaitable[Any]]


def _candle_channel(coin: str, interval: str) -> str:
    return f"candle:{coin}:{interval}"


def _trades_channel(coin: str) -> str:
    return f"trades:{coin}"


def _l2_book_channel(coin: str) -> str:
    return f"l2Book:{coin}"


def _order_updates_channel(address: str) -> str:
    return f"orderUpdates:{address}"


def _user_fills_channel(address: str) -> str:
    return f"userFills:{address}"


class RawEventSource(core_ws.ChannelEventSource):
    """Passes raw WebSocket message dicts through as events.

    Used as the base for channel-specific event sources (candles, trades, L2 book).
    """

    def __init__(self, producer: event.Producer):
        super().__init__(producer=producer)

    async def push_from_message(self, message: dict):
        self.push(message)


class WebSocketClient(core_ws.WebSocketClient):
    """Hyperliquid WebSocket client.

    Extends Basana's ``core_ws.WebSocketClient`` to provide channel-based
    subscriptions for candles, trades, L2 order book, order updates, and fills.
    """

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        config_overrides: dict = {},
    ):
        super().__init__(
            get_config_value(config.DEFAULTS, "api.websockets.base_url", overrides=config_overrides),
            session=session,
            config_overrides=config_overrides,
            heartbeat=get_config_value(config.DEFAULTS, "api.websockets.heartbeat", overrides=config_overrides),
        )

    async def subscribe_to_channels(self, channels: List[str], ws_cli: aiohttp.ClientWebSocketResponse):
        for channel in channels:
            subscription = self._channel_to_subscription(channel)
            if subscription:
                await ws_cli.send_json({"method": "subscribe", "subscription": subscription})
                logger.debug("Subscribed to %s", channel)

    async def handle_message(self, message: dict) -> bool:
        channel_name = message.get("channel", "")
        data = message.get("data", {})

        if not channel_name or channel_name == "subscriptionResponse":
            return True  # Ack messages — handled, nothing to dispatch

        # Route to the matching ChannelEventSource
        matched = False
        for registered_channel, event_source in self._event_sources.items():
            if self._matches(registered_channel, channel_name, data):
                await event_source.push_from_message(data)
                matched = True

        return matched

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _channel_to_subscription(channel: str) -> Optional[dict]:
        """Convert a registered channel key back to the HL subscription payload."""
        parts = channel.split(":")
        sub_type = parts[0]
        if sub_type == "candle" and len(parts) == 3:
            return {"type": "candle", "coin": parts[1], "interval": parts[2]}
        elif sub_type in ("trades", "l2Book") and len(parts) == 2:
            return {"type": sub_type, "coin": parts[1]}
        elif sub_type in ("orderUpdates", "userFills") and len(parts) == 2:
            return {"type": sub_type, "user": parts[1]}
        return None

    @staticmethod
    def _matches(registered_channel: str, ws_channel: str, data: dict) -> bool:
        """Check if a WebSocket message belongs to a registered channel."""
        parts = registered_channel.split(":")
        if parts[0] != ws_channel:
            return False

        # For coin-scoped subscriptions, also verify the coin in the payload.
        if ws_channel in ("candle", "trades", "l2Book") and len(parts) >= 2:
            payload_coin = data.get("coin") or data.get("s", "")
            return payload_coin == parts[1]

        # For user-scoped subscriptions, no extra check needed.
        return True
