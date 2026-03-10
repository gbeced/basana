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

from typing import Any, Awaitable, Callable, Dict, List, Optional
import datetime
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


class RawEvent(event.Event):
    """A raw Hyperliquid websocket payload wrapped as a Basana event."""

    def __init__(self, message: dict):
        super().__init__(datetime.datetime.now(datetime.timezone.utc))
        self.message = message


class RawEventSource(core_ws.ChannelEventSource):
    """Passes raw WebSocket message dicts through as events.

    Used as the base for channel-specific event sources (candles, trades, L2 book).
    """

    def __init__(self, producer: event.Producer):
        super().__init__(producer=producer)

    async def push_from_message(self, message: dict):
        self.push(RawEvent(message))


class WebSocketClient(core_ws.WebSocketClient):
    """Hyperliquid WebSocket client.

    Extends Basana's ``core_ws.WebSocketClient`` to provide channel-based
    subscriptions for candles, trades, L2 order book, order updates, and fills.
    """

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        config_overrides: Optional[dict] = None,
    ):
        if config_overrides is None:
            config_overrides = {}
        super().__init__(
            get_config_value(config.DEFAULTS, "api.websockets.base_url", overrides=config_overrides),
            session=session,
            config_overrides=config_overrides,
            heartbeat=get_config_value(config.DEFAULTS, "api.websockets.heartbeat", overrides=config_overrides),
        )
        self._registered_channels: Dict[str, core_ws.ChannelEventSource] = {}

    def set_channel_event_source(self, channel: str, event_source: core_ws.ChannelEventSource):
        self._registered_channels[channel] = event_source
        super().set_channel_event_source(channel, event_source)

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

        lookup_key = self._message_to_registered_channel(channel_name, data)
        if lookup_key is None:
            return False

        event_source = self._registered_channels.get(lookup_key)
        if event_source is None:
            return False

        await event_source.push_from_message(data)
        return True

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
    def _message_to_registered_channel(ws_channel: str, data: dict) -> Optional[str]:
        """Build the registered channel key from a WebSocket payload."""
        if ws_channel == "candle":
            coin = data.get("coin") or data.get("s", "")
            interval = data.get("interval") or data.get("i")
            if coin and interval:
                return _candle_channel(coin, interval)
            return None

        if ws_channel == "trades":
            coin = data.get("coin") or data.get("s", "")
            return _trades_channel(coin) if coin else None

        if ws_channel == "l2Book":
            coin = data.get("coin") or data.get("s", "")
            return _l2_book_channel(coin) if coin else None

        if ws_channel == "orderUpdates":
            user = data.get("user") or data.get("address")
            return _order_updates_channel(user) if user else None

        if ws_channel == "userFills":
            user = data.get("user") or data.get("address")
            return _user_fills_channel(user) if user else None

        return None
