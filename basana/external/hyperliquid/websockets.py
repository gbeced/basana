# Basana - Hyperliquid connector
#
# Licensed under the Apache License, Version 2.0
#
# WebSocket manager for real-time Hyperliquid feeds.
# Hyperliquid WS endpoint: wss://api.hyperliquid.xyz/ws
#
# Supported subscription types:
#   - candle       (OHLCV bars per coin/interval)
#   - trades       (individual trades per coin)
#   - l2Book       (order book updates per coin)
#   - orderUpdates (order status updates for a user address)
#   - userFills    (fill events for a user address)

from typing import Any, Callable, Dict, Optional
import asyncio
import json
import logging

import aiohttp

from basana.core import dispatcher

logger = logging.getLogger(__name__)

WS_URL = "wss://api.hyperliquid.xyz/ws"
TESTNET_WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"

MessageHandler = Callable[[dict], Any]


class WebsocketManager:
    """Manages a single persistent WebSocket connection to Hyperliquid.

    Subscriptions are registered before the connection is established.
    The manager auto-reconnects on disconnect.
    """

    def __init__(
        self,
        event_dispatcher: dispatcher.EventDispatcher,
        testnet: bool = False,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self._dispatcher = event_dispatcher
        self._ws_url = TESTNET_WS_URL if testnet else WS_URL
        self._session = session
        self._own_session = session is None
        self._subscriptions: list[dict] = []
        self._handlers: Dict[str, list[MessageHandler]] = {}
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def subscribe_to_candle_events(self, coin: str, interval: str, handler: MessageHandler) -> None:
        """Subscribe to OHLCV candle events.

        :param coin: e.g. "ETH"
        :param interval: e.g. "1m", "5m", "1h", "4h", "1d"
        :param handler: Async callable receiving the raw candle dict.
        """
        sub = {"type": "candle", "coin": coin, "interval": interval}
        key = f"candle:{coin}:{interval}"
        self._add_subscription(sub, key, handler)

    def subscribe_to_trade_events(self, coin: str, handler: MessageHandler) -> None:
        """Subscribe to real-time trade events.

        :param coin: e.g. "ETH"
        :param handler: Async callable receiving a list of trade dicts.
        """
        sub = {"type": "trades", "coin": coin}
        key = f"trades:{coin}"
        self._add_subscription(sub, key, handler)

    def subscribe_to_order_book_events(self, coin: str, handler: MessageHandler) -> None:
        """Subscribe to L2 order book updates.

        :param coin: e.g. "ETH"
        :param handler: Async callable receiving the order book dict.
        """
        sub = {"type": "l2Book", "coin": coin}
        key = f"l2Book:{coin}"
        self._add_subscription(sub, key, handler)

    def subscribe_to_order_updates(self, address: str, handler: MessageHandler) -> None:
        """Subscribe to order status updates for a wallet address.

        :param address: EVM wallet address (0x...).
        :param handler: Async callable receiving order update dicts.
        """
        sub = {"type": "orderUpdates", "user": address}
        key = f"orderUpdates:{address}"
        self._add_subscription(sub, key, handler)

    def subscribe_to_user_fills(self, address: str, handler: MessageHandler) -> None:
        """Subscribe to fill events for a wallet address.

        :param address: EVM wallet address (0x...).
        :param handler: Async callable receiving fill event dicts.
        """
        sub = {"type": "userFills", "user": address}
        key = f"userFills:{address}"
        self._add_subscription(sub, key, handler)

    def start(self) -> None:
        """Start the WebSocket connection loop (called by the event dispatcher)."""
        if not self._running and self._subscriptions:
            self._running = True
            self._task = asyncio.ensure_future(self._run())

    async def stop(self) -> None:
        """Stop the WebSocket connection."""
        self._running = False
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._task:
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._own_session and self._session:
            await self._session.close()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _add_subscription(self, sub: dict, key: str, handler: MessageHandler) -> None:
        # Only add the WS subscription once per key (multiple handlers allowed).
        if key not in self._handlers:
            self._subscriptions.append(sub)
            self._handlers[key] = []
        self._handlers[key].append(handler)

    async def _run(self) -> None:
        while self._running:
            try:
                if self._own_session or self._session is None:
                    self._session = aiohttp.ClientSession()
                async with self._session.ws_connect(self._ws_url, heartbeat=30) as ws:
                    self._ws = ws
                    # Send all subscriptions on connect/reconnect.
                    for sub in self._subscriptions:
                        await ws.send_json({"method": "subscribe", "subscription": sub})
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_message(json.loads(msg.data))
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except Exception as e:
                logger.warning("Hyperliquid WS error: %s — reconnecting in 5s", e)
                await asyncio.sleep(5)

    async def _handle_message(self, message: dict) -> None:
        channel = message.get("channel", "")
        data = message.get("data", {})

        # Route to registered handlers by channel key.
        for key, handlers in self._handlers.items():
            sub_type = key.split(":")[0]
            if channel == sub_type or channel.startswith(sub_type):
                for handler in handlers:
                    try:
                        if asyncio.iscoroutinefunction(handler):
                            await handler(data)
                        else:
                            handler(data)
                    except Exception as e:
                        logger.error("Handler error for %s: %s", key, e)
