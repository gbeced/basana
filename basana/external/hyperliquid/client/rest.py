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

from typing import Any, Dict, List, Optional
import asyncio
import functools
import logging

import eth_account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange as HLExchange

from basana.core.config import get_config_value
from basana.external.hyperliquid import config


logger = logging.getLogger(__name__)


class Error(Exception):
    def __init__(self, msg: str, response: Optional[dict] = None):
        super().__init__(msg)
        self.response = response


class APIClient:
    """Hyperliquid REST API client.

    Wraps the official ``hyperliquid-python-sdk``, running all blocking SDK calls
    in a thread pool executor so as not to block the async event loop.

    :param private_key: Optional EVM private key (hex string with 0x prefix).
        Required for trading. If omitted, only public endpoints are available.
    :param config_overrides: Optional dict for overriding config settings (base URL, timeout).
    """

    def __init__(
        self,
        private_key: Optional[str] = None,
        config_overrides: dict = {},
    ):
        base_url = get_config_value(config.DEFAULTS, "api.http.base_url", overrides=config_overrides).rstrip("/")
        self._info = Info(base_url, skip_ws=True)
        self._wallet: Optional[Any] = None
        self._exchange: Optional[HLExchange] = None

        if private_key:
            self._wallet = eth_account.Account.from_key(private_key)
            self._exchange = HLExchange(self._wallet, base_url)

    # ------------------------------------------------------------------
    # Market data (public)
    # ------------------------------------------------------------------

    async def get_all_mids(self) -> Dict[str, str]:
        """Return mid prices for all coins as ``{coin: price_str}``."""
        return await self._run(self._info.all_mids)

    async def get_l2_snapshot(self, coin: str) -> dict:
        """Return the current L2 order book snapshot for ``coin``."""
        return await self._run(self._info.l2_snapshot, coin)

    async def get_candles(self, coin: str, interval: str, start_time: int, end_time: int) -> List[dict]:
        """Return OHLCV candle data.

        :param coin: e.g. ``"ETH"``, ``"BTC"``
        :param interval: e.g. ``"1m"``, ``"1h"``, ``"4h"``, ``"1d"``
        :param start_time: Start timestamp in milliseconds.
        :param end_time: End timestamp in milliseconds.
        """
        return await self._run(self._info.candles_snapshot, coin, interval, start_time, end_time)

    async def get_meta(self) -> dict:
        """Return exchange metadata: tradeable assets, lot sizes, max leverage."""
        return await self._run(self._info.meta)

    async def get_funding_history(self, coin: str, start_time: int, end_time: Optional[int] = None) -> List[dict]:
        """Return funding rate history for ``coin``."""
        return await self._run(self._info.funding_history, coin, start_time, end_time)

    # ------------------------------------------------------------------
    # Account data (requires private key)
    # ------------------------------------------------------------------

    async def get_user_state(self) -> dict:
        """Return account state: balances, open positions, margin summary."""
        self._require_auth()
        return await self._run(self._info.user_state, self._wallet.address)

    async def get_open_orders(self) -> List[dict]:
        """Return all open orders for this account."""
        self._require_auth()
        return await self._run(self._info.open_orders, self._wallet.address)

    async def get_order_status(self, oid: int) -> dict:
        """Return the status of order ``oid``."""
        self._require_auth()
        return await self._run(self._info.query_order_by_oid, self._wallet.address, oid)

    async def get_user_fills(self, start_time: int) -> List[dict]:
        """Return trade fills since ``start_time`` (milliseconds)."""
        self._require_auth()
        return await self._run(self._info.user_fills_by_time, self._wallet.address, start_time)

    # ------------------------------------------------------------------
    # Trading (requires private key)
    # ------------------------------------------------------------------

    async def market_open(self, coin: str, is_buy: bool, sz: float, slippage: float = 0.01) -> dict:
        """Place a market order.

        :param coin: e.g. ``"ETH"``
        :param is_buy: ``True`` for long, ``False`` for short.
        :param sz: Size in base asset units.
        :param slippage: Maximum acceptable slippage (default 1 %).
        """
        self._require_auth()
        result = await self._run(self._exchange.market_open, coin, is_buy, sz, None, slippage)
        self._check_result(result)
        return result

    async def market_close(self, coin: str, sz: Optional[float] = None, slippage: float = 0.01) -> dict:
        """Close an open position at market price.

        :param coin: e.g. ``"ETH"``
        :param sz: Amount to close. ``None`` closes the entire position.
        :param slippage: Maximum acceptable slippage (default 1 %).
        """
        self._require_auth()
        result = await self._run(self._exchange.market_close, coin, sz, None, slippage)
        self._check_result(result)
        return result

    async def limit_order(
        self,
        coin: str,
        is_buy: bool,
        sz: float,
        limit_px: float,
        reduce_only: bool = False,
    ) -> dict:
        """Place a limit order.

        :param coin: e.g. ``"ETH"``
        :param is_buy: ``True`` to buy, ``False`` to sell.
        :param sz: Size in base asset units.
        :param limit_px: Limit price in USD.
        :param reduce_only: If ``True``, the order can only reduce an existing position.
        """
        self._require_auth()
        order_type = {"limit": {"tif": "Gtc"}}
        result = await self._run(self._exchange.order, coin, is_buy, sz, limit_px, order_type, reduce_only)
        self._check_result(result)
        return result

    async def cancel_order(self, coin: str, oid: int) -> dict:
        """Cancel order ``oid`` for ``coin``."""
        self._require_auth()
        result = await self._run(self._exchange.cancel, coin, oid)
        self._check_result(result)
        return result

    async def set_leverage(self, coin: str, leverage: int, is_cross: bool = True) -> dict:
        """Set leverage for ``coin``.

        :param coin: e.g. ``"ETH"``
        :param leverage: Leverage multiplier (1–50 depending on the asset).
        :param is_cross: ``True`` for cross margin, ``False`` for isolated.
        """
        self._require_auth()
        result = await self._run(self._exchange.update_leverage, leverage, coin, is_cross)
        self._check_result(result)
        return result

    @property
    def address(self) -> Optional[str]:
        """Wallet address, or ``None`` when not authenticated."""
        return self._wallet.address if self._wallet else None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_auth(self) -> None:
        if self._wallet is None:
            raise Error("Private key required for this operation")

    @staticmethod
    def _check_result(result: dict) -> None:
        if result.get("status") != "ok":
            raise Error(f"API error: {result}", response=result)

    @staticmethod
    async def _run(fn, *args) -> Any:
        """Run a blocking SDK call in the default thread pool executor."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, functools.partial(fn, *args))
