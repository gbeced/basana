# Basana - Hyperliquid connector
#
# Licensed under the Apache License, Version 2.0
#
# REST client wrapping the Hyperliquid Python SDK.
# Hyperliquid uses two endpoints:
#   POST https://api.hyperliquid.xyz/info     - market data and account info
#   POST https://api.hyperliquid.xyz/exchange - order placement and management

from typing import Any, Dict, List, Optional, Tuple
import eth_account
import logging

from hyperliquid.info import Info
from hyperliquid.exchange import Exchange as HLExchange
from hyperliquid.utils import constants

logger = logging.getLogger(__name__)


class Error(Exception):
    """Raised when the Hyperliquid API returns an error."""

    def __init__(self, message: str, response: Optional[dict] = None):
        super().__init__(message)
        self.response = response


class APIClient:
    """Low-level Hyperliquid REST client.

    :param private_key: Optional EVM private key (hex string with 0x prefix).
        Required for trading. If not set, only public/read endpoints are available.
    :param testnet: If True, connects to the Hyperliquid testnet.
    """

    def __init__(
        self,
        private_key: Optional[str] = None,
        testnet: bool = False,
    ):
        self._base_url = constants.TESTNET_API_URL if testnet else constants.MAINNET_API_URL
        self._info = Info(self._base_url, skip_ws=True)
        self._wallet: Optional[Any] = None
        self._exchange: Optional[HLExchange] = None

        if private_key:
            self._wallet = eth_account.Account.from_key(private_key)
            self._exchange = HLExchange(self._wallet, self._base_url)

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------

    def get_all_mids(self) -> Dict[str, str]:
        """Return mid prices for all coins. Returns {coin: mid_price_str}."""
        return self._info.all_mids()

    def get_l2_snapshot(self, coin: str) -> dict:
        """Return the current L2 order book for a coin."""
        return self._info.l2_snapshot(coin)

    def get_candles(self, coin: str, interval: str, start_time: int, end_time: int) -> List[dict]:
        """Return OHLCV candles.

        :param coin: e.g. "ETH", "BTC"
        :param interval: e.g. "1m", "5m", "1h", "4h", "1d"
        :param start_time: Start timestamp in milliseconds.
        :param end_time: End timestamp in milliseconds.
        """
        return self._info.candles_snapshot(coin, interval, start_time, end_time)

    def get_meta(self) -> dict:
        """Return exchange metadata: all tradeable assets, lot sizes, etc."""
        return self._info.meta()

    def get_funding_history(self, coin: str, start_time: int, end_time: Optional[int] = None) -> List[dict]:
        """Return funding rate history for a coin."""
        return self._info.funding_history(coin, start_time, end_time)

    # ------------------------------------------------------------------
    # Account data (requires wallet)
    # ------------------------------------------------------------------

    def _require_auth(self) -> None:
        if self._wallet is None:
            raise Error("Private key required for this operation")

    def get_user_state(self) -> dict:
        """Return account state: balances, positions, margin info."""
        self._require_auth()
        return self._info.user_state(self._wallet.address)

    def get_open_orders(self) -> List[dict]:
        """Return all open orders for this account."""
        self._require_auth()
        return self._info.open_orders(self._wallet.address)

    def get_order_status(self, oid: int) -> dict:
        """Return the status of a specific order by ID."""
        self._require_auth()
        return self._info.query_order_by_oid(self._wallet.address, oid)

    def get_user_fills(self, start_time: int) -> List[dict]:
        """Return trade history (fills) since start_time (ms)."""
        self._require_auth()
        return self._info.user_fills_by_time(self._wallet.address, start_time)

    # ------------------------------------------------------------------
    # Trading (requires wallet)
    # ------------------------------------------------------------------

    def market_open(self, coin: str, is_buy: bool, sz: float, slippage: float = 0.01) -> dict:
        """Place a market order.

        :param coin: e.g. "ETH"
        :param is_buy: True for long, False for short.
        :param sz: Size in base asset units.
        :param slippage: Max acceptable slippage (default 1%).
        """
        self._require_auth()
        result = self._exchange.market_open(coin, is_buy, sz, None, slippage)
        self._check_result(result)
        return result

    def market_close(self, coin: str, sz: Optional[float] = None, slippage: float = 0.01) -> dict:
        """Close an open position at market price.

        :param coin: e.g. "ETH"
        :param sz: Size to close. If None, closes the entire position.
        :param slippage: Max acceptable slippage (default 1%).
        """
        self._require_auth()
        result = self._exchange.market_close(coin, sz, None, slippage)
        self._check_result(result)
        return result

    def limit_order(
        self,
        coin: str,
        is_buy: bool,
        sz: float,
        limit_px: float,
        reduce_only: bool = False,
    ) -> dict:
        """Place a limit order.

        :param coin: e.g. "ETH"
        :param is_buy: True to buy, False to sell.
        :param sz: Size in base asset units.
        :param limit_px: Limit price.
        :param reduce_only: If True, the order can only reduce an existing position.
        """
        self._require_auth()
        order_type = {"limit": {"tif": "Gtc"}}
        result = self._exchange.order(coin, is_buy, sz, limit_px, order_type, reduce_only=reduce_only)
        self._check_result(result)
        return result

    def cancel_order(self, coin: str, oid: int) -> dict:
        """Cancel an open order by ID."""
        self._require_auth()
        result = self._exchange.cancel(coin, oid)
        self._check_result(result)
        return result

    def cancel_all_orders(self, coin: Optional[str] = None) -> None:
        """Cancel all open orders, optionally filtered by coin."""
        self._require_auth()
        orders = self.get_open_orders()
        if coin:
            orders = [o for o in orders if o.get("coin") == coin]
        for order in orders:
            try:
                self.cancel_order(order["coin"], order["oid"])
            except Error as e:
                logger.warning("Failed to cancel order %s: %s", order["oid"], e)

    def set_leverage(self, coin: str, leverage: int, is_cross: bool = True) -> dict:
        """Set leverage for a coin.

        :param coin: e.g. "ETH"
        :param leverage: Leverage multiplier (1-50 depending on asset).
        :param is_cross: True for cross margin, False for isolated.
        """
        self._require_auth()
        result = self._exchange.update_leverage(leverage, coin, is_cross)
        self._check_result(result)
        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def address(self) -> Optional[str]:
        """Return the wallet address, or None if not authenticated."""
        return self._wallet.address if self._wallet else None

    @staticmethod
    def _check_result(result: dict) -> None:
        status = result.get("status")
        if status != "ok":
            raise Error(f"API error: {result}", response=result)
