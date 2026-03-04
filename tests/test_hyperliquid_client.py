# Basana - Hyperliquid connector tests
#
# Licensed under the Apache License, Version 2.0

from decimal import Decimal
from unittest.mock import MagicMock, patch
import pytest

from basana.external.hyperliquid.client.rest import APIClient, Error


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_info():
    """Patch hyperliquid.info.Info with a MagicMock."""
    with patch("basana.external.hyperliquid.client.rest.Info") as MockInfo:
        yield MockInfo.return_value


@pytest.fixture()
def mock_exchange_sdk():
    """Patch hyperliquid.exchange.Exchange with a MagicMock."""
    with patch("basana.external.hyperliquid.client.rest.HLExchange") as MockEx:
        yield MockEx.return_value


@pytest.fixture()
def mock_account():
    """Patch eth_account so no real key is required."""
    with patch("basana.external.hyperliquid.client.rest.eth_account") as mock_eth:
        mock_wallet = MagicMock()
        mock_wallet.address = "0xDEADBEEF"
        mock_eth.Account.from_key.return_value = mock_wallet
        yield mock_eth


# ---------------------------------------------------------------------------
# Public market data
# ---------------------------------------------------------------------------

class TestPublicEndpoints:
    def test_get_all_mids(self, mock_info):
        mock_info.all_mids.return_value = {"ETH": "2100.0", "BTC": "70000.0"}
        cli = APIClient()
        mids = cli.get_all_mids()
        assert mids["ETH"] == "2100.0"
        assert mids["BTC"] == "70000.0"
        mock_info.all_mids.assert_called_once()

    def test_get_l2_snapshot(self, mock_info):
        mock_info.l2_snapshot.return_value = {
            "coin": "ETH",
            "levels": [
                [{"px": "2099.9", "sz": "1.0", "n": 1}],
                [{"px": "2100.1", "sz": "1.0", "n": 1}],
            ],
        }
        cli = APIClient()
        book = cli.get_l2_snapshot("ETH")
        assert book["levels"][0][0]["px"] == "2099.9"
        mock_info.l2_snapshot.assert_called_once_with("ETH")

    def test_get_meta(self, mock_info):
        mock_info.meta.return_value = {
            "universe": [
                {"name": "BTC", "szDecimals": 5, "maxLeverage": 50},
                {"name": "ETH", "szDecimals": 4, "maxLeverage": 25},
            ]
        }
        cli = APIClient()
        meta = cli.get_meta()
        coins = [a["name"] for a in meta["universe"]]
        assert "BTC" in coins and "ETH" in coins

    def test_get_candles(self, mock_info):
        mock_info.candles_snapshot.return_value = [
            {"t": 1700000000000, "o": "2000", "h": "2100", "l": "1990", "c": "2050", "v": "1000"},
        ]
        cli = APIClient()
        candles = cli.get_candles("ETH", "1h", 1700000000000, 1700003600000)
        assert len(candles) == 1
        assert candles[0]["c"] == "2050"


# ---------------------------------------------------------------------------
# Auth guard
# ---------------------------------------------------------------------------

class TestAuthGuard:
    def test_get_user_state_without_key_raises(self, mock_info):
        cli = APIClient()  # no private key
        with pytest.raises(Error, match="Private key required"):
            cli.get_user_state()

    def test_get_open_orders_without_key_raises(self, mock_info):
        cli = APIClient()
        with pytest.raises(Error, match="Private key required"):
            cli.get_open_orders()

    def test_market_open_without_key_raises(self, mock_info):
        cli = APIClient()
        with pytest.raises(Error, match="Private key required"):
            cli.market_open("ETH", True, 0.1)


# ---------------------------------------------------------------------------
# Authenticated endpoints (mocked key)
# ---------------------------------------------------------------------------

class TestAuthenticatedEndpoints:
    def test_get_user_state(self, mock_info, mock_account, mock_exchange_sdk):
        mock_info.user_state.return_value = {
            "marginSummary": {"accountValue": "5000.0"},
            "assetPositions": [],
        }
        cli = APIClient(private_key="0xdeadbeef")
        state = cli.get_user_state()
        assert state["marginSummary"]["accountValue"] == "5000.0"
        mock_info.user_state.assert_called_once_with("0xDEADBEEF")

    def test_market_open_success(self, mock_info, mock_account, mock_exchange_sdk):
        mock_exchange_sdk.market_open.return_value = {"status": "ok", "response": {"data": {"statuses": [{}]}}}
        cli = APIClient(private_key="0xdeadbeef")
        result = cli.market_open("ETH", True, 0.1)
        assert result["status"] == "ok"

    def test_market_open_api_error_raises(self, mock_info, mock_account, mock_exchange_sdk):
        mock_exchange_sdk.market_open.return_value = {"status": "err", "response": "Insufficient margin"}
        cli = APIClient(private_key="0xdeadbeef")
        with pytest.raises(Error, match="API error"):
            cli.market_open("ETH", True, 0.1)

    def test_cancel_order(self, mock_info, mock_account, mock_exchange_sdk):
        mock_exchange_sdk.cancel.return_value = {"status": "ok"}
        cli = APIClient(private_key="0xdeadbeef")
        result = cli.cancel_order("ETH", 12345)
        assert result["status"] == "ok"
        mock_exchange_sdk.cancel.assert_called_once_with("ETH", 12345)

    def test_set_leverage(self, mock_info, mock_account, mock_exchange_sdk):
        mock_exchange_sdk.update_leverage.return_value = {"status": "ok"}
        cli = APIClient(private_key="0xdeadbeef")
        result = cli.set_leverage("ETH", 10, is_cross=True)
        assert result["status"] == "ok"
        mock_exchange_sdk.update_leverage.assert_called_once_with(10, "ETH", True)

    def test_address_exposed(self, mock_info, mock_account, mock_exchange_sdk):
        cli = APIClient(private_key="0xdeadbeef")
        assert cli.address == "0xDEADBEEF"

    def test_address_none_without_key(self, mock_info):
        cli = APIClient()
        assert cli.address is None
