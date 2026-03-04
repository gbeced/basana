# Basana - Hyperliquid connector tests
#
# Licensed under the Apache License, Version 2.0

from decimal import Decimal
from unittest.mock import MagicMock, patch
import pytest

import basana as b
from basana.external.hyperliquid.exchange import Exchange, Error, AssetInfo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_api_client():
    with patch("basana.external.hyperliquid.exchange.client.APIClient") as MockClient:
        instance = MockClient.return_value
        instance.get_all_mids.return_value = {"ETH": "2100.0", "BTC": "70000.0", "SOL": "150.0"}
        instance.get_l2_snapshot.return_value = {
            "coin": "ETH",
            "levels": [
                [{"px": "2099.5", "sz": "2.0", "n": 1}],
                [{"px": "2100.5", "sz": "1.5", "n": 1}],
            ],
        }
        instance.get_meta.return_value = {
            "universe": [
                {"name": "BTC", "szDecimals": 5, "maxLeverage": 50},
                {"name": "ETH", "szDecimals": 4, "maxLeverage": 25},
                {"name": "SOL", "szDecimals": 2, "maxLeverage": 20},
            ]
        }
        yield instance


@pytest.fixture()
def mock_ws_manager():
    with patch("basana.external.hyperliquid.exchange.websockets.WebsocketManager") as MockWS:
        yield MockWS.return_value


@pytest.fixture()
def exchange(mock_api_client, mock_ws_manager):
    d = b.RealtimeDispatcher(max_concurrent=10)
    return Exchange(dispatcher=d)


# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------

class TestMarketData:
    def test_get_mid_price(self, exchange):
        price = exchange.get_mid_price("ETH")
        assert price == Decimal("2100.0")
        assert isinstance(price, Decimal)

    def test_get_mid_price_unknown_coin_raises(self, exchange):
        with pytest.raises(Error, match="Unknown coin"):
            exchange.get_mid_price("NOTACOIN")

    def test_get_bid_ask(self, exchange):
        bid, ask = exchange.get_bid_ask("ETH")
        assert bid == Decimal("2099.5")
        assert ask == Decimal("2100.5")
        assert ask > bid

    def test_get_pair_info(self, exchange):
        info = exchange.get_pair_info("ETH")
        assert isinstance(info, AssetInfo)
        assert info.sz_decimals == 4
        assert info.max_leverage == 25

    def test_get_pair_info_btc(self, exchange):
        info = exchange.get_pair_info("BTC")
        assert info.sz_decimals == 5
        assert info.max_leverage == 50

    def test_get_pair_info_cached(self, exchange, mock_api_client):
        exchange.get_pair_info("ETH")
        exchange.get_pair_info("ETH")
        # Meta should only be fetched once (cached)
        mock_api_client.get_meta.assert_called_once()

    def test_list_coins(self, exchange):
        coins = exchange.list_coins()
        assert "BTC" in coins
        assert "ETH" in coins
        assert "SOL" in coins
        assert len(coins) == 3


# ---------------------------------------------------------------------------
# WebSocket subscriptions
# ---------------------------------------------------------------------------

class TestSubscriptions:
    def test_subscribe_to_bar_events(self, exchange, mock_ws_manager):
        handler = MagicMock()
        exchange.subscribe_to_bar_events("ETH", "1h", handler)
        mock_ws_manager.subscribe_to_candle_events.assert_called_once()
        args = mock_ws_manager.subscribe_to_candle_events.call_args[0]
        # Coin and interval are passed through correctly.
        assert args[0] == "ETH"
        assert args[1] == "1h"
        # The third arg is an internal async wrapper (_on_candle), not the raw handler.
        import asyncio
        assert asyncio.iscoroutinefunction(args[2])

    def test_subscribe_to_trade_events(self, exchange, mock_ws_manager):
        handler = MagicMock()
        exchange.subscribe_to_trade_events("ETH", handler)
        mock_ws_manager.subscribe_to_trade_events.assert_called_once_with("ETH", handler)

    def test_subscribe_to_order_book_events(self, exchange, mock_ws_manager):
        handler = MagicMock()
        exchange.subscribe_to_order_book_events("ETH", handler)
        mock_ws_manager.subscribe_to_order_book_events.assert_called_once_with("ETH", handler)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_start_delegates_to_ws_manager(self, exchange, mock_ws_manager):
        exchange.start()
        mock_ws_manager.start.assert_called_once()

    def test_perps_account_accessible(self, exchange):
        from basana.external.hyperliquid.perps import Account
        assert isinstance(exchange.perps_account, Account)
