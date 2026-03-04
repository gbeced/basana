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

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

import basana as bs
from basana.external.hyperliquid.exchange import Exchange, Error, AssetInfo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_api_client():
    with patch("basana.external.hyperliquid.exchange.client.APIClient") as MockClient:
        instance = MockClient.return_value
        instance.get_all_mids = AsyncMock(return_value={
            "ETH": "2100.0", "BTC": "70000.0", "SOL": "150.0"
        })
        instance.get_l2_snapshot = AsyncMock(return_value={
            "coin": "ETH",
            "levels": [
                [{"px": "2099.5", "sz": "2.0", "n": 1}],
                [{"px": "2100.5", "sz": "1.5", "n": 1}],
            ],
        })
        instance.get_meta = AsyncMock(return_value={
            "universe": [
                {"name": "BTC", "szDecimals": 5, "maxLeverage": 50},
                {"name": "ETH", "szDecimals": 4, "maxLeverage": 25},
                {"name": "SOL", "szDecimals": 2, "maxLeverage": 20},
            ]
        })
        yield instance


@pytest.fixture()
def mock_ws_client():
    with patch("basana.external.hyperliquid.exchange.websockets.WebSocketClient") as MockWS:
        instance = MockWS.return_value
        instance.set_channel_event_source = MagicMock()
        yield instance


@pytest.fixture()
def exchange(mock_api_client, mock_ws_client):
    d = bs.realtime_dispatcher()
    return Exchange(dispatcher=d)


# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------

class TestMarketData:
    def test_get_mid_price(self, exchange):
        price = asyncio.run(exchange.get_mid_price("ETH"))
        assert price == Decimal("2100.0")
        assert isinstance(price, Decimal)

    def test_get_mid_price_unknown_coin_raises(self, exchange):
        with pytest.raises(Error, match="Unknown coin"):
            asyncio.run(exchange.get_mid_price("NOTACOIN"))

    def test_get_bid_ask(self, exchange):
        bid, ask = asyncio.run(exchange.get_bid_ask("ETH"))
        assert bid == Decimal("2099.5")
        assert ask == Decimal("2100.5")
        assert ask > bid

    def test_get_pair_info(self, exchange):
        info = asyncio.run(exchange.get_pair_info("ETH"))
        assert isinstance(info, AssetInfo)
        assert info.sz_decimals == 4
        assert info.max_leverage == 25

    def test_get_pair_info_btc(self, exchange):
        info = asyncio.run(exchange.get_pair_info("BTC"))
        assert info.sz_decimals == 5
        assert info.max_leverage == 50

    def test_get_pair_info_cached(self, exchange, mock_api_client):
        asyncio.run(exchange.get_pair_info("ETH"))
        asyncio.run(exchange.get_pair_info("ETH"))
        mock_api_client.get_meta.assert_called_once()

    def test_list_coins(self, exchange):
        coins = asyncio.run(exchange.list_coins())
        assert "BTC" in coins and "ETH" in coins and "SOL" in coins
        assert len(coins) == 3


# ---------------------------------------------------------------------------
# WebSocket subscriptions
# ---------------------------------------------------------------------------

class TestSubscriptions:
    def test_subscribe_to_bar_events(self, exchange, mock_ws_client):
        handler = AsyncMock()
        exchange.subscribe_to_bar_events("ETH", "1h", handler)
        mock_ws_client.set_channel_event_source.assert_called_once()
        args = mock_ws_client.set_channel_event_source.call_args[0]
        assert args[0] == "candle:ETH:1h"

    def test_subscribe_to_trade_events(self, exchange, mock_ws_client):
        handler = AsyncMock()
        exchange.subscribe_to_trade_events("ETH", handler)
        mock_ws_client.set_channel_event_source.assert_called_once()
        args = mock_ws_client.set_channel_event_source.call_args[0]
        assert args[0] == "trades:ETH"

    def test_subscribe_to_order_book_events(self, exchange, mock_ws_client):
        handler = AsyncMock()
        exchange.subscribe_to_order_book_events("ETH", handler)
        mock_ws_client.set_channel_event_source.assert_called_once()
        args = mock_ws_client.set_channel_event_source.call_args[0]
        assert args[0] == "l2Book:ETH"


# ---------------------------------------------------------------------------
# Bar event construction
# ---------------------------------------------------------------------------

class TestBarEventSource:
    def test_candle_to_bar_event(self):
        from basana.external.hyperliquid.exchange import BarEventSource
        from basana.core.pair import Pair

        pair = Pair("ETH", "USD")
        producer = MagicMock()
        producer.initialize = MagicMock()
        source = BarEventSource(pair=pair, producer=producer)

        events = []
        async def run():
            await source.push_from_message({
                "t": 1709500000000,
                "T": 1709503600000,
                "o": "2100.0",
                "h": "2150.0",
                "l": "2090.0",
                "c": "2130.0",
                "v": "500.5",
                "coin": "ETH",
            })
            while True:
                event = source.pop()
                if event is None:
                    break
                events.append(event)

        asyncio.run(run())
        assert len(events) == 1
        bar_event = events[0]
        assert bar_event.bar.close == Decimal("2130.0")
        assert bar_event.bar.pair == pair
        assert bar_event.when.tzinfo is not None

    def test_malformed_candle_does_not_raise(self):
        from basana.external.hyperliquid.exchange import BarEventSource
        from basana.core.pair import Pair

        pair = Pair("ETH", "USD")
        producer = MagicMock()
        source = BarEventSource(pair=pair, producer=producer)
        # Should log a warning but not raise
        asyncio.run(source.push_from_message({"invalid": "data"}))


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_perps_account_accessible(self, exchange):
        from basana.external.hyperliquid.perps import Account
        assert isinstance(exchange.perps_account, Account)
