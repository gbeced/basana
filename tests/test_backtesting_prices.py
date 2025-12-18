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

from decimal import Decimal
import datetime

import pytest

from basana.backtesting import config, errors, prices
from basana.core import dt
from basana.core.bar import Bar, BarEvent
from basana.core.pair import Pair, PairInfo


def test_no_prices():
    conf = config.Config()
    p = prices.Prices(bid_ask_spread_pct=Decimal("0.1"), config=conf)

    with pytest.raises(errors.NotFound):
        p.get_bid_ask(Pair("BTC", "USDT"))
    with pytest.raises(errors.NotFound):
        p.get_last_price(Pair("BTC", "USDT"))
    with pytest.raises(errors.NotFound):
        p.convert(Decimal(1), "BTC", "USDT")


def test_prices():
    pair = Pair("BTC", "USDT")
    conf = config.Config()
    conf.set_pair_info(pair, PairInfo(8, 2))
    p = prices.Prices(bid_ask_spread_pct=Decimal("1"), config=conf)

    now = dt.local_now()
    p.on_bar_event(
        BarEvent(
            now,
            Bar(
                now, pair, Decimal(10), Decimal(10), Decimal(10), Decimal(10), Decimal(10),
                datetime.timedelta(seconds=1)
            )
        )
    )

    assert p.get_bid_ask(pair) == (Decimal("9.95"), Decimal("10.05"))
    assert p.get_last_price(pair) == Decimal(10)
    assert p.convert(Decimal(100), pair.base_symbol, pair.quote_symbol) == Decimal(1000)
    assert p.convert(Decimal(1000), pair.quote_symbol, pair.base_symbol) == Decimal(100)


def test_convert_value_map():
    # Setup test pairs and prices
    btc_usdt = Pair("BTC", "USDT")
    eth_usdt = Pair("ETH", "USDT")
    eth_btc = Pair("ETH", "BTC")

    conf = config.Config()
    conf.set_pair_info(btc_usdt, PairInfo(8, 2))
    conf.set_pair_info(eth_usdt, PairInfo(8, 2))
    conf.set_pair_info(eth_btc, PairInfo(8, 8))

    p = prices.Prices(bid_ask_spread_pct=Decimal("1"), config=conf)

    # Set up price data
    now = dt.local_now()
    p.on_bar_event(
        BarEvent(
            now,
            Bar(
                now, btc_usdt, Decimal(50000), Decimal(50000), Decimal(50000), Decimal(50000), Decimal(50000),
                datetime.timedelta(seconds=1)
            )
        )
    )
    p.on_bar_event(
        BarEvent(
            now,
            Bar(
                now, eth_usdt, Decimal(3000), Decimal(3000), Decimal(3000), Decimal(3000), Decimal(3000),
                datetime.timedelta(seconds=1)
            )
        )
    )
    p.on_bar_event(
        BarEvent(
            now,
            Bar(
                now, eth_btc, Decimal("0.06"), Decimal("0.06"), Decimal("0.06"), Decimal("0.06"), Decimal("0.06"),
                datetime.timedelta(seconds=1)
            )
        )
    )

    # Test converting a value map to USDT
    values = {
        "BTC": Decimal("2.0"),      # 2 BTC
        "ETH": Decimal("10.0"),     # 10 ETH
        "USDT": Decimal("1000.0"),  # 1000 USDT
    }

    result = p.convert_value_map(values, "USDT")

    # Expected results:
    # BTC: 2.0 * 50000 = 100000 USDT
    # ETH: 10.0 * 3000 = 30000 USDT
    # USDT: 1000.0 (no conversion needed)
    expected = {
        "BTC": Decimal("100000.0"),
        "ETH": Decimal("30000.0"),
        "USDT": Decimal("1000.0"),
    }

    assert result == expected

    # Test converting to BTC
    result_btc = p.convert_value_map(values, "BTC")

    # Expected results:
    # BTC: 2.0 (no conversion needed)
    # ETH: 10.0 * 0.06 = 0.6 BTC
    # USDT: 1000.0 / 50000 = 0.02 BTC
    expected_btc = {
        "BTC": Decimal("2.0"),
        "ETH": Decimal("0.6"),
        "USDT": Decimal("0.02"),
    }

    assert result_btc == expected_btc

    # Test with empty value map
    empty_values = {}
    result_empty = p.convert_value_map(empty_values, "USDT")
    assert result_empty == {}

    # Test with single symbol (no conversion needed)
    single_values = {"USDT": Decimal("500.0")}
    result_single = p.convert_value_map(single_values, "USDT")
    assert result_single == {"USDT": Decimal("500.0")}
