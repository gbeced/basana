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

import pytest

from basana.backtesting import config, errors, prices
from basana.core import dt
from basana.core.bar import Bar, BarEvent
from basana.core.pair import Pair, PairInfo


def test_no_prices():
    conf = config.Config()
    p = prices.Prices(bid_ask_spread_pct=Decimal("0.1"), config=conf)

    with pytest.raises(errors.NoPrice):
        p.get_bid_ask(Pair("BTC", "USDT"))
    with pytest.raises(errors.NoPrice):
        p.get_price(Pair("BTC", "USDT"))
    with pytest.raises(errors.NoPrice):
        p.convert(Decimal(1), "BTC", "USDT")


def test_prices():
    pair = Pair("BTC", "USDT")
    conf = config.Config()
    conf.set_pair_info(pair, PairInfo(8, 2))
    p = prices.Prices(bid_ask_spread_pct=Decimal("1"), config=conf)

    now = dt.local_now()
    p.on_bar_event(
        BarEvent(now, Bar(now, pair, Decimal(10), Decimal(10), Decimal(10), Decimal(10), Decimal(10)))
    )

    assert p.get_bid_ask(pair) == (Decimal("9.95"), Decimal("10.05"))
    assert p.get_price(pair) == Decimal(10)
    assert p.convert(Decimal(100), pair.base_symbol, pair.quote_symbol) == Decimal(1000)
    assert p.convert(Decimal(1000), pair.quote_symbol, pair.base_symbol) == Decimal(100)
