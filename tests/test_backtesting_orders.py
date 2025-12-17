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
from uuid import uuid4
import datetime

import pytest

from basana.backtesting import exchange, liquidity, value_map
from basana.backtesting.orders import OrderOperation, MarketOrder, LimitOrder, StopOrder, \
    StopLimitOrder
from basana.core import bar, dt
from basana.core.pair import Pair, PairInfo

base_currency = "BTC"
quote_currency = "USD"
pair = Pair(base_currency, quote_currency)
pair_info = PairInfo(8, 2)


@pytest.mark.parametrize("order, ohlcv, expected_balance_updates", [
    # Buy market
    (
        MarketOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("1")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("1"),
            quote_currency: Decimal("-40001.76"),
        }
    ),
    # Buy market. Rounding should take place.
    (
        MarketOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("1.61")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("1.61"),
            quote_currency: Decimal("-64402.83"),
        }
    ),
    (
        MarketOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("1.61")),
        (Decimal("3.14"), Decimal("3.14"), Decimal("3.14"), Decimal("3.14"), Decimal("1")),
        {
            base_currency: Decimal("1.61"),
            quote_currency: Decimal("-5.06"),
        }
    ),
    # Buy limit
    (
        LimitOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("1"), Decimal("39000.01")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("1"),
            quote_currency: Decimal("-39000.01"),
        }
    ),
    (
        LimitOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("1"), Decimal("4")),
        (Decimal("3.14"), Decimal("3.14"), Decimal("3.14"), Decimal("3.14"), Decimal("1")),
        {
            base_currency: Decimal("1"),
            quote_currency: Decimal("-3.14"),
        }
    ),
    # Buy limit uses open price which is better.
    (
        LimitOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("2"), Decimal("42000")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("2"),
            quote_currency: Decimal("-80003.52"),
        }
    ),
    # Buy limit uses open price which is better. Rounding should take place
    (
        LimitOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("3.8"), Decimal("42000")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("3.8"),
            quote_currency: Decimal("-152006.69"),
        }
    ),
    # Buy limit price not hit.
    (
        LimitOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("2"), Decimal("29000")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {}
    ),
    (
        LimitOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("2"), Decimal("50")),
        (Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("500")),
        {}
    ),
    # Buy stop not hit.
    (
        StopOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("2"), Decimal("50401.02")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {}
    ),
    (
        StopOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("2"), Decimal("100.01")),
        (Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("500")),
        {}
    ),
    # Buy stop hit on open.
    (
        StopOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("1"), Decimal("30000.01")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("1"),
            quote_currency: Decimal("-40001.76"),
        }
    ),
    (
        StopOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("1"), Decimal("99.99")),
        (Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), Decimal("500")),
        {
            base_currency: Decimal("1"),
            quote_currency: Decimal("-100"),
        }
    ),
    # Buy stop hit after open.
    (
        StopOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("1"), Decimal("40001.77")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("1"),
            quote_currency: Decimal("-40001.77"),
        }
    ),
    # Buy stop limit not hit.
    (
        StopLimitOrder(
            uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("2"), Decimal("50401.02"), Decimal("60000")
        ),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {}
    ),
    (
        StopLimitOrder(
            uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("2"), Decimal("50401.02"), Decimal("60000")
        ),
        (Decimal("50401.01"), Decimal("50401.01"), Decimal("50401.01"), Decimal("50401.01"), Decimal("10")),
        {}
    ),
    # Buy stop limit not hit.
    (
        StopLimitOrder(
            uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("2"), Decimal("20000"), Decimal("21000")
        ),
        (Decimal("21100"), Decimal("21900"), Decimal("21000.01"), Decimal("21000.01"), Decimal("100")),
        {}
    ),
    (
        StopLimitOrder(
            uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("2"), Decimal("20000"), Decimal("21000")
        ),
        (Decimal("25000"), Decimal("25000"), Decimal("25000"), Decimal("25000"), Decimal("100")),
        {}
    ),
    # Buy stop limit hit on open.
    (
        StopLimitOrder(
            uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("1"), Decimal("40000"), Decimal("42000")
        ),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("1"),
            quote_currency: Decimal("-40001.76"),
        }
    ),
    # Buy stop limit hit after open.
    (
        StopLimitOrder(
            uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("2"), Decimal("41000"), Decimal("42000")
        ),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("2"),
            quote_currency: Decimal("-84000"),
        }
    ),
    (
        StopLimitOrder(
            uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("1"), Decimal("41000"), Decimal("42000")
        ),
        (Decimal("41900"), Decimal("41900"), Decimal("41900"), Decimal("41900"), Decimal("100")),
        {
            base_currency: Decimal("1"),
            quote_currency: Decimal("-41900"),
        }
    ),
    # Sell market
    (
        MarketOrder(uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("1")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("-1"),
            quote_currency: Decimal("40001.76"),
        }
    ),
    # Sell limit
    (
        LimitOrder(uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("1"), Decimal("41200.02")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("-1"),
            quote_currency: Decimal("41200.02"),
        }
    ),
    # Sell limit uses open price which is better.
    (
        LimitOrder(uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("2"), Decimal("39000")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("-2"),
            quote_currency: Decimal("80003.52"),
        }
    ),
    # Sell limit price not hit.
    (
        LimitOrder(uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("2"), Decimal("50401.02")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {}
    ),
    # Sell stop not hit.
    (
        StopOrder(uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("2"), Decimal("29999.99")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {}
    ),
    # Sell stop hit on open.
    (
        StopOrder(uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("1"), Decimal("40002")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("-1"),
            quote_currency: Decimal("40001.76"),
        }
    ),
    # Sell stop hit on open. Rounding should take place
    (
        StopOrder(uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("3.6"), Decimal("40002")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("-3.6"),
            quote_currency: Decimal("144006.34"),
        }
    ),
    # Sell stop hit after open.
    (
        StopOrder(uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("1"), Decimal("35000")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("-1"),
            quote_currency: Decimal("35000"),
        }
    ),
    # Sell stop limit hit on open.
    (
        StopLimitOrder(
            uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("1"), Decimal("41000"), Decimal("40000")
        ),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("-1"),
            quote_currency: Decimal("40001.76"),
        }
    ),
    # Sell stop limit hit after open.
    (
        StopLimitOrder(
            uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("1"), Decimal("35000"), Decimal("41000")
        ),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("-1"),
            quote_currency: Decimal("41000"),
        }
    ),
    # Sell stop limit hit after open. Rounding should take place.
    (
        StopLimitOrder(
            uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("1.07"), Decimal("35000"), Decimal("41000.09")
        ),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("-1.07"),
            quote_currency: Decimal("43870.10"),
        }
    ),
])
def test_try_fill_with_infinite_liquidity(order, ohlcv, expected_balance_updates, backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {})  # Just for rounding purposes
    e.set_pair_info(pair, PairInfo(8, 2))

    ls = liquidity.InfiniteLiquidity()
    b = bar.Bar(dt.local_now(), pair, *ohlcv, datetime.timedelta(seconds=1))
    fill = order.try_fill(b, ls)
    balance_updates = value_map.ValueMap({} if fill is None else fill.balance_updates)
    assert balance_updates == expected_balance_updates


@pytest.mark.parametrize("order, ohlcv, expected_balance_updates", [
    # Buy market but there is not enough volume.
    (
        MarketOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("2000")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {}
    ),
    (
        MarketOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("10")),
        (Decimal("1000"), Decimal("1000"), Decimal("1000"), Decimal("1000"), Decimal("1")),
        {}
    ),
    # Buy market but there is not enough volume. 250.075 should be available, which should get truncated to 250.07.
    (
        MarketOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("250.08")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {}
    ),
    # Buy market with slippage.
    (
        MarketOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("10")),
        (Decimal("50000"), Decimal("51000.01"), Decimal("49000"), Decimal("50500"), Decimal("40")),
        {
            base_currency: Decimal("10"),
            quote_currency: Decimal("-550000"),
        }
    ),
    (
        MarketOrder(uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("10")),
        (Decimal("50000"), Decimal("50000"), Decimal("50000"), Decimal("50000"), Decimal("40")),
        {
            base_currency: Decimal("10"),
            quote_currency: Decimal("-550000"),
        }
    ),
    # Sell market. Rounding takes place. 250.075 should be available, which should get truncated to 250.07.
    (
        MarketOrder(uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("250.07")),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {
            base_currency: Decimal("-250.07000000"),
            quote_currency: Decimal("9002955.12"),
        }
    ),
    # Sell stop but there is not enough volume.
    (
        StopOrder(
            uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("1004"), Decimal("35000")
        ),
        (Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")),
        {}
    ),
])
def test_try_fill_with_finite_liquidity(order, ohlcv, expected_balance_updates, backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {})  # Just for rounding purposes
    e.set_pair_info(pair, PairInfo(8, 2))

    ls = liquidity.VolumeShareImpact()
    b = bar.Bar(
        dt.local_now(), pair, *ohlcv,
        datetime.timedelta(seconds=1)
    )
    ls.on_bar(b)

    fill = order.try_fill(b, ls)
    balance_updates = value_map.ValueMap({} if fill is None else fill.balance_updates)
    e._order_mgr._round_balance_updates(balance_updates, order.pair)
    assert balance_updates == expected_balance_updates


@pytest.mark.parametrize("order", [
    LimitOrder(
        uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("1"), Decimal("39000.01")
    ),
    LimitOrder(
        uuid4().hex, OrderOperation.BUY, pair, pair_info, Decimal("1"), Decimal("49000.01")
    ),
    StopLimitOrder(
        uuid4().hex, OrderOperation.SELL, pair, pair_info, Decimal("1"), Decimal("39000.01"),
        Decimal("39000.01")
    ),
])
def test_no_liquidity_calculating_balance_updates(order, backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {})  # Just for rounding purposes
    e.set_pair_info(pair, PairInfo(8, 2))

    ls = liquidity.VolumeShareImpact()
    b = bar.Bar(
        dt.local_now(), pair,
        Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("0"),
        datetime.timedelta(seconds=1)
    )
    ls.on_bar(b)

    assert order.try_fill(b, ls) is None
