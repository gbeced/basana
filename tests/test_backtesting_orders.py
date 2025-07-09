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

from uuid import uuid4
from decimal import Decimal

import pytest

from basana.backtesting import exchange, liquidity, value_map
from basana.backtesting.orders import OrderOperation, MarketOrder, LimitOrder, StopOrder, \
    StopLimitOrder
from basana.core import bar, dt
from basana.core.pair import Pair, PairInfo

base_currency = "BTC"
quote_currency = "USD"
pair = Pair(base_currency, quote_currency)


@pytest.mark.parametrize(
    "order, expected_balance_updates", [
        # Buy market
        (
            MarketOrder(uuid4().hex, OrderOperation.BUY, pair, Decimal("1")),
            {
                base_currency: Decimal("1"),
                quote_currency: Decimal("-40001.76"),
            }
        ),
        # Buy market. Rounding should take place.
        (
            MarketOrder(uuid4().hex, OrderOperation.BUY, pair, Decimal("1.61")),
            {
                base_currency: Decimal("1.61"),
                quote_currency: Decimal("-64402.83"),
            }
        ),
        # Buy limit
        (
            LimitOrder(
                uuid4().hex, OrderOperation.BUY, pair, Decimal("1"), Decimal("39000.01")
            ),
            {
                base_currency: Decimal("1"),
                quote_currency: Decimal("-39000.01"),
            }
        ),
        # Buy limit uses open price which is better.
        (
            LimitOrder(
                uuid4().hex, OrderOperation.BUY, pair, Decimal("2"), Decimal("42000")
            ),
            {
                base_currency: Decimal("2"),
                quote_currency: Decimal("-80003.52"),
            }
        ),
        # Buy limit uses open price which is better. Rounding should take place
        (
                LimitOrder(
                    uuid4().hex, OrderOperation.BUY, pair, Decimal("3.8"), Decimal("42000")
                ),
                {
                    base_currency: Decimal("3.8"),
                    quote_currency: Decimal("-152006.69"),
                }
        ),
        # Buy limit price not hit.
        (
            LimitOrder(
                uuid4().hex, OrderOperation.BUY, pair, Decimal("2"), Decimal("29000")
            ),
            {}
        ),
        # Buy stop not hit.
        (
            StopOrder(
                uuid4().hex, OrderOperation.BUY, pair, Decimal("2"), Decimal("50401.02")
            ),
            {}
        ),
        # Buy stop hit on open.
        (
            StopOrder(
                uuid4().hex, OrderOperation.BUY, pair, Decimal("1"), Decimal("30000.01")
            ),
            {
                base_currency: Decimal("1"),
                quote_currency: Decimal("-40001.76"),
            }
        ),
        # Buy stop hit after open.
        (
            StopOrder(
                uuid4().hex, OrderOperation.BUY, pair, Decimal("1"), Decimal("40001.77")
            ),
            {
                base_currency: Decimal("1"),
                quote_currency: Decimal("-40001.77"),
            }
        ),
        # Buy stop limit not hit.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.BUY, pair, Decimal("2"), Decimal("50401.02"),
                Decimal("60000"),
            ),
            {}
        ),
        # Buy stop limit not hit.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.BUY, pair, Decimal("2"), Decimal("20000"), Decimal("21000")
            ),
            {}
        ),
        # Buy stop limit hit on open.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.BUY, pair, Decimal("1"), Decimal("40000"), Decimal("42000")
            ),
            {
                base_currency: Decimal("1"),
                quote_currency: Decimal("-40001.76"),
            }
        ),
        # Buy stop limit hit after open.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.BUY, pair, Decimal("2"), Decimal("41000"), Decimal("42000")
            ),
            {
                base_currency: Decimal("2"),
                quote_currency: Decimal("-84000"),
            }
        ),
        # Sell market
        (
            MarketOrder(uuid4().hex, OrderOperation.SELL, pair, Decimal("1")),
            {
                base_currency: Decimal("-1"),
                quote_currency: Decimal("40001.76"),
            }
        ),
        # Sell limit
        (
            LimitOrder(
                uuid4().hex, OrderOperation.SELL, pair, Decimal("1"), Decimal("41200.02")
            ),
            {
                base_currency: Decimal("-1"),
                quote_currency: Decimal("41200.02"),
            }
        ),
        # Sell limit uses open price which is better.
        (
            LimitOrder(
                uuid4().hex, OrderOperation.SELL, pair, Decimal("2"), Decimal("39000")
            ),
            {
                base_currency: Decimal("-2"),
                quote_currency: Decimal("80003.52"),
            }
        ),
        # Sell limit price not hit.
        (
            LimitOrder(
                uuid4().hex, OrderOperation.SELL, pair, Decimal("2"), Decimal("50401.02")
            ),
            {}
        ),
        # Sell stop not hit.
        (
            StopOrder(
                uuid4().hex, OrderOperation.SELL, pair, Decimal("2"), Decimal("29999.99")
            ),
            {}
        ),
        # Sell stop hit on open.
        (
            StopOrder(
                uuid4().hex, OrderOperation.SELL, pair, Decimal("1"), Decimal("40002")
            ),
            {
                base_currency: Decimal("-1"),
                quote_currency: Decimal("40001.76"),
            }
        ),
        # Sell stop hit on open. Rounding should take place
        (
            StopOrder(
                uuid4().hex, OrderOperation.SELL, pair, Decimal("3.6"), Decimal("40002")
            ),
            {
                base_currency: Decimal("-3.6"),
                quote_currency: Decimal("144006.34"),
            }
        ),
        # Sell stop hit after open.
        (
            StopOrder(
                uuid4().hex, OrderOperation.SELL, pair, Decimal("1"), Decimal("35000")
            ),
            {
                base_currency: Decimal("-1"),
                quote_currency: Decimal("35000"),
            }
        ),
        # Sell stop limit hit on open.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.SELL, pair, Decimal("1"), Decimal("41000"), Decimal("40000")
            ),
            {
                base_currency: Decimal("-1"),
                quote_currency: Decimal("40001.76"),
            }
        ),
        # Sell stop limit hit after open.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.SELL, pair, Decimal("1"), Decimal("35000"), Decimal("41000")
            ),
            {
                base_currency: Decimal("-1"),
                quote_currency: Decimal("41000"),
            }
        ),
        # Sell stop limit hit after open. Rounding should take place.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.SELL, pair, Decimal("1.07"), Decimal("35000"),
                Decimal("41000.09")
            ),
            {
                base_currency: Decimal("-1.07"),
                quote_currency: Decimal("43870.10"),
            }
        ),
    ]
)
def test_get_balance_updates_with_infinite_liquidity(order, expected_balance_updates, backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {})  # Just for rounding purposes
    e.set_pair_info(pair, PairInfo(8, 2))

    ls = liquidity.InfiniteLiquidity()
    b = bar.Bar(
        dt.local_now(), pair,
        Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")
    )
    balance_updates = value_map.ValueMap(order.get_balance_updates(b, ls))
    e._order_mgr._round_balance_updates(balance_updates, order.pair)
    assert balance_updates == expected_balance_updates


@pytest.mark.parametrize(
    "order, expected_balance_updates", [
        # Buy market but there is not enough balance.
        (
            MarketOrder(uuid4().hex, OrderOperation.BUY, pair, Decimal("2000")),
            {}
        ),
        # Buy market. Rounding takes place. 250.075 should be available, which should get truncated to 250.07.
        (
            MarketOrder(uuid4().hex, OrderOperation.BUY, pair, Decimal("250.08")),
            {}
        ),
        # Sell market. Rounding takes place. 250.075 should be available, which should get truncated to 250.07.
        (
            MarketOrder(uuid4().hex, OrderOperation.SELL, pair, Decimal("250.07")),
            {
                base_currency: Decimal("-250.07"),
                quote_currency: Decimal("9002956.11"),
            }
        ),
        # Sell stop but there is not enough balance.
        (
            StopOrder(
                uuid4().hex, OrderOperation.SELL, pair, Decimal("1004"), Decimal("35000")
            ),
            {}
        ),
    ]
)
def test_get_balance_updates_with_finite_liquidity(order, expected_balance_updates, backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {})  # Just for rounding purposes
    e.set_pair_info(pair, PairInfo(8, 2))

    ls = liquidity.VolumeShareImpact()
    b = bar.Bar(
        dt.local_now(), pair,
        Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")
    )
    ls.on_bar(b)

    balance_updates = value_map.ValueMap(order.get_balance_updates(b, ls))
    e._order_mgr._round_balance_updates(balance_updates, order.pair)
    assert balance_updates == expected_balance_updates


@pytest.mark.parametrize(
    "order", [
        LimitOrder(
            uuid4().hex, OrderOperation.SELL, pair, Decimal("1"), Decimal("39000.01")
        ),
        LimitOrder(
            uuid4().hex, OrderOperation.BUY, pair, Decimal("1"), Decimal("49000.01")
        ),
        StopLimitOrder(
            uuid4().hex, OrderOperation.SELL, pair, Decimal("1"), Decimal("39000.01"),
            Decimal("39000.01")
        ),
    ]
)
def test_no_liquidity_calculating_balance_updates(order, backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {})  # Just for rounding purposes
    e.set_pair_info(pair, PairInfo(8, 2))

    ls = liquidity.VolumeShareImpact()
    b = bar.Bar(
        dt.local_now(), pair,
        Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("0")
    )
    ls.on_bar(b)

    assert order.get_balance_updates(b, ls) == {}
