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
from basana.backtesting.orders import OrderOperation, OrderState, MarketOrder, LimitOrder, StopOrder, \
    StopLimitOrder
from basana.core import bar, dt
from basana.core.pair import Pair, PairInfo


@pytest.mark.parametrize(
    "order, expected_balance_updates", [
        # Buy market
        (
            MarketOrder(uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("1"), OrderState.OPEN),
            {
                "BTC": Decimal("1"),
                "USD": Decimal("-40001.76"),
            }
        ),
        # Buy market. Rounding should take place.
        (
            MarketOrder(uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("1.61"), OrderState.OPEN),
            {
                "BTC": Decimal("1.61"),
                "USD": Decimal("-64402.83"),
            }
        ),
        # Buy limit
        (
            LimitOrder(
                uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("1"), Decimal("39000.01"),
                OrderState.OPEN
            ),
            {
                "BTC": Decimal("1"),
                "USD": Decimal("-39000.01"),
            }
        ),
        # Buy limit uses open price which is better.
        (
            LimitOrder(
                uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("2"), Decimal("42000"), OrderState.OPEN
            ),
            {
                "BTC": Decimal("2"),
                "USD": Decimal("-80003.52"),
            }
        ),
        # Buy limit uses open price which is better. Rounding should take place
        (
                LimitOrder(
                    uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("3.8"), Decimal("42000"),
                    OrderState.OPEN
                ),
                {
                    "BTC": Decimal("3.8"),
                    "USD": Decimal("-152006.69"),
                }
        ),
        # Buy limit price not hit.
        (
            LimitOrder(
                uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("2"), Decimal("29000"), OrderState.OPEN
            ),
            {}
        ),
        # Buy stop not hit.
        (
            StopOrder(
                uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("2"), Decimal("50401.02"),
                OrderState.OPEN
            ),
            {}
        ),
        # Buy stop hit on open.
        (
            StopOrder(
                uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("1"), Decimal("30000.01"),
                OrderState.OPEN
            ),
            {
                "BTC": Decimal("1"),
                "USD": Decimal("-40001.76"),
            }
        ),
        # Buy stop hit after open.
        (
            StopOrder(
                uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("1"), Decimal("40001.77"),
                OrderState.OPEN
            ),
            {
                "BTC": Decimal("1"),
                "USD": Decimal("-40001.77"),
            }
        ),
        # Buy stop limit not hit.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("2"), Decimal("50401.02"),
                Decimal("60000"), OrderState.OPEN,
            ),
            {}
        ),
        # Buy stop limit not hit.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("2"), Decimal("20000"), Decimal("21000"),
                OrderState.OPEN,
            ),
            {}
        ),
        # Buy stop limit hit on open.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("1"), Decimal("40000"), Decimal("42000"),
                OrderState.OPEN,
            ),
            {
                "BTC": Decimal("1"),
                "USD": Decimal("-40001.76"),
            }
        ),
        # Buy stop limit hit after open.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("2"), Decimal("41000"), Decimal("42000"),
                OrderState.OPEN
            ),
            {
                "BTC": Decimal("2"),
                "USD": Decimal("-84000"),
            }
        ),
        # Sell market
        (
            MarketOrder(uuid4().hex, OrderOperation.SELL, Pair("BTC", "USD"), Decimal("1"), OrderState.OPEN),
            {
                "BTC": Decimal("-1"),
                "USD": Decimal("40001.76"),
            }
        ),
        # Sell limit
        (
            LimitOrder(
                uuid4().hex, OrderOperation.SELL, Pair("BTC", "USD"), Decimal("1"), Decimal("41200.02"),
                OrderState.OPEN
            ),
            {
                "BTC": Decimal("-1"),
                "USD": Decimal("41200.02"),
            }
        ),
        # Sell limit uses open price which is better.
        (
            LimitOrder(
                uuid4().hex, OrderOperation.SELL, Pair("BTC", "USD"), Decimal("2"), Decimal("39000"), OrderState.OPEN
            ),
            {
                "BTC": Decimal("-2"),
                "USD": Decimal("80003.52"),
            }
        ),
        # Sell limit price not hit.
        (
            LimitOrder(
                uuid4().hex, OrderOperation.SELL, Pair("BTC", "USD"), Decimal("2"), Decimal("50401.02"),
                OrderState.OPEN
            ),
            {}
        ),
        # Sell stop not hit.
        (
            StopOrder(
                uuid4().hex, OrderOperation.SELL, Pair("BTC", "USD"), Decimal("2"), Decimal("29999.99"),
                OrderState.OPEN
            ),
            {}
        ),
        # Sell stop hit on open.
        (
            StopOrder(
                uuid4().hex, OrderOperation.SELL, Pair("BTC", "USD"), Decimal("1"), Decimal("40002"), OrderState.OPEN
            ),
            {
                "BTC": Decimal("-1"),
                "USD": Decimal("40001.76"),
            }
        ),
        # Sell stop hit on open. Rounding should take place
        (
            StopOrder(
                uuid4().hex, OrderOperation.SELL, Pair("BTC", "USD"), Decimal("3.6"), Decimal("40002"),
                OrderState.OPEN
            ),
            {
                "BTC": Decimal("-3.6"),
                "USD": Decimal("144006.34"),
            }
        ),
        # Sell stop hit after open.
        (
            StopOrder(
                uuid4().hex, OrderOperation.SELL, Pair("BTC", "USD"), Decimal("1"), Decimal("35000"), OrderState.OPEN
            ),
            {
                "BTC": Decimal("-1"),
                "USD": Decimal("35000"),
            }
        ),
        # Sell stop limit hit on open.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.SELL, Pair("BTC", "USD"), Decimal("1"), Decimal("41000"), Decimal("40000"),
                OrderState.OPEN
            ),
            {
                "BTC": Decimal("-1"),
                "USD": Decimal("40001.76"),
            }
        ),
        # Sell stop limit hit after open.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.SELL, Pair("BTC", "USD"), Decimal("1"), Decimal("35000"), Decimal("41000"),
                OrderState.OPEN
            ),
            {
                "BTC": Decimal("-1"),
                "USD": Decimal("41000"),
            }
        ),
        # Sell stop limit hit after open. Rounding should take place.
        (
            StopLimitOrder(
                uuid4().hex, OrderOperation.SELL, Pair("BTC", "USD"), Decimal("1.07"), Decimal("35000"),
                Decimal("41000.09"), OrderState.OPEN
            ),
            {
                "BTC": Decimal("-1.07"),
                "USD": Decimal("43870.10"),
            }
        ),
    ]
)
def test_get_balance_updates_with_infinite_liquidity(order, expected_balance_updates, backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {})  # Just for rounding purposes
    p = Pair("BTC", "USD")
    e.set_pair_info(p, PairInfo(8, 2))

    ls = liquidity.InfiniteLiquidity()
    b = bar.Bar(
        dt.local_now(), p,
        Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")
    )
    balance_updates = value_map.ValueMap(order.get_balance_updates(b, ls))
    e._order_mgr._round_balance_updates(balance_updates, order.pair)
    assert balance_updates == expected_balance_updates


@pytest.mark.parametrize(
    "order, expected_balance_updates", [
        # Buy market but there is not enough balance.
        (
            MarketOrder(uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("2000"), OrderState.OPEN),
            {}
        ),
        # Buy market. Rounding takes place. 250.075 should be available, which should get truncated to 250.07.
        (
            MarketOrder(uuid4().hex, OrderOperation.BUY, Pair("BTC", "USD"), Decimal("250.08"), OrderState.OPEN),
            {}
        ),
        # Sell market. Rounding takes place. 250.075 should be available, which should get truncated to 250.07.
        (
            MarketOrder(uuid4().hex, OrderOperation.SELL, Pair("BTC", "USD"), Decimal("250.07"), OrderState.OPEN),
            {
                "BTC": Decimal("-250.07"),
                "USD": Decimal("9002956.11"),
            }
        ),
        # Sell stop but there is not enough balance.
        (
            StopOrder(
                uuid4().hex, OrderOperation.SELL, Pair("BTC", "USD"), Decimal("1004"), Decimal("35000"),
                OrderState.OPEN
            ),
            {}
        ),
    ]
)
def test_get_balance_updates_with_finite_liquidity(order, expected_balance_updates, backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {})  # Just for rounding purposes
    p = Pair("BTC", "USD")
    e.set_pair_info(p, PairInfo(8, 2))

    ls = liquidity.VolumeShareImpact()
    b = bar.Bar(
        dt.local_now(), p,
        Decimal("40001.76"), Decimal("50401.01"), Decimal("30000"), Decimal("45157.09"), Decimal("1000.3")
    )
    ls.on_bar(b)

    balance_updates = value_map.ValueMap(order.get_balance_updates(b, ls))
    e._order_mgr._round_balance_updates(balance_updates, order.pair)
    assert balance_updates == expected_balance_updates
