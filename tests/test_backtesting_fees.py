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

from basana.backtesting import fees, orders
from basana.backtesting.exchange import OrderOperation
from basana.core import dt
from basana.core.pair import Pair


def test_percentage_fee_with_partial_fills():
    fee_strategy = fees.Percentage(Decimal("1"))
    order = orders.MarketOrder("1", OrderOperation.BUY, Pair("BTC", "USD"), Decimal("0.01"), orders.OrderState.OPEN)

    # Fill #1 - A 0.009 fee gets rounded to 0.01
    balance_updates = {
        "BTC": Decimal("0.001"),
        "USD": Decimal("-0.9"),
    }
    assert fee_strategy.calculate_fees(order, balance_updates) == {"USD": Decimal("-0.009")}
    order.add_fill(dt.utc_now(), balance_updates, {"USD": Decimal("-0.01")})

    # Fill #2 - A 0.008 fee gets rounded to 0.01
    balance_updates = {
        "BTC": Decimal("0.001"),
        "USD": Decimal("-0.9"),
    }
    assert fee_strategy.calculate_fees(order, balance_updates) == {"USD": Decimal("-0.008")}
    order.add_fill(dt.utc_now(), balance_updates, {"USD": Decimal("-0.01")})

    # Fill #3 - Final fill. Total fees, prior to rounding, should be 0.118, but we charged 0.02 already, so the last
    # chunk, prior to rounding, should be 0.098.
    balance_updates = {
        "BTC": Decimal("0.008"),
        "USD": Decimal("-10"),
    }
    assert fee_strategy.calculate_fees(order, balance_updates) == {"USD": Decimal("-0.098")}
    order.add_fill(dt.utc_now(), balance_updates, {"USD": Decimal("-0.1")})


def test_percentage_fee_with_minium():
    fee_strategy = fees.Percentage(Decimal("1"), min_fee=Decimal("5"))
    order = orders.MarketOrder("1", OrderOperation.BUY, Pair("BTC", "USD"), Decimal("0.1"), orders.OrderState.OPEN)

    balance_updates = {
        "BTC": Decimal("0.1"),
        "USD": Decimal("-50.15"),
    }
    assert fee_strategy.calculate_fees(order, balance_updates) == {"USD": Decimal("-5")}
