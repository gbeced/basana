# Basana
#
# Copyright 2022-2023 Gabriel Martin Becedillas Ruiz
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

from basana.backtesting import account_balances, orders
from basana.core import dt, pair


def add_order_fill(order, balances, balance_updates):
    order.add_fill(dt.utc_now(), balance_updates, {})
    balances.order_updated(order, balance_updates)


def test_updates_and_holds():
    balances = account_balances.AccountBalances({})
    balances.update(balance_updates={"BTC": Decimal(0)})

    with pytest.raises(Exception, match="Not enough BTC available"):
        balances.update(balance_updates={"BTC": Decimal(-1)})

    balances.update(balance_updates={"BTC": Decimal(1)})

    with pytest.raises(Exception, match="Not enough USD available"):
        balances.update(balance_updates={"BTC": Decimal(2), "USD": Decimal(-1)})

    assert balances.get_available_balance("BTC") == Decimal(1)
    balances.update(hold_updates={"BTC": Decimal("0.8")})
    assert balances.get_available_balance("BTC") == Decimal("0.2")
    balances.update(hold_updates={"BTC": Decimal("0.1")})
    assert balances.get_available_balance("BTC") == Decimal("0.1")
    balances.update(hold_updates={"BTC": Decimal("0.1")})
    assert balances.get_available_balance("BTC") == Decimal("0")
    assert balances.get_balance_on_hold("BTC") == Decimal("1")

    with pytest.raises(Exception, match="Not enough BTC available"):
        balances.update(hold_updates={"BTC": Decimal("0.001")})

    with pytest.raises(Exception, match="Not enough BTC available"):
        balances.update(balance_updates={"BTC": Decimal("-0.001")})

    balances.update(hold_updates={"BTC": Decimal("-0.5")})
    assert balances.get_available_balance("BTC") == Decimal("0.5")
    assert balances.get_balance_on_hold("BTC") == Decimal("0.5")

    balances.update(balance_updates={"BTC": Decimal("-0.5")})
    assert balances.get_available_balance("BTC") == Decimal("0")
    assert balances.get_balance_on_hold("BTC") == Decimal("0.5")

    balances.update(hold_updates={"BTC": Decimal("-0.5")})
    assert balances.get_available_balance("BTC") == Decimal("0.5")
    assert balances.get_balance_on_hold("BTC") == Decimal("0")


def test_market_order_gets_completed():
    balances = account_balances.AccountBalances({"USD": Decimal(10000)})

    assert balances.get_available_balance("USD") == Decimal(10000)
    assert balances.get_balance_on_hold("USD") == Decimal(0)
    assert balances.get_available_balance("BTC") == Decimal(0)
    assert balances.get_balance_on_hold("BTC") == Decimal(0)
    assert balances.get_balance_on_hold_for_order("1", "USD") == Decimal(0)

    order = orders.MarketOrder(
        "1", orders.OrderOperation.BUY, pair.Pair("BTC", "USD"), Decimal("0.1"), orders.OrderState.OPEN
    )
    balances.order_accepted(order, {})

    assert balances.get_available_balance("USD") == Decimal(10000)
    assert balances.get_balance_on_hold("USD") == Decimal(0)
    assert balances.get_available_balance("BTC") == Decimal(0)
    assert balances.get_balance_on_hold("BTC") == Decimal(0)
    assert balances.get_balance_on_hold_for_order(order.id, "USD") == Decimal(0)

    add_order_fill(order, balances, {"BTC": Decimal("0.05"), "USD": Decimal("-1010")})

    assert balances.get_available_balance("USD") == Decimal(8990)
    assert balances.get_balance_on_hold("USD") == Decimal(0)
    assert balances.get_available_balance("BTC") == Decimal("0.05")
    assert balances.get_balance_on_hold("BTC") == Decimal(0)
    assert balances.get_balance_on_hold_for_order(order.id, "USD") == Decimal(0)

    add_order_fill(order, balances, {"BTC": Decimal("0.04"), "USD": Decimal("-1010")})

    assert balances.get_available_balance("USD") == Decimal(7980)
    assert balances.get_balance_on_hold("USD") == Decimal(0)
    assert balances.get_available_balance("BTC") == Decimal("0.09")
    assert balances.get_balance_on_hold("BTC") == Decimal(0)
    assert balances.get_balance_on_hold_for_order(order.id, "USD") == Decimal(0)

    add_order_fill(order, balances, {"BTC": Decimal("0.01"), "USD": Decimal("-250")})

    assert balances.get_available_balance("USD") == Decimal(7730)
    assert balances.get_balance_on_hold("USD") == Decimal(0)
    assert balances.get_available_balance("BTC") == Decimal("0.1")
    assert balances.get_balance_on_hold("BTC") == Decimal(0)
    assert balances.get_balance_on_hold_for_order(order.id, "USD") == Decimal(0)


def test_limit_order_gets_completed():
    balances = account_balances.AccountBalances({"USD": Decimal(10000)})

    assert balances.get_available_balance("USD") == Decimal(10000)
    assert balances.get_balance_on_hold("USD") == Decimal(0)
    assert balances.get_available_balance("BTC") == Decimal(0)
    assert balances.get_balance_on_hold("BTC") == Decimal(0)
    assert balances.get_balance_on_hold_for_order("1", "USD") == Decimal(0)

    order = orders.LimitOrder(
        "1", orders.OrderOperation.BUY, pair.Pair("BTC", "USD"), Decimal("0.1"), Decimal("30000"),
        orders.OrderState.OPEN
    )
    balances.order_accepted(order, {"USD": Decimal("3000")})

    assert balances.get_available_balance("USD") == Decimal(7000)
    assert balances.get_balance_on_hold("USD") == Decimal(3000)
    assert balances.get_available_balance("BTC") == Decimal(0)
    assert balances.get_balance_on_hold("BTC") == Decimal(0)
    assert balances.get_balance_on_hold_for_order(order.id, "USD") == Decimal(3000)

    add_order_fill(order, balances, {"BTC": Decimal("0.05"), "USD": Decimal("-1500")})

    assert balances.get_available_balance("USD") == Decimal(7000)
    assert balances.get_balance_on_hold("USD") == Decimal(1500)
    assert balances.get_available_balance("BTC") == Decimal("0.05")
    assert balances.get_balance_on_hold("BTC") == Decimal(0)
    assert balances.get_balance_on_hold_for_order(order.id, "USD") == Decimal(1500)

    add_order_fill(order, balances, {"BTC": Decimal("0.05"), "USD": Decimal("-1400")})

    assert balances.get_available_balance("USD") == Decimal(7100)
    assert balances.get_balance_on_hold("USD") == Decimal(0)
    assert balances.get_available_balance("BTC") == Decimal("0.1")
    assert balances.get_balance_on_hold("BTC") == Decimal(0)
    assert balances.get_balance_on_hold_for_order(order.id, "USD") == Decimal(0)


def test_order_gets_canceled():
    balances = account_balances.AccountBalances({"USD": Decimal(10000)})

    assert balances.get_available_balance("USD") == Decimal(10000)
    assert balances.get_balance_on_hold("USD") == Decimal(0)
    assert balances.get_available_balance("BTC") == Decimal(0)
    assert balances.get_balance_on_hold("BTC") == Decimal(0)
    assert balances.get_balance_on_hold_for_order("1", "USD") == Decimal(0)

    order = orders.MarketOrder(
        "1", orders.OrderOperation.BUY, pair.Pair("BTC", "USD"), Decimal("0.1"), orders.OrderState.OPEN
    )
    balances.order_accepted(order, {"USD": Decimal("2100")})

    assert balances.get_available_balance("USD") == Decimal(7900)
    assert balances.get_balance_on_hold("USD") == Decimal(2100)
    assert balances.get_available_balance("BTC") == Decimal(0)
    assert balances.get_balance_on_hold("BTC") == Decimal(0)
    assert balances.get_balance_on_hold_for_order(order.id, "USD") == Decimal(2100)

    balances.order_updated(order, {"BTC": Decimal("0.05"), "USD": Decimal("-1010")})

    assert balances.get_available_balance("USD") == Decimal(7900)
    assert balances.get_balance_on_hold("USD") == Decimal(1090)
    assert balances.get_available_balance("BTC") == Decimal("0.05")
    assert balances.get_balance_on_hold("BTC") == Decimal(0)
    assert balances.get_balance_on_hold_for_order(order.id, "USD") == Decimal(1090)

    order.cancel()
    balances.order_updated(order, {})

    assert balances.get_available_balance("USD") == Decimal(8990)
    assert balances.get_balance_on_hold("USD") == Decimal(0)
    assert balances.get_available_balance("BTC") == Decimal("0.05")
    assert balances.get_balance_on_hold("BTC") == Decimal(0)
    assert balances.get_balance_on_hold_for_order(order.id, "USD") == Decimal(0)


def test_symbols():
    balances = account_balances.AccountBalances({"USD": Decimal(10000)})
    assert balances.get_symbols() == ["USD"]

    order = orders.MarketOrder(
        "1", orders.OrderOperation.BUY, pair.Pair("BTC", "USD"), Decimal("0.1"), orders.OrderState.OPEN
    )
    balances.order_accepted(order, {})
    assert balances.get_symbols() == ["USD"]

    add_order_fill(order, balances, {"BTC": Decimal("0.05"), "USD": Decimal("-5000")})
    symbols = balances.get_symbols()
    symbols.sort()
    assert symbols == ["BTC", "USD"]

    add_order_fill(order, balances, {"BTC": Decimal("0.05"), "USD": Decimal("-5000")})
    symbols = balances.get_symbols()
    symbols.sort()
    assert symbols == ["BTC", "USD"]
    assert balances.get_balance_on_hold("USD") == 0
    assert balances.get_available_balance("USD") == 0
