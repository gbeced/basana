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
import asyncio

import pytest

from basana.backtesting import account_balances, exchange, liquidity, orders
from basana.backtesting.value_map import ValueMap
from basana.core import bar, dt, event
from basana.core.pair import Pair, PairInfo


def test_updates_and_holds():
    balances = account_balances.AccountBalances({})

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

    with pytest.raises(Exception, match="Not enough BTC available to hold"):
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


def test_invalid_updates_and_holds():
    balances = account_balances.AccountBalances({})

    with pytest.raises(Exception, match="Not enough BTC available"):
        balances.update(balance_updates={"BTC": Decimal(-1)})

    with pytest.raises(Exception, match="Not enough BTC available to hold"):
        balances.update(hold_updates={"BTC": Decimal(1)})

    with pytest.raises(Exception, match="hold update amount for BTC is invalid"):
        balances.update(hold_updates={"BTC": Decimal(-1)})

    with pytest.raises(Exception, match="borrowed update amount for BTC is invalid"):
        balances.update(borrowed_updates={"BTC": Decimal(-1)})


@pytest.mark.parametrize("order_fun, checkpoints", [
    (
        lambda e: e.create_market_order(orders.OrderOperation.BUY, Pair("BTC", "USD"), Decimal("1")),
        {
            dt.local_datetime(2001, 1, 2): {
                "BTC": {"available": Decimal("1"), "hold": Decimal("0"), "borrowed": Decimal("0")},
                "USD": {"available": Decimal("999900"), "hold": Decimal("0"), "borrowed": Decimal("0")},
            }
        }
    ),
    (
        lambda e: e.create_limit_order(orders.OrderOperation.BUY, Pair("BTC", "USD"), Decimal("150"), Decimal("100")),
        {
            dt.local_datetime(2001, 1, 1): {
                "BTC": {"available": Decimal("0"), "hold": Decimal("0"), "borrowed": Decimal("0")},
                "USD": {"available": Decimal("1e6"), "hold": Decimal("15000"), "borrowed": Decimal("0")},
            },
            dt.local_datetime(2001, 1, 2): {
                "BTC": {"available": Decimal("100"), "hold": Decimal("0"), "borrowed": Decimal("0")},
                "USD": {"available": Decimal("985000"), "hold": Decimal("5000"), "borrowed": Decimal("0")},
            },
            dt.local_datetime(2001, 1, 3): {
                "BTC": {"available": Decimal("150"), "hold": Decimal("0"), "borrowed": Decimal("0")},
                "USD": {"available": Decimal("985000"), "hold": Decimal("0"), "borrowed": Decimal("0")},
            }
        }
    ),
])
def test_balance_updates_as_orders_get_processed(order_fun, checkpoints, backtesting_dispatcher):
    pair = Pair("BTC", "USD")
    e = exchange.Exchange(
        backtesting_dispatcher,
        {pair.quote_symbol: Decimal(1e6)},
        liquidity_strategy_factory=lambda: liquidity.VolumeShareImpact(
            volume_limit_pct=Decimal(100), price_impact=Decimal(0)
        ),
    )
    e.set_pair_info(pair, PairInfo(8, 2))
    mismatches = []

    async def on_bar(bar_event):
        expected_balances = checkpoints.get(bar_event.when, {})
        for symbol, expected_balance in expected_balances.items():
            balance = await e.get_balance(symbol)
            for name, value in expected_balance.items():
                if getattr(balance, name) != value:
                    mismatches.append((bar_event.when, symbol, name, getattr(balance, name)))

    async def impl():
        # These are for testing scenarios where fills take place in multiple bars.
        src = event.FifoQueueEventSource(events=[
            bar.BarEvent(
                dt.local_datetime(2001, 1, 2),
                bar.Bar(
                    dt.local_datetime(2001, 1, 1),
                    pair, Decimal(100), Decimal(100), Decimal(100), Decimal(100), Decimal(100)
                )
            ),
            bar.BarEvent(
                dt.local_datetime(2001, 1, 3),
                bar.Bar(
                    dt.local_datetime(2001, 1, 2),
                    pair, Decimal(100), Decimal(100), Decimal(100), Decimal(100), Decimal(100)
                )
            ),
            bar.BarEvent(
                dt.local_datetime(2001, 1, 4),
                bar.Bar(
                    dt.local_datetime(2001, 1, 3),
                    pair, Decimal(100), Decimal(100), Decimal(100), Decimal(100), Decimal(100)
                )
            ),
        ])
        e.add_bar_source(src)
        e.subscribe_to_bar_events(pair, on_bar)
        await order_fun(e)
        await backtesting_dispatcher.run()
        assert mismatches == []

    asyncio.run(impl())


def test_account_locked():
    class LockAccount(account_balances.UpdateRule):
        def check(self, updated_balances: ValueMap, updated_holds: ValueMap, updated_borrowed: ValueMap):
            raise Exception("Account locked")

    balances = account_balances.AccountBalances({})
    balances.push_update_rule(LockAccount())

    with pytest.raises(Exception, match="Account locked"):
        balances.update(balance_updates={"BTC": Decimal(1)})
