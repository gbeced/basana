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

from basana.core import pair, bar, dt
from basana.backtesting import liquidity


def test_infinite_liquidity():
    strat = liquidity.InfiniteLiquidity()
    strat.on_bar(
        bar.Bar(
            dt.utc_now(), pair.Pair("BTC", "USD"),
            Decimal("50000"), Decimal("70000"), Decimal("49900"), Decimal("69999.07"), Decimal("0.00000001")
        )
    )

    assert strat.available_liquidity == Decimal("Infinity")
    assert strat.calculate_amount(Decimal("0")) == Decimal("Infinity")
    assert strat.calculate_amount(Decimal("0.01")) == Decimal("Infinity")
    for i in range(6):
        amount = Decimal(1 * 10**i)
        assert strat.calculate_price_impact(amount) == Decimal(0)
        assert strat.take_liquidity(amount) == Decimal(0)
    assert strat.available_liquidity == Decimal("Infinity")
    assert strat.calculate_amount(Decimal("0")) == Decimal("Infinity")
    assert strat.calculate_amount(Decimal("0.01")) == Decimal("Infinity")


def test_volume_share_impact():
    strat = liquidity.VolumeShareImpact()
    strat.on_bar(
        bar.Bar(
            dt.utc_now(), pair.Pair("BTC", "USD"),
            Decimal("50000"), Decimal("70000"), Decimal("49900"), Decimal("69999.07"), Decimal("10000")
        )
    )

    assert strat.available_liquidity == Decimal("2500")

    assert strat.calculate_price_impact(Decimal("2500")) == Decimal("0.1")
    assert strat.calculate_price_impact(Decimal("1250")) == Decimal("0.025")
    assert strat.available_liquidity == Decimal("2500")

    assert strat.calculate_amount(Decimal("0.1")) == Decimal("2500")
    assert strat.calculate_amount(Decimal("0.025")) == Decimal("1250")
    assert strat.calculate_amount(Decimal("0.09999")) < Decimal("2500")
    assert strat.available_liquidity == Decimal("2500")

    cummulative_slippage = Decimal(0)
    for _ in range(10):
        cummulative_slippage += strat.take_liquidity(Decimal("250"))
    assert cummulative_slippage == Decimal("0.1")
    assert strat.available_liquidity == Decimal("0")

    with pytest.raises(Exception, match="Not enough liquidity"):
        strat.calculate_amount(Decimal("0.1"))
    with pytest.raises(Exception, match="Not enough liquidity"):
        strat.calculate_price_impact(Decimal("1"))


def test_volume_share_impact_without_liquidity():
    strat = liquidity.VolumeShareImpact()
    strat.on_bar(
        bar.Bar(
            dt.utc_now(), pair.Pair("BTC", "USD"),
            Decimal("50000"), Decimal("70000"), Decimal("49900"), Decimal("69999.07"), Decimal(0)
        )
    )

    assert strat.available_liquidity == Decimal(0)
    assert strat.calculate_price_impact(Decimal(0)) == Decimal(0)
    assert strat.calculate_amount(Decimal(0)) == Decimal(0)
    assert strat.take_liquidity(Decimal(0)) == Decimal(0)

    error_msg = "Not enough liquidity"
    with pytest.raises(Exception, match=error_msg):
        strat.calculate_price_impact(Decimal("0.00001"))
    with pytest.raises(Exception, match=error_msg):
        strat.calculate_amount(Decimal("0.01"))
    with pytest.raises(Exception, match=error_msg):
        strat.take_liquidity(Decimal("0.00001"))


def test_volume_share_impact_with_zero_price_impact():
    strat = liquidity.VolumeShareImpact(price_impact=Decimal(0))
    strat.on_bar(
        bar.Bar(
            dt.utc_now(), pair.Pair("BTC", "USD"),
            Decimal("50000"), Decimal("70000"), Decimal("49900"), Decimal("69999.07"), Decimal("100")
        )
    )

    assert strat.available_liquidity == Decimal(25)
    assert strat.calculate_price_impact(Decimal(0)) == Decimal(0)
    assert strat.calculate_price_impact(Decimal(1)) == Decimal(0)
    assert strat.calculate_price_impact(Decimal(25)) == Decimal(0)
    with pytest.raises(Exception, match="Not enough liquidity"):
        strat.calculate_price_impact(Decimal(26))

    assert strat.calculate_amount(Decimal(0)) == Decimal(0)
    with pytest.raises(Exception, match="Not enough liquidity"):
        strat.calculate_amount(Decimal(1))

    assert strat.take_liquidity(Decimal(0)) == Decimal(0)
    assert strat.take_liquidity(Decimal(5)) == Decimal(0)
    assert strat.take_liquidity(Decimal(20)) == Decimal(0)
    with pytest.raises(Exception, match="Not enough liquidity"):
        strat.take_liquidity(Decimal(1))
