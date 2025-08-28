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

from basana.backtesting.value_map import ValueMap


@pytest.mark.parametrize("lhs, rhs, expected_result", [
    ({}, {}, {}),
    (
        {"BTC": Decimal("1.1"), "USD": Decimal("1")},
        {"BTC": Decimal("1.1"), "ETH": Decimal("3")},
        {"BTC": Decimal("2.2"), "USD": Decimal("1"), "ETH": Decimal("3")},
    ),
])
def test_add(lhs, rhs, expected_result):
    assert (ValueMap(lhs) + rhs) == expected_result
    assert (lhs + ValueMap(rhs)) == expected_result

    res = ValueMap(lhs)
    res += rhs
    assert res == expected_result


@pytest.mark.parametrize("lhs, rhs, expected_result", [
    ({}, {}, {}),
    (
        {"BTC": Decimal("1.1"), "USD": Decimal("1")},
        {"BTC": Decimal("1.1"), "ETH": Decimal("3")},
        {"BTC": Decimal("0"), "USD": Decimal("1"), "ETH": Decimal("-3")},
    ),
])
def test_sub(lhs, rhs, expected_result):
    assert (ValueMap(lhs) - rhs) == expected_result
    assert (lhs - ValueMap(rhs)) == expected_result

    res = ValueMap(lhs)
    res -= rhs
    assert res == expected_result


@pytest.mark.parametrize("lhs, rhs, expected_result", [
    ({}, {}, {}),
    (
        {"BTC": Decimal("-1.1"), "USD": Decimal("1")},
        {"BTC": Decimal("3"), "ETH": Decimal("3")},
        {"BTC": Decimal("-3.3"), "USD": Decimal("0"), "ETH": Decimal("0")},
    ),
])
def test_mul(lhs, rhs, expected_result):
    assert (ValueMap(lhs) * rhs) == expected_result
    assert (lhs * ValueMap(rhs)) == expected_result

    res = ValueMap(lhs)
    res *= rhs
    assert res == expected_result


@pytest.mark.parametrize("lhs, rhs, expected_result", [
    ({}, {}, {}),
    (
        {"BTC": Decimal("-3")},
        {"BTC": Decimal("2"), "ETH": Decimal("3")},
        {"BTC": Decimal("-1.5"), "ETH": Decimal("0")},
    ),
    (
        {},
        {"BTC": Decimal("0"), "ETH": Decimal("3")},
        {"BTC": Decimal("0"), "ETH": Decimal("0")},
    ),
    (
        {"BTC": Decimal("0")},
        {"BTC": Decimal("0"), "ETH": Decimal("3")},
        {"BTC": Decimal("0"), "ETH": Decimal("0")},
    ),

])
def test_div(lhs, rhs, expected_result):
    # div
    assert (ValueMap(lhs) / rhs) == expected_result
    # rdiv
    assert (lhs / ValueMap(rhs)) == expected_result
    # idiv
    res = ValueMap(lhs)
    res /= rhs
    assert res == expected_result


@pytest.mark.parametrize("lhs, rhs", [
    (
        {"BTC": Decimal(1)},
        {}
    ),
    (
        {"ETH": Decimal(1)},
        {"ETH": Decimal(0)}
    ),
])
def test_zero_div_error(lhs, rhs):
    # div
    with pytest.raises(ZeroDivisionError):
        _ = ValueMap(lhs) / rhs
    # rdiv
    with pytest.raises(ZeroDivisionError):
        _ = lhs / ValueMap(rhs)
    # idiv
    with pytest.raises(ZeroDivisionError):
        res = ValueMap(lhs)
        res /= rhs


def test_prune():
    values = ValueMap({
        "BTC": Decimal(1),
        "USD": Decimal(1),
        "ETH": Decimal(1),
    })

    values.prune()
    assert values == {
        "BTC": Decimal(1),
        "USD": Decimal(1),
        "ETH": Decimal(1),
    }

    values["ETH"] = Decimal(0)
    assert values == {
        "BTC": Decimal(1),
        "USD": Decimal(1),
        "ETH": Decimal(0),
    }

    values.prune()
    assert values == {
        "BTC": Decimal(1),
        "USD": Decimal(1),
    }
