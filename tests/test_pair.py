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

from basana.core import pair
from basana.core.enums import PrecisionMode


def test_str():
    assert str(pair.Pair("BTC", "USD")) == "BTC/USD"


def test_eq():
    assert pair.Pair("BTC", "USD") == pair.Pair("BTC", "USD")
    assert pair.Pair("BTC", "USD") != pair.Pair("ARS", "USD")


def test_pair_info_decimal_places():
    pair_info = pair.PairInfo(2, 3)
    assert pair_info.precision_mode == PrecisionMode.DECIMAL_PLACES
    assert pair_info.precision_unit == "decimal digits"
    assert pair_info.truncate_base(Decimal("1.239")) == Decimal("1.23")
    assert pair_info.truncate_quote(Decimal("1.2399")) == Decimal("1.239")
    assert pair_info.round_base(Decimal("1.235")) == Decimal("1.24")
    assert pair_info.round_quote(Decimal("1.2395")) == Decimal("1.240")


def test_pair_info_significant_digits():
    pair_info = pair.PairInfo(5, 5, precision_mode=PrecisionMode.SIGNIFICANT_DIGITS)
    assert pair_info.precision_unit == "significant digits"
    assert pair_info.truncate_base(Decimal("1.23456789")) == Decimal("1.2345")
    assert pair_info.truncate_quote(Decimal("12345.678")) == Decimal("12345")
    assert pair_info.round_base(Decimal("1.23456789")) == Decimal("1.2346")
    assert pair_info.round_quote(Decimal("0.002395114")) == Decimal("0.0023951")


def test_pair_info_tick_size_requires_tick_sizes():
    with pytest.raises(
            ValueError, match="base_tick_size and quote_tick_size must be set when precision_mode is TICK_SIZE"
    ):
        pair.PairInfo(2, 2, precision_mode=PrecisionMode.TICK_SIZE)


@pytest.mark.parametrize("base_tick_size, quote_tick_size", [
    ("0", "0.05"),
    ("-0.05", "0.05"),
    ("0.05", "0"),
    ("0.05", "-0.05"),
])
def test_pair_info_tick_size_invalid(base_tick_size, quote_tick_size):
    with pytest.raises(ValueError, match="tick sizes must be > 0"):
        pair.PairInfo(
            2, 2,
            precision_mode=PrecisionMode.TICK_SIZE,
            base_tick_size=Decimal(base_tick_size),
            quote_tick_size=Decimal(quote_tick_size),
        )


def test_pair_info_tick_size():
    pair_info = pair.PairInfo(
        2, 2,
        precision_mode=PrecisionMode.TICK_SIZE,
        base_tick_size=Decimal("0.05"),
        quote_tick_size=Decimal("0.05"),
    )
    assert pair_info.precision_unit == "tick size"
    assert pair_info.base_tick_size == Decimal("0.05")
    assert pair_info.quote_tick_size == Decimal("0.05")

    assert pair_info.truncate_base(Decimal("1.2341")) == Decimal("1.2")
    assert pair_info.truncate_quote(Decimal("1.24")) == Decimal("1.2")
    assert pair_info.truncate_base(Decimal("1.25")) == Decimal("1.25")
    assert pair_info.truncate_quote(Decimal("1.257")) == Decimal("1.25")
    assert pair_info.truncate_base(Decimal("1.27")) == Decimal("1.25")
    assert pair_info.round_quote(Decimal("12.22")) == Decimal("12.2")
    assert pair_info.round_base(Decimal("12.24")) == Decimal("12.25")
    assert pair_info.round_quote(Decimal("12.25")) == Decimal("12.25")
    assert pair_info.round_base(Decimal("12.26")) == Decimal("12.25")
    assert pair_info.round_quote(Decimal("12.28")) == Decimal("12.3")
