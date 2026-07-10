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
from typing import Optional
import dataclasses

from basana.core import helpers
from basana.core.enums import PrecisionMode


@dataclasses.dataclass(frozen=True)
class Pair:
    """A trading pair.

    :param base_symbol: The base symbol. It could be a stock, a crypto currency, a currency, etc.
    :param quote_symbol: The quote symbol. It could be a stock, a crypto currency, a currency, etc.
    """

    #: The base symbol.
    base_symbol: str

    #: The quote symbol.
    quote_symbol: str

    def __str__(self):
        return "{}/{}".format(self.base_symbol, self.quote_symbol)


@dataclasses.dataclass(frozen=True)
class PairInfo:
    """Information about a trading pair.

    :param base_precision: The precision for the base symbol.
    :param quote_precision: The precision for the quote symbol.
    :param precision_mode: The precision mode.
    :param base_tick_size: The tick size for the base symbol, if precision_mode is TICK_SIZE.
    :param quote_tick_size: The tick size for the quote symbol, if precision_mode is TICK_SIZE.
    """

    #: The precision for the base symbol.
    base_precision: int

    #: The precision for the quote symbol.
    quote_precision: int

    #: The precision mode.
    precision_mode: PrecisionMode = dataclasses.field(
        default=PrecisionMode.DECIMAL_PLACES, kw_only=True
    )

    #: The tick size for the base symbol.
    base_tick_size: Optional[Decimal] = dataclasses.field(default=None, kw_only=True)

    #: The tick size for the quote symbol.
    quote_tick_size: Optional[Decimal] = dataclasses.field(default=None, kw_only=True)

    def __post_init__(self):
        if self.precision_mode == PrecisionMode.TICK_SIZE:
            if self.base_tick_size is None or self.quote_tick_size is None:
                raise ValueError("base_tick_size and quote_tick_size must be set when precision_mode is TICK_SIZE")
            if self.base_tick_size <= 0 or self.quote_tick_size <= 0:
                raise ValueError("tick sizes must be > 0")

    @property
    def precision_unit(self) -> str:
        if self.precision_mode == PrecisionMode.SIGNIFICANT_DIGITS:
            return "significant digits"
        if self.precision_mode == PrecisionMode.TICK_SIZE:
            return "tick size"
        return "decimal digits"

    def truncate_base(self, value: Decimal) -> Decimal:
        return helpers.truncate_with_precision(
            value, self.base_precision, self.precision_mode, tick_size=self.base_tick_size
        )

    def truncate_quote(self, value: Decimal) -> Decimal:
        return helpers.truncate_with_precision(
            value, self.quote_precision, self.precision_mode, tick_size=self.quote_tick_size
        )

    def round_base(self, value: Decimal, rounding=None) -> Decimal:
        return helpers.round_with_precision(
            value, self.base_precision, self.precision_mode, rounding=rounding, tick_size=self.base_tick_size
        )

    def round_quote(self, value: Decimal, rounding=None) -> Decimal:
        return helpers.round_with_precision(
            value, self.quote_precision, self.precision_mode, rounding=rounding, tick_size=self.quote_tick_size
        )