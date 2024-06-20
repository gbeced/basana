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
from typing import Dict, Tuple

from basana.backtesting import config, errors, value_map
from basana.core import helpers as core_helpers
from basana.core.bar import Bar, BarEvent
from basana.core.pair import Pair


class Prices:
    def __init__(self, bid_ask_spread_pct: Decimal, config: config.Config):
        assert bid_ask_spread_pct > Decimal(0)

        self._bid_ask_spread_pct = bid_ask_spread_pct
        self._config = config
        self._last_bars: Dict[Pair, Bar] = {}

    def on_bar_event(self, event: BarEvent):
        self._last_bars[event.bar.pair] = event.bar

    def get_bid_ask(self, pair: Pair) -> Tuple[Decimal, Decimal]:
        last_bar = self._last_bars.get(pair)
        if not last_bar:
            raise errors.NoPrice(f"No price for {pair}")

        last_price = last_bar.close
        pair_info = self._config.get_pair_info(pair)
        half_spread = core_helpers.truncate_decimal(
            (last_price * self._bid_ask_spread_pct / Decimal("100")) / Decimal(2),
            pair_info.quote_precision
        )
        bid = last_price - half_spread
        ask = last_price + half_spread
        return bid, ask

    def get_price(self, pair: Pair) -> Decimal:
        last_bar = self._last_bars.get(pair)
        if not last_bar:
            raise errors.NoPrice(f"No price for {pair}")
        return last_bar.close

    def convert(self, amount: Decimal, from_symbol: str, to_symbol: str) -> Decimal:
        if amount == Decimal(0):
            return Decimal(0)

        for pair, price_fun in [
                (Pair(from_symbol, to_symbol), lambda price: price),
                (Pair(to_symbol, from_symbol), lambda price: Decimal(1) / price),
        ]:
            last_bar = self._last_bars.get(pair)
            if last_bar:
                return amount * price_fun(last_bar.close)
        raise errors.NoPrice(f"No price to convert from {from_symbol} to {to_symbol}")

    def convert_value_map(self, values: value_map.ValueMapDict, to_symbol: str) -> Decimal:
        ret = Decimal(0)
        for symbol, value in values.items():
            if symbol != to_symbol:
                value = self.convert(value, symbol, to_symbol)
            ret += value
        return ret
