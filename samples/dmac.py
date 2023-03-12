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

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

from talipp.indicators import EMA

import basana as bs


# Strategy based on Dual Moving Average Crossover.
class Strategy(bs.TradingSignalSource):
    def __init__(self, dispatcher: bs.EventDispatcher, short_term_period: int, long_term_period: int):
        super().__init__(dispatcher)
        self._st_sma = EMA(period=short_term_period)
        self._lt_sma = EMA(period=long_term_period)

    async def on_bar_event(self, bar_event: bs.BarEvent):
        # Feed the technical indicator.
        value = float(bar_event.bar.close)
        self._st_sma.add_input_value(value)
        self._lt_sma.add_input_value(value)

        # Are MAs ready ?
        if len(self._st_sma) < 2 or len(self._lt_sma) < 2:
            return

        # Short term MA crossed above long term MA ?
        if self._st_sma[-2] <= self._lt_sma[-2] and self._st_sma[-1] > self._lt_sma[-1]:
            self.push(bs.TradingSignal(bar_event.when, bs.OrderOperation.BUY, bar_event.bar.pair))
        # Short term MA crossed below long term MA ?
        elif self._st_sma[-2] >= self._lt_sma[-2] and self._st_sma[-1] < self._lt_sma[-1]:
            self.push(bs.TradingSignal(bar_event.when, bs.OrderOperation.SELL, bar_event.bar.pair))
