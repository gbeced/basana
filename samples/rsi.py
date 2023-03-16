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

from talipp.indicators import RSI

import basana as bs


# Strategy based on RSI: https://www.investopedia.com/terms/r/rsi.asp
class Strategy(bs.TradingSignalSource):
    def __init__(self, dispatcher: bs.EventDispatcher, period: int, oversold_level: float, overbought_level: float):
        super().__init__(dispatcher)
        self._oversold_level = oversold_level
        self._overbought_level = overbought_level
        self._rsi = RSI(period=period)

    async def on_bar_event(self, bar_event: bs.BarEvent):
        # Feed the technical indicator.
        self._rsi.add_input_value(float(bar_event.bar.close))

        # Is the indicator ready ?
        if len(self._rsi) < 2:
            return

        # RSI crossed below oversold level
        if self._rsi[-2] >= self._oversold_level and self._rsi[-1] < self._oversold_level:
            self.push(bs.TradingSignal(bar_event.when, bs.OrderOperation.BUY, bar_event.bar.pair))
        # RSI crossed above overbought level
        elif self._rsi[-2] <= self._overbought_level and self._rsi[-1] > self._overbought_level:
            self.push(bs.TradingSignal(bar_event.when, bs.OrderOperation.SELL, bar_event.bar.pair))
