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

from talipp.indicators import BB

import basana as bs


# Strategy based on Bollinger Bands: https://www.investopedia.com/articles/trading/07/bollinger.asp
class Strategy(bs.TradingSignalSource):
    def __init__(self, dispatcher: bs.EventDispatcher, period: int, std_dev_multiplier: float):
        super().__init__(dispatcher)
        self.bb = BB(period, std_dev_multiplier)
        self._values = (None, None)

    async def on_bar_event(self, bar_event: bs.BarEvent):
        # Feed the technical indicator.
        value = float(bar_event.bar.close)
        self.bb.add_input_value(value)

        # Keep a small window of values to check if there is a crossover.
        self._values = (self._values[-1], value)

        # Is the indicator ready ?
        if len(self.bb) < 2:
            return

        # Price moved below lower band ?
        if self._values[-2] >= self.bb[-2].lb and self._values[-1] < self.bb[-1].lb:
            self.push(bs.TradingSignal(bar_event.when, bs.OrderOperation.BUY, bar_event.bar.pair))
        # Price moved above upper band ?
        elif self._values[-2] <= self.bb[-2].ub and self._values[-1] > self.bb[-1].ub:
            self.push(bs.TradingSignal(bar_event.when, bs.OrderOperation.SELL, bar_event.bar.pair))
