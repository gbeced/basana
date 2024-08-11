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

from typing import Optional
import logging

from statsmodels.tsa import stattools
import pandas as pd

from basana.core.logs import StructuredMessage
import basana as bs


def get_p_value(values_1, values_2):
    return stattools.coint(values_1, values_2)[1]


# Strategy based on https://notebook.community/gwulfs/research_public/lectures/pairs_trading/Pairs%20Trading
class Strategy(bs.TradingSignalSource):
    def __init__(
            self, dispatcher: bs.EventDispatcher, pair_1: bs.Pair, pair_2: bs.Pair, window_size: int,
            z_score_window_size: int, p_value_threshold: float, z_score_entry_ge: float, z_score_exit_lt: float
    ):
        assert z_score_window_size <= window_size

        super().__init__(dispatcher)

        self._df = pd.DataFrame()
        self._pair_1 = pair_1
        self._pair_2 = pair_2
        self._window_size = window_size
        self._z_score_window_size = z_score_window_size
        self._p_value_threshold = p_value_threshold
        self._z_score_entry_ge = z_score_entry_ge
        self._z_score_exit_lt = z_score_exit_lt
        self._p_value: Optional[float] = None
        self._z_score: Optional[float] = None
        self._last_position: Optional[bs.Position] = None

    @property
    def p_value(self) -> Optional[float]:
        return self._p_value

    @property
    def z_score(self) -> Optional[float]:
        return self._z_score

    async def on_bar_event(self, bar_event: bs.BarEvent):
        bar = bar_event.bar
        assert bar.pair in (self._pair_1, self._pair_2)

        # Update indicators.
        self._update_df(bar_event)
        if not self._df_ready():
            return
        self._update_indicators()
        logging.debug(StructuredMessage("Indicators updated", p_value=self.p_value, z_score=self.z_score))

        # Calculate target position. If series are no longer cointegrated, switch to a neutral position.
        if self.p_value < self._p_value_threshold:
            target_position = self._get_target_position()
        else:
            target_position = bs.Position.NEUTRAL

        # Create and push an event if a change in position took place. If we're just starting up, push an event to
        # notify were you should be standing.
        if target_position and target_position != self._last_position:
            self._last_position = target_position
            signal = bs.TradingSignal(bar_event.when, target_position, self._pair_1)
            signal.add_pair(
                self._pair_2,
                {
                    bs.Position.LONG: bs.Position.SHORT,
                    bs.Position.SHORT: bs.Position.LONG,
                    bs.Position.NEUTRAL: bs.Position.NEUTRAL,
                }[target_position]
            )
            self.push(signal)

    def _get_target_position(self) -> Optional[bs.Position]:
        ret = None
        if abs(self.z_score) >= self._z_score_entry_ge:
            ret = bs.Position.SHORT if self.z_score > 0 else bs.Position.LONG
        elif abs(self.z_score) < self._z_score_exit_lt:
            ret = bs.Position.NEUTRAL
        return ret

    def _update_df(self, bar_event: bs.BarEvent):
        self._df.at[bar_event.bar.datetime, bar_event.bar.pair.base_symbol] = float(bar_event.bar.close)
        self._df = self._df.iloc[-self._window_size:]

    def _df_ready(self):
        if len(self._df.columns) != 2:
            return False
        if len(self._df) != self._window_size:
            return False
        if self._df.isnull().values.any():
            return False
        return True

    def _update_indicators(self):
        values_1 = self._df[self._pair_1.base_symbol]
        values_2 = self._df[self._pair_2.base_symbol]
        self._p_value = get_p_value(values_1, values_2)

        ratios = values_1[-self._z_score_window_size:] / values_2[-self._z_score_window_size:]
        self._z_score = (ratios.iloc[-1] - ratios.mean()) / ratios.std()
