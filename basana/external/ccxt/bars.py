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

from typing import Any, Dict, List, Optional
import asyncio
import datetime
import logging

from . import helpers
from basana.core import bar, event, logs
from basana.core.pair import Pair


logger = logging.getLogger(__name__)



class WatchOHLCVEventSource(event.FifoQueueEventSource, event.Producer):
    def __init__(
            self, cli: Any, pair: Pair, timeframe: str, skip_first_bar: bool = True,
            params: Optional[Dict[str, Any]] = None
    ):
        if timeframe not in cli.timeframes:
            raise ValueError(f"Invalid bar_duration: {timeframe}")

        if not cli.has.get("watchOHLCV"):
            raise NotImplementedError("The exchange does not support watchOHLCV")

        super().__init__(producer=self)
        self._cli = cli
        self._pair = pair
        self._symbol = helpers.pair_to_symbol(pair)
        self._timeframe = timeframe
        self._skip_first_bar = skip_first_bar
        self._params = dict(params or {})
        self._duration_secs: Optional[int] = None
        self._last_ohlcv: Optional[helpers.Candle] = None

    async def initialize(self):
        await self._cli.load_markets()
        self._duration_secs = self._cli.parse_timeframe(self._timeframe)

    async def main(self):
        while True:
            try:
                ohlcv = await self._cli.watch_ohlcv(self._symbol, self._timeframe, params=self._params)
                ohlcv.reverse()
                self._handle_ohlcv(ohlcv)
            except Exception as e:
                logger.error(logs.StructuredMessage(
                    "Error watching OHLCV", pair=self._pair, timeframe=self._timeframe, error=e
                ))
            await asyncio.sleep(0)

    async def finalize(self):
        if not self._cli.has.get("unWatchOHLCV"):
            return

        try:
            await self._cli.un_watch_ohlcv(self._symbol, self._timeframe, params=self._params)
        except Exception as error:
            logger.error(logs.StructuredMessage(
                "Error unwatching OHLCV", pair=self._pair, timeframe=self._timeframe, error=error
            ))

    def _handle_ohlcv(self, ohlcv: List[helpers.Candle]):
        for item in ohlcv:
            if self._last_ohlcv and item[0] != self._last_ohlcv[0]:
                self._push_last()
            self._last_ohlcv = item

    def _push_last(self):
        assert self._last_ohlcv is not None

        if self._skip_first_bar:
            self._skip_first_bar = False
        else:
            candle_bar = helpers.ohlcv_to_bar(self._pair, self._last_ohlcv, self._duration_secs)
            when = candle_bar.begin + datetime.timedelta(seconds=self._duration_secs)
            self.push(bar.BarEvent(when, candle_bar))
