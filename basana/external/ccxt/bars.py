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

from typing import Any, Dict, List, Optional, Union
import asyncio
import datetime
import logging

from . import helpers
from basana.core import bar, event, logs
from basana.core.pair import Pair


logger = logging.getLogger(__name__)


class Candle:
    def __init__(self, ohlcv: List[Union[int, float, str]]):
        self.raw = ohlcv
        self.timestamp = int(ohlcv[0])
        # Smaller timestamps, and smaller volumes go first
        self.sort_key = (self.timestamp, helpers.to_decimal(self.raw[5]))

    def to_bar(self, pair: Pair, duration: datetime.timedelta):
        begin = helpers.timestamp_to_datetime(self.timestamp)
        return bar.Bar(
            begin, pair,
            helpers.to_decimal(self.raw[1]), helpers.to_decimal(self.raw[2]),
            helpers.to_decimal(self.raw[3]), helpers.to_decimal(self.raw[4]),
            helpers.to_decimal(self.raw[5]), duration
        )


class WatchOHLCVEventSource(event.FifoQueueEventSource, event.Producer):
    def __init__( self, cli: Any, pair: Pair, timeframe: str, params: Optional[Dict[str, Any]] = None):
        if timeframe not in cli.timeframes:
            raise ValueError(f"Invalid bar_duration: {timeframe}")

        if not cli.has.get("watchOHLCV"):
            raise NotImplementedError("The exchange does not support watchOHLCV")

        super().__init__(producer=self)
        self._cli = cli
        self._pair = pair
        self._symbol = helpers.pair_to_symbol(pair)
        self._timeframe = timeframe
        self._params = dict(params or {})
        self._duration_secs = self._cli.parse_timeframe(self._timeframe)
        self._last_candle: Optional[Candle] = None

    async def initialize(self):
        await self._cli.load_markets()

    async def main(self):
        while True:
            try:
                ohlcv = await self._cli.watch_ohlcv(self._symbol, self._timeframe, params=self._params)
                self._handle_ohlcv(ohlcv)
            except Exception as e:
                logger.error(logs.StructuredMessage(
                    "Error watching OHLCV", pair=self._pair, timeframe=self._timeframe, error=e
                ))
            # Yield to the event loop to allow other tasks to run.
            await asyncio.sleep(0)

    async def finalize(self):
        if not self._cli.has.get("unWatchOHLCV"):
            return

        try:
            await self._cli.un_watch_ohlcv(self._symbol, self._timeframe)
        except Exception as error:
            logger.error(logs.StructuredMessage(
                "Error unwatching OHLCV", pair=self._pair, timeframe=self._timeframe, error=error
            ))

    def _handle_ohlcv(self, ohlcv: list):
        candles = list(map(Candle, ohlcv))
        candles.sort(key=lambda candle: candle.sort_key)
        for candle in candles:
            if self._last_candle is None:
                self._last_candle = candle
            elif candle.timestamp > self._last_candle.timestamp:
                self._push_last()
                self._last_candle = candle
            elif candle.timestamp == self._last_candle.timestamp:
                self._last_candle = candle
            else:
                logger.error(logs.StructuredMessage(
                    "Ignoring out of order candle", previous=self._last_candle.raw, received=candle.raw
                ))

    def _push_last(self):
        assert self._last_candle is not None

        candle_bar = self._last_candle.to_bar(self._pair, datetime.timedelta(seconds=self._duration_secs))
        when = candle_bar.begin + candle_bar.duration
        self.push(bar.BarEvent(when, candle_bar))
