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

from decimal import Decimal
from typing import Any, List, Optional, Tuple
import asyncio
import datetime
import logging

from basana.core import dt, event, logs, pair


logger = logging.getLogger(__name__)


class InvalidBar(Exception):
    pass


class Bar:
    """A Bar, also known as candlestick or OHLC, is the summary of the trading activity in a given period.

    :param datetime: The beginning of the period. It must have timezone information set.
    :param pair: The trading pair.
    :param open: The opening price.
    :param high: The highest traded price.
    :param low: The lowest traded price.
    :param close: The closing price.
    :param volume: The volume traded.
    """

    def __init__(
            self, datetime: datetime.datetime, pair: pair.Pair,
            open: Decimal, high: Decimal, low: Decimal, close: Decimal, volume: Decimal
    ):
        if high < low:
            raise InvalidBar(f"high < low on {datetime}")
        elif high < open:
            raise InvalidBar(f"high < open on {datetime}")
        elif high < close:
            raise InvalidBar(f"high < close on {datetime}")
        elif low > open:
            raise InvalidBar(f"low > open on {datetime}")
        elif low > close:
            raise InvalidBar(f"low > close on {datetime}")

        #: The beginning of the period.
        self.datetime = datetime
        #: The trading pair.
        self.pair = pair
        #: The opening price.
        self.open = open
        #: The highest traded price.
        self.high = high
        #: The lowest traded price.
        self.low = low
        #: The closing price.
        self.close = close
        #: The volume traded.
        self.volume = volume


class BarEvent(event.Event):
    """An event for :class:`Bar` instances.

    :param when: The datetime when the event occurred. It must have timezone information set.
    :param bar: The bar.
    """

    def __init__(self, when: datetime.datetime, bar: Bar):
        super().__init__(when)

        #: The bar.
        self.bar = bar


class RealTimeTradesToBar(event.FifoQueueEventSource, event.Producer):
    def __init__(self, pair: pair.Pair, bar_duration: int, skip_first_bar: bool = True, flush_delay: float = 0.5):
        assert bar_duration > 0
        assert flush_delay >= 0
        super().__init__(producer=self)
        self._pair = pair
        self._bar_duration = bar_duration
        self._trades: List[Tuple[datetime.datetime, Decimal, Decimal]] = []
        self._skip_first_bar = skip_first_bar
        self._next_trade_ge: Optional[datetime.datetime] = None
        self._flush_delay = flush_delay

    def on_error(self, error: Any):
        logger.error(error)

    def push_trade(self, when: datetime.datetime, price: Decimal, amount: Decimal):
        # Trades must arrive in order.
        if self._next_trade_ge and when < self._next_trade_ge:
            self.on_error(logs.StructuredMessage(
                "Trade pushed out of order", last=self._next_trade_ge, current=when, pair=self._pair
            ))
            return

        self._trades.append((when, price, amount))
        self._next_trade_ge = when

    def _flush(self, begin: datetime.datetime, end: datetime.datetime):
        logger.debug(logs.StructuredMessage("Flushing", begin=begin, end=end, pair=self._pair))
        assert end > begin

        self._next_trade_ge = end if self._next_trade_ge is None else max(self._next_trade_ge, end)
        open = Decimal(0)
        high = Decimal(0)
        low = Decimal(0)
        close = Decimal(0)
        volume = Decimal(0)
        future_trades_begin = None

        # Calculate open, high, low, close and volume in the given window.
        for i, (when, price, amount) in enumerate(self._trades):
            if when < begin:
                self.on_error(logs.StructuredMessage(
                    "Trade is out of order", datetime=when, begin=begin, end=end, pair=self._pair
                ))
                continue
            # If the trade belongs to a future window, then we're done processing the current window.
            if when > end:
                future_trades_begin = i
                break

            open = price if not open else open
            high = price if not high else max(high, price)
            low = price if not low else min(low, price)
            close = price
            volume += amount

        # If there were trades in the current window then build the bar and publish the event.
        if volume and not self._skip_first_bar:
            bar = Bar(begin, self._pair, open, high, low, close, volume)
            self.push(BarEvent(end, bar))
        self._skip_first_bar = False

        # Dump the trades that were already processed
        self._trades = [] if future_trades_begin is None else self._trades[future_trades_begin:]

    async def main(self):
        now = dt.utc_now()
        begin = now - datetime.timedelta(seconds=now.timestamp() % self._bar_duration)
        end = begin + datetime.timedelta(seconds=self._bar_duration, milliseconds=-1)
        while True:
            sleep_time = (end - dt.utc_now()).total_seconds() + self._flush_delay
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            self._flush(begin, end)
            begin += datetime.timedelta(seconds=self._bar_duration)
            end += datetime.timedelta(seconds=self._bar_duration)
