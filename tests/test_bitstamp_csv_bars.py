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

from decimal import Decimal
import asyncio
import datetime

from .helpers import abs_data_path
from basana.core.pair import Pair
from basana.external.bitstamp.csv import bars as csv_bars


def test_bars_from_csv(backtesting_dispatcher):
    bars = []

    async def on_bar(bar_event):
        print(bar_event.bar.datetime)
        bars.append(bar_event.bar)

    async def impl():
        pair = Pair("BTC", "USD")
        src = csv_bars.BarSource(pair, abs_data_path("bitstamp_btcusd_day_2015.csv"), csv_bars.BarPeriod.DAY)
        backtesting_dispatcher.subscribe(src, on_bar)
        await backtesting_dispatcher.run()

        assert len(bars) == 365 - 3  # There are 3 bars with no volume that are skipped.

        assert bars[0].open == Decimal(321)
        assert bars[0].high == Decimal(321)
        assert bars[0].low == Decimal("312.6")
        assert bars[0].close == Decimal("313.81")
        assert bars[0].volume == Decimal("3087.43655395")
        assert bars[-1].datetime == datetime.datetime(2015, 12, 31, 23, 59, 59, 999999, tzinfo=datetime.timezone.utc)
        assert bars[-1].open == Decimal("426.09")

    asyncio.run(impl())
