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
import asyncio
import datetime

from .helpers import abs_data_path
from basana.core.pair import Pair
from basana.external.binance.csv import bars as csv_bars


def test_daily_bars_from_csv(backtesting_dispatcher):
    bars = []
    events = []

    async def on_bar(bar_event):
        bars.append(bar_event.bar)
        events.append(bar_event)

    async def impl():
        pair = Pair("BTC", "USDT")
        src = csv_bars.BarSource(pair, abs_data_path("binance_btcusdt_day_2020.csv"), "1d")
        backtesting_dispatcher.subscribe(src, on_bar)
        await backtesting_dispatcher.run()

        assert len(bars) == 365  # Removed Feb-29 just to get coverage over a 0 volume condition in the row parser.

        assert bars[0].open == Decimal("7195.24")
        assert bars[0].high == Decimal("7255")
        assert bars[0].low == Decimal("7175.15")
        assert bars[0].close == Decimal("7200.85")
        assert bars[0].volume == Decimal("16792.388165")
        assert bars[-1].datetime == datetime.datetime(2020, 12, 31, tzinfo=datetime.timezone.utc)
        assert bars[-1].open == Decimal("28875.55")
        assert events[-1].when == datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc)

    asyncio.run(impl())
