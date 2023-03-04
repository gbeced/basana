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

from dateutil import tz
import pytest

from . import helpers
from basana.core import dispatcher, pair, bar
from basana.core.helpers import round_decimal
from basana.external.yahoo import bars


bars_to_sanitize = [
    # Open < Low
    {
        "Date": "2000-12-29",
        "Open": "1.87",
        "High": "31.31",
        "Low": "28.69",
        "Close": "29.06",
        "Volume": "31655500",
        "Adj Close": "28.41",
    },
    # Open > High
    {
        "Date": "2000-12-29",
        "Open": "40.87",
        "High": "31.31",
        "Low": "28.69",
        "Close": "29.06",
        "Volume": "31655500",
        "Adj Close": "28.41",
    },
    # High < Low
    {
        "Date": "2000-12-29",
        "Open": "10",
        "High": "1",
        "Low": "20",
        "Close": "10",
        "Volume": "31655500",
        "Adj Close": "28.41",
    },
    # High < Close
    {
        "Date": "2000-12-29",
        "Open": "30.87",
        "High": "31.31",
        "Low": "28.69",
        "Close": "60.06",
        "Volume": "31655500",
        "Adj Close": "28.41",
    },
    # Low > Close
    {
        "Date": "2000-12-29",
        "Open": "30.87",
        "High": "31.31",
        "Low": "28.69",
        "Close": "27.06",
        "Volume": "31655500",
        "Adj Close": "28.41",
    },
]


def test_multiple_sources():
    d = dispatcher.EventDispatcher()
    event_bars = []

    async def add_bar(event: bar.BarEvent):
        event_bars.append(event.bar)

    src_1 = bars.CSVBarSource(
        pair.Pair("ORCL", "USD"), helpers.abs_data_path("orcl-2000-yahoo-sorted.csv"), sort=False,
        tzinfo=datetime.timezone.utc
    )
    src_2 = bars.CSVBarSource(
        pair.Pair("ORCL", "USD"), helpers.abs_data_path("orcl-2001-yahoo.csv"), adjust_ohlc=True
    )

    d.subscribe(src_1, add_bar)
    d.subscribe(src_2, add_bar)

    asyncio.run(d.run())

    assert event_bars[0].datetime == datetime.datetime(
        2000, 1, 3, hour=23, minute=59, second=59, microsecond=999999, tzinfo=datetime.timezone.utc
    )
    assert event_bars[0].pair.base_symbol == "ORCL"
    assert event_bars[0].pair.quote_symbol == "USD"
    assert event_bars[0].open == Decimal("124.62")
    assert event_bars[0].high == Decimal("125.19")
    assert event_bars[0].low == Decimal("111.62")
    assert event_bars[0].close == Decimal("118.12")
    assert event_bars[0].volume == Decimal("98122000")

    assert event_bars[-1].datetime == datetime.datetime(
        2001, 12, 31, hour=23, minute=59, second=59, microsecond=999999, tzinfo=tz.tzlocal()
    )
    assert round_decimal(event_bars[-1].open, 2) == Decimal("13.78")
    assert round_decimal(event_bars[-1].high, 2) == Decimal("13.91")
    assert round_decimal(event_bars[-1].low, 2) == Decimal("13.49")
    assert event_bars[-1].close == Decimal("13.50")


@pytest.mark.parametrize("row_dict", bars_to_sanitize)
def test_bars_need_sanitization(row_dict):
    row_parser = bars.RowParser(pair.Pair("ORCL", "USD"))
    with pytest.raises(bar.InvalidBar):
        row_parser.parse_row(row_dict)


@pytest.mark.parametrize("row_dict", bars_to_sanitize)
def test_row_parser_sanitization(row_dict):
    row_parser = bars.RowParser(pair.Pair("ORCL", "USD"))
    row_parser.sanitize = True
    row_parser.parse_row(row_dict)
