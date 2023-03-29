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
from typing import Sequence, Tuple
import datetime

from dateutil import tz

from basana.core.event_sources import csv
from basana.core import pair, event, bar


######################################################################
# Yahoo Finance CSV parser
#
# File format:
# Date,Open,High,Low,Close,Volume,Adj Close
#
# The csv Date column must have the following format: YYYY-MM-DD


def adjust_ohlc(
        open: Decimal, high: Decimal, low: Decimal, close: Decimal, adj_close: Decimal
) -> Tuple[Decimal, Decimal, Decimal, Decimal]:
    adj_factor = adj_close / close
    open *= adj_factor
    high *= adj_factor
    low *= adj_factor
    close = adj_close
    return open, high, low, close


def sanitize_ohlc(open: Decimal, high: Decimal, low: Decimal, close: Decimal) -> Tuple[
    Decimal, Decimal, Decimal, Decimal
]:
    if low > open:
        low = open
    if low > close:
        low = close
    if high < open:
        high = open
    if high < close:
        high = close
    return open, high, low, close


class RowParser(csv.RowParser):
    def __init__(
            self, pair: pair.Pair, adjust_ohlc: bool = False, tzinfo: datetime.tzinfo = tz.tzlocal(),
            timedelta: datetime.timedelta = datetime.timedelta(hours=24, microseconds=-1)
    ):
        self.pair = pair
        self.tzinfo = tzinfo
        self.timedelta = timedelta
        self.sanitize = False
        self.adjust_ohlc = adjust_ohlc

    def parse_row(self, row_dict: dict) -> Sequence[event.Event]:
        dt = datetime.datetime.strptime(row_dict["Date"], "%Y-%m-%d").replace(tzinfo=self.tzinfo)
        open, high, low, close = (
            Decimal(row_dict["Open"]), Decimal(row_dict["High"]), Decimal(row_dict["Low"]), Decimal(row_dict["Close"])
        )
        if self.sanitize:
            open, high, low, close = sanitize_ohlc(open, high, low, close)
        if self.adjust_ohlc:
            open, high, low, close = adjust_ohlc(open, high, low, close, Decimal(row_dict["Adj Close"]))

        return [
            bar.BarEvent(
                dt + self.timedelta,
                bar.Bar(dt, self.pair, open, high, low, close, Decimal(row_dict["Volume"]))
            )
        ]


class CSVBarSource(csv.EventSource):
    def __init__(
            self, pair: pair.Pair, csv_path: str, adjust_ohlc: bool = False, sort: bool = True,
            tzinfo: datetime.tzinfo = tz.tzlocal(),
            timedelta: datetime.timedelta = datetime.timedelta(hours=24, microseconds=-1),
            dict_reader_kwargs: dict = {}
    ):
        self.row_parser = RowParser(pair, adjust_ohlc=adjust_ohlc, tzinfo=tzinfo, timedelta=timedelta)
        super().__init__(csv_path, self.row_parser, sort=sort, dict_reader_kwargs=dict_reader_kwargs)
