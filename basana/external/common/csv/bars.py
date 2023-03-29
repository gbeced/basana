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
from typing import Sequence
import datetime

from basana.core import pair, event, bar
from basana.core.event_sources import csv


class RowParser(csv.RowParser):
    def __init__(
            self, pair: pair.Pair, tzinfo: datetime.tzinfo, timedelta: datetime.timedelta
    ):
        self.pair = pair
        self.tzinfo = tzinfo
        self.timedelta = timedelta

    def parse_row(self, row_dict: dict) -> Sequence[event.Event]:
        # File format:
        #
        # datetime,open,high,low,close,volume
        # 2015-01-01 00:00:00,321,321,321,321,1.73697242

        volume = Decimal(row_dict["volume"])
        # Skip bars with no volume.
        if volume == 0:
            return []

        dt = datetime.datetime.strptime(row_dict["datetime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=self.tzinfo)
        return [
            bar.BarEvent(
                dt + self.timedelta,
                bar.Bar(
                    dt, self.pair, Decimal(row_dict["open"]), Decimal(row_dict["high"]), Decimal(row_dict["low"]),
                    Decimal(row_dict["close"]), volume
                )
            )
        ]
