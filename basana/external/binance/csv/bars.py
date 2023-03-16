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

import datetime

from basana.core import pair
from basana.core.event_sources import csv
from basana.external.binance.tools.download_bars import period_to_step
from basana.external.common.csv.bars import RowParser


period_to_timedelta = {
    period_str: datetime.timedelta(seconds=period_secs, microseconds=-1)
    for period_str, period_secs in period_to_step.items()
}


class BarSource(csv.EventSource):
    def __init__(
            self, pair: pair.Pair, csv_path: str, period: str,
            sort: bool = False, tzinfo: datetime.tzinfo = datetime.timezone.utc,
            dict_reader_kwargs: dict = {}
    ):
        # The datetime in the files are the beginning of the period but we need to generate the event at the period's
        # end.
        timedelta = period_to_timedelta.get(period)
        assert timedelta is not None, "Invalid period"
        self.row_parser = RowParser(pair, tzinfo=tzinfo, timedelta=timedelta)
        super().__init__(csv_path, self.row_parser, sort=sort, dict_reader_kwargs=dict_reader_kwargs)
