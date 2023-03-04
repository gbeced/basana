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

from typing import List, Optional
import argparse
import datetime

import aiohttp
import asyncio

from basana.core import dt, token_bucket
from basana.external.bitstamp import client


def parse_date(date: str):
    return datetime.datetime.combine(
        datetime.date.fromisoformat(date), datetime.time()
    ).replace(tzinfo=datetime.timezone.utc)


class CSVWriter:
    def __init__(self):
        self._header_written = False

    def write_ohlc(self, ohlc: dict):
        if not self._header_written:
            print("datetime,open,high,low,close,volume")
            self._header_written = True
        print(",".join([
            str(datetime.datetime.utcfromtimestamp(int(ohlc["timestamp"]))),
            ohlc["open"], ohlc["high"], ohlc["low"], ohlc["close"], ohlc["volume"]
        ]))


async def main(params: Optional[List[str]] = None, config_overrides: dict = {}):
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--currency-pair", help="The currency pair.", required=True)
    parser.add_argument(
        "-p", "--period", help="The period for the bars.", choices=["min", "hour", "day"], required=True
    )
    parser.add_argument(
        "-s", "--start", help="The starting date YYYY-MM-DD format. Included in the range.", required=True
    )
    parser.add_argument(
        "-e", "--end", help="The ending date YYYY-MM-DD format. Included in the range.", required=True
    )
    args = parser.parse_args(args=params)

    step = {
        "min": 60,
        "hour": 3600,
        "day": 86400,
    }[args.period]

    start = parse_date(args.start)
    end = parse_date(args.end)
    assert start <= end, "Invalid start/end"
    end += datetime.timedelta(days=1)
    start = dt.to_utc_timestamp(start)
    end = dt.to_utc_timestamp(end)

    tb = token_bucket.TokenBucketLimiter(10, 1)
    writer = CSVWriter()
    async with aiohttp.ClientSession() as session:
        cli = client.APIClient(session=session, tb=tb, config_overrides=config_overrides)
        while start < end:
            # Not using 1000 as the limit because it is missing the first element for daily steps.
            response = await cli.get_ohlc_data(args.currency_pair, step, 999, start=start)
            for ohlc in response["data"]["ohlc"]:
                timestamp = int(ohlc["timestamp"])
                start = max(start, timestamp)
                if timestamp >= end:
                    continue
                writer.write_ohlc(ohlc)
            start += step


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
