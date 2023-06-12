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

from typing import List, Optional
import argparse
import datetime

import aiohttp
import asyncio

from basana.core import dt, token_bucket
from basana.external.bitstamp import client


period_to_step = {
    "min": 60,
    "hour": 3600,
    "day": 86400,
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "12h": 43200,
    "1d": 86400,
    "3d": 259200,
}


class OHLC:
    def __init__(self, ohlc: dict):
        self.open_timestamp = int(ohlc["timestamp"])
        self.open = ohlc["open"]
        self.high = ohlc["high"]
        self.low = ohlc["low"]
        self.close = ohlc["close"]
        self.volume = ohlc["volume"]


def parse_date(date: str):
    return datetime.datetime.combine(
        datetime.date.fromisoformat(date), datetime.time()
    ).replace(tzinfo=datetime.timezone.utc)


def to_bitstamp_currency_pair(currency_pair: str):
    parts = currency_pair.lower().split("/")
    return "".join(parts)


class CSVWriter:
    def __init__(self):
        self._header_written = False

    def write_ohlc(self, ohlc: OHLC):
        if not self._header_written:
            print("datetime,open,high,low,close,volume")
            self._header_written = True
        print(",".join([
            str(datetime.datetime.utcfromtimestamp(ohlc.open_timestamp)),
            ohlc.open, ohlc.high, ohlc.low, ohlc.close, ohlc.volume
        ]))


async def main(params: Optional[List[str]] = None, config_overrides: dict = {}):
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--currency-pair", help="The currency pair.", required=True)
    parser.add_argument(
        "-p", "--period", help="The period for the bars.", choices=period_to_step.keys(), required=True
    )
    parser.add_argument(
        "-s", "--start", help="The starting date YYYY-MM-DD format. Included in the range.", required=True
    )
    parser.add_argument(
        "-e", "--end", help="The ending date YYYY-MM-DD format. Included in the range.", required=True
    )
    args = parser.parse_args(args=params)

    step = period_to_step[args.period]
    start = parse_date(args.start)
    end = parse_date(args.end)
    assert start <= end, "Invalid start/end"
    # start/end are set as dates so to get past the end we need to use a step >= 1 day.
    past_the_end = end + datetime.timedelta(seconds=max(period_to_step["1d"], step))

    start_ts = dt.to_utc_timestamp(start)
    past_the_end_ts = dt.to_utc_timestamp(past_the_end)

    tb = token_bucket.TokenBucketLimiter(10, 1)
    writer = CSVWriter()
    async with aiohttp.ClientSession() as session:
        cli = client.APIClient(session=session, tb=tb, config_overrides=config_overrides)
        eof = False
        currency_pair = to_bitstamp_currency_pair(args.currency_pair)
        while not eof:
            response = await cli.get_ohlc_data(currency_pair, step, 1000, start=start_ts, exclude_current_candle=True)
            eof = True
            for ohlc in response["data"]["ohlc"]:
                eof = False
                ohlc = OHLC(ohlc)
                start_ts = max(start_ts, ohlc.open_timestamp)
                if ohlc.open_timestamp >= past_the_end_ts:
                    continue
                writer.write_ohlc(ohlc)

            if not eof:
                start_ts += step
                eof = start_ts >= past_the_end_ts


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
