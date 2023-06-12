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
from basana.external.binance import client, helpers


period_to_step = {
    "1s": 1,
    "1m": 60,
    "3m": 3 * 60,
    "5m": 5 * 60,
    "15m": 15 * 60,
    "30m": 30 * 60,
    "1h": 3600,
    "2h": 2 * 3600,
    "4h": 4 * 3600,
    "6h": 6 * 3600,
    "8h": 8 * 3600,
    "12h": 12 * 3600,
    "1d": 86400,
    "3d": 3 * 86400,
    "1w": 7 * 86400,
    "1M": 31 * 86400,
}


def parse_date(date: str):
    return datetime.datetime.combine(
        datetime.date.fromisoformat(date), datetime.time()
    ).replace(tzinfo=datetime.timezone.utc)


class Candlestick:
    def __init__(self, candlestick: list):
        self.open_timestamp = candlestick[0]
        self.open = candlestick[1]
        self.high = candlestick[2]
        self.low = candlestick[3]
        self.close = candlestick[4]
        self.volume = candlestick[5]
        self.close_timestamp = candlestick[6]


def to_binance_currency_pair(currency_pair: str):
    parts = currency_pair.upper().split("/")
    return "".join(parts)


class CSVWriter:
    def __init__(self):
        self._header_written = False

    def write_candlestick(self, candlestick: Candlestick):
        if not self._header_written:
            print("datetime,open,high,low,close,volume")
            self._header_written = True
        print(",".join([
            str(datetime.datetime.utcfromtimestamp(candlestick.open_timestamp / 1000)),
            candlestick.open, candlestick.high, candlestick.low, candlestick.close, candlestick.volume
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

    start_ts = helpers.datetime_to_timestamp(start)
    past_the_end_ts = helpers.datetime_to_timestamp(past_the_end)
    step = step * 1000

    tb = token_bucket.TokenBucketLimiter(10, 1)
    now_ts = helpers.datetime_to_timestamp(dt.utc_now())
    writer = CSVWriter()
    async with aiohttp.ClientSession() as session:
        cli = client.APIClient(session=session, tb=tb, config_overrides=config_overrides)
        eof = False
        currency_pair = to_binance_currency_pair(args.currency_pair)
        while not eof:
            response = await cli.get_candlestick_data(
                currency_pair, args.period, start_time=start_ts, end_time=past_the_end_ts, limit=1000
            )
            eof = True
            for candlestick in response:
                eof = False
                candlestick = Candlestick(candlestick)
                start_ts = max(start_ts, candlestick.open_timestamp)
                if candlestick.open_timestamp >= past_the_end_ts or candlestick.close_timestamp >= now_ts:
                    continue
                writer.write_candlestick(candlestick)
            if not eof:
                start_ts += step
                eof = start_ts >= past_the_end_ts


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
