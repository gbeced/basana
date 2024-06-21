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

import asyncio
import re

import aioresponses
import pytest

from basana.external.bitstamp.tools import download_bars


@pytest.fixture()
def bitstamp_http_api_mock():
    with aioresponses.aioresponses() as m:
        m.get(re.compile(r'http://bitstamp.mock/api/v2/ohlc/btcusd/.*'), status=200, payload={
            "data": {
                "ohlc": [
                    {
                        "close": "433.82", "high": "436", "low": "427.2", "open": "430.89",
                        "timestamp": "1451606400", "volume": "3788.11117403"
                    },
                    {
                        "close": "433.55", "high": "435.99", "low": "430.42", "open": "434.87",
                        "timestamp": "1451692800", "volume": "2972.06344935"
                    },
                    {
                        "close": "431.04", "high": "434.09", "low": "424.06", "open": "433.2",
                        "timestamp": "1451779200", "volume": "4571.09703841"
                    }
                ],
                "pair": "BTC/USD"
            }
        })
        yield m


def test_download_ohlc(bitstamp_http_api_mock, capsys):
    async def test_main():
        await download_bars.main(
            params=["-c", "btcusd", "-p", "day", "-s", "2016-01-01", "-e", "2016-01-01"],
            config_overrides={"api": {"http": {"base_url": "http://bitstamp.mock/"}}}
        )
        assert capsys.readouterr().out == """datetime,open,high,low,close,volume
2016-01-01 00:00:00,430.89,436,427.2,433.82,3788.11117403
"""

    asyncio.run(test_main())
