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

from basana.external.binance.tools import download_bars


@pytest.fixture()
def binance_http_api_mock():
    with aioresponses.aioresponses() as m:
        yield m


def test_download_ohlc(binance_http_api_mock, capsys):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/klines\\?.*"),
        status=200,
        payload=[
            [
                1577836800000,
                "7195.24000000",
                "7255.00000000",
                "7175.15000000",
                "7200.85000000",
                "16792.38816500",
                1577923199999,
                "121214452.11606228",
                194010,
                "8946.95553500",
                "64597785.21233434",
                "0"
            ],
            [
                1577923200000,
                "7200.77000000",
                "7212.50000000",
                "6924.74000000",
                "6965.71000000",
                "31951.48393200",
                1578009599999,
                "225982341.30114030",
                302667,
                "15141.61134000",
                "107060829.07806464",
                "0"
            ],
            # This one closes in the future and should be skipped.
            [
                1577836800000,
                "7195.24000000",
                "7255.00000000",
                "7175.15000000",
                "7200.85000000",
                "16792.38816500",
                2538693277999,
                "121214452.11606228",
                194010,
                "8946.95553500",
                "64597785.21233434",
                "0"
            ],

        ]
    )

    async def test_main():
        await download_bars.main(
            params=["-c", "BTCUSDT", "-p", "1d", "-s", "2020-01-01", "-e", "2020-01-01"],
            config_overrides={"api": {"http": {"base_url": "http://binance.mock/"}}}
        )
        assert capsys.readouterr().out == """datetime,open,high,low,close,volume
2020-01-01 00:00:00,7195.24000000,7255.00000000,7175.15000000,7200.85000000,16792.38816500
"""

    asyncio.run(test_main())
