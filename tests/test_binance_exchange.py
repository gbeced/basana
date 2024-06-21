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
import re

import aiohttp

from . import helpers
from basana.core import pair
from basana.external.binance import exchange


DEPTH_RESPONSE = {
    "lastUpdateId": 27229732069,
    "bids": [
        [
            "16757.47000000",
            "0.04893000"
        ],
        [
            "16757.41000000",
            "0.00073000"
        ],
        [
            "16756.52000000",
            "0.00690000"
        ]
    ],
    "asks": [
        [
            "16758.13000000",
            "0.00682000"
        ],
        [
            "16758.55000000",
            "0.04963000"
        ],
        [
            "16759.25000000",
            "0.00685000"
        ]
    ]
}


def test_bid_ask(binance_http_api_mock, binance_exchange):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/depth\\?.*"), status=200,
        payload=DEPTH_RESPONSE
    )

    async def test_main():
        bid, ask = await binance_exchange.get_bid_ask(pair.Pair("BTC", "USDT"))
        assert bid == Decimal("16757.47")
        assert ask == Decimal("16758.13")

    asyncio.run(test_main())


def test_pair_info_explicit_session(binance_http_api_mock, realtime_dispatcher):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/exchangeInfo\\?.*"), status=200,
        payload=helpers.load_json("binance_btc_usdt_exchange_info.json")
    )

    async def test_main():
        async with aiohttp.ClientSession() as session:
            e = exchange.Exchange(
                realtime_dispatcher, "api_key", "api_secret", session=session,
                config_overrides={"api": {"http": {"base_url": "http://binance.mock/"}}}
            )

            pair_info = await e.get_pair_info(pair.Pair("BTC", "USDT"))
            assert pair_info.base_precision == 5
            assert pair_info.quote_precision == 2
            assert "SPOT" in pair_info.permissions

    asyncio.run(test_main())
