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

import aioresponses
import pytest

from basana.external.bitstamp import client


@pytest.fixture()
def bitstamp_http_api_mock():
    with aioresponses.aioresponses() as m:
        yield m


def test_get_ohlc_data_using_end(bitstamp_http_api_mock):
    bitstamp_http_api_mock.get(
        "http://bitstamp.mock/api/v2/ohlc/btcusd/?end=1451606400&limit=1&step=60", status=200,
        payload={
            "data": {
                "ohlc": [
                    {
                        "close": "430.89",
                        "high": "430.89",
                        "low": "430.89",
                        "open": "430.89",
                        "timestamp": "1451606400",
                        "volume": "0.00000000"
                    }
                ],
                "pair": "BTC/USD"
            }
        }
    )

    async def test_main():
        c = client.APIClient(
            config_overrides={"api": {"http": {"base_url": "http://bitstamp.mock/"}}}
        )
        response = await c.get_ohlc_data("btcusd", 60, end=1451606400, limit=1)
        assert len(response["data"]["ohlc"]) == 1

    asyncio.run(test_main())


@pytest.mark.parametrize("status_code, response_body, expected", [
    (403, {}, "403 Forbidden"),
    (200, {"status": "error", "reason": "Order not found"}, "Order not found"),
    (200, {"error": "Order not found"}, "Order not found"),
    (200, {"code": "Order not found", "errors": "blabla"}, "Order not found"),
])
def test_error_parsing(status_code, response_body, expected, bitstamp_http_api_mock):
    bitstamp_http_api_mock.get(
        "http://bitstamp.mock/api/v2/order_book/btcusd/", status=status_code, payload=response_body
    )

    async def test_main():
        c = client.APIClient(
            config_overrides={"api": {"http": {"base_url": "http://bitstamp.mock/"}}}
        )
        with pytest.raises(client.Error) as excinfo:
            await c.get_order_book("btcusd")
        assert str(excinfo.value) == expected

    asyncio.run(test_main())
