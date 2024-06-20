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

from basana.external.binance import client


@pytest.fixture()
def binance_http_api_mock():
    with aioresponses.aioresponses() as m:
        yield m


@pytest.mark.parametrize("status_code, response_body, expected", [
    (403, {}, "403 Forbidden"),
    (
        400,
        {"code": -1022, "msg": "Signature for this request is not valid."},
        "Signature for this request is not valid."
    ),
])
def test_error_parsing(status_code, response_body, expected, binance_http_api_mock):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/account\\?.*"), status=status_code, payload=response_body
    )

    async def test_main():
        c = client.APIClient(
            api_key="key", api_secret="secret", config_overrides={"api": {"http": {"base_url": "http://binance.mock/"}}}
        )
        with pytest.raises(client.Error) as excinfo:
            await c.spot_account.get_account_information()
        assert str(excinfo.value) == expected

    asyncio.run(test_main())
