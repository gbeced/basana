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

import re

import aioresponses
import pytest

from basana.external.binance import client, helpers as binance_helpers


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
async def test_error_parsing(status_code, response_body, expected, binance_http_api_mock):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/account\\?.*"), status=status_code, payload=response_body
    )

    c = client.APIClient(
        api_key="key", api_secret="secret", config_overrides={"api": {"http": {"base_url": "http://binance.mock/"}}}
    )
    with pytest.raises(client.Error) as excinfo:
        await c.spot_account.get_account_information()
    assert str(excinfo.value) == expected


async def test_get_exchange_info_no_symbol(binance_http_api_mock):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/exchangeInfo"), status=200,
        payload={"symbols": []}
    )
    c = client.APIClient(
        api_key="key", api_secret="secret", config_overrides={"api": {"http": {"base_url": "http://binance.mock/"}}}
    )
    result = await c.get_exchange_info()
    assert result == {"symbols": []}


async def test_non_json_response(binance_http_api_mock):
    # Respond without application/json content type – json_response should be None and raise_for_error should trigger.
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/exchangeInfo.*"),
        status=403, body="Forbidden", content_type="text/plain"
    )
    c = client.APIClient(
        api_key="key", api_secret="secret", config_overrides={"api": {"http": {"base_url": "http://binance.mock/"}}}
    )
    with pytest.raises(client.Error):
        await c.get_exchange_info()


async def test_spot_get_open_orders_no_symbol(binance_http_api_mock):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/openOrders.*"), status=200, payload=[]
    )
    c = client.APIClient(
        api_key="key", api_secret="secret", config_overrides={"api": {"http": {"base_url": "http://binance.mock/"}}}
    )
    result = await c.spot_account.get_open_orders()
    assert result == []


async def test_spot_get_trades_no_order_id(binance_http_api_mock):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/myTrades.*"), status=200, payload=[]
    )
    c = client.APIClient(
        api_key="key", api_secret="secret", config_overrides={"api": {"http": {"base_url": "http://binance.mock/"}}}
    )
    result = await c.spot_account.get_trades("BTCUSDT")
    assert result == []


async def test_margin_get_open_orders_no_symbol(binance_http_api_mock):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/sapi/v1/margin/openOrders.*"), status=200, payload=[]
    )
    c = client.APIClient(
        api_key="key", api_secret="secret", config_overrides={"api": {"http": {"base_url": "http://binance.mock/"}}}
    )
    result = await c.cross_margin_account.get_open_orders()
    assert result == []


async def test_margin_get_trades_no_order_id(binance_http_api_mock):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/sapi/v1/margin/myTrades.*"), status=200, payload=[]
    )
    c = client.APIClient(
        api_key="key", api_secret="secret", config_overrides={"api": {"http": {"base_url": "http://binance.mock/"}}}
    )
    result = await c.cross_margin_account.get_trades("BTCUSDT")
    assert result == []


def test_get_signature_no_params():
    # get_signature with empty qs_params should produce a valid signature (skipping urlencode for empty qs_params).
    sig = binance_helpers.get_signature("secret", qs_params={}, data={})
    assert isinstance(sig, str)
    assert len(sig) == 64  # SHA256 hex digest


def test_get_optional_decimal_missing_key():
    # When the key is not present, get_optional_decimal should return None.
    result = binance_helpers.get_optional_decimal({}, "missing_key", skip_zero=False)
    assert result is None