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
import datetime
import re

import pytest

from . import helpers
from basana.core.pair import Pair
from basana.external.binance import exchange


CREATE_ORDER_RESPONSE_16620163621 = {
    "symbol": "BTCUSDT",
    "orderId": 16620163621,
    "clientOrderId": "8174E476E48943AE95D005BAFA473792",
    "transactTime": 1671549673288,
    "price": "14000",
    "origQty": "0.001",
    "executedQty": "0",
    "cummulativeQuoteQty": "0",
    "status": "NEW",
    "timeInForce": "GTC",
    "type": "LIMIT",
    "side": "BUY",
    "fills": [],
    "isIsolated": True
}


def test_transfer(binance_http_api_mock, binance_exchange):
    async def test_main():
        binance_http_api_mock.post(
            re.compile(r"http://binance.mock/sapi/v1/margin/isolated/transfer\\?.*"), status=200,
            payload={"tranId": 124739543934, "clientTag": ""}, repeat=True
        )
        assert (await binance_exchange.isolated_margin_account.transfer_from_spot_account(
            "USDT", Pair("BTC", "USDT"), Decimal("100")))["tranId"] == 124739543934
        assert (await binance_exchange.isolated_margin_account.transfer_to_spot_account(
            "USDT", Pair("BTC", "USDT"), Decimal("100")))["tranId"] == 124739543934

    asyncio.run(test_main())


def test_account_balances(binance_http_api_mock, binance_exchange):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/sapi/v1/margin/isolated/account\\?.*"), status=200,
        payload=helpers.load_json("binance_isolated_margin_account_details.json")
    )

    async def test_main():
        balances = await binance_exchange.isolated_margin_account.get_balances()

        isolated_balance = balances[Pair("BTC", "USDT")]
        assert isolated_balance.base_asset == "BTC"
        assert isolated_balance.base_asset_balance.total == Decimal("0")
        assert isolated_balance.quote_asset == "USDT"
        assert isolated_balance.quote_asset_balance.total == Decimal("100")
        assert isolated_balance.quote_asset_balance.available == Decimal("100")
        assert isolated_balance.quote_asset_balance.locked == Decimal("0")
        assert isolated_balance.quote_asset_balance.borrowed == Decimal("0")

    asyncio.run(test_main())


@pytest.mark.parametrize("create_order_fun, response_payload, expected_attrs, expected_fills", [
    (
        lambda e: e.isolated_margin_account.create_limit_order(
            exchange.OrderOperation.BUY, Pair("BTC", "USDT"), amount=Decimal("0.001"),
            limit_price=Decimal("14000"), client_order_id="8174E476E48943AE95D005BAFA473792"
        ),
        CREATE_ORDER_RESPONSE_16620163621,
        {
            "id": "16620163621",
            "datetime": datetime.datetime(2022, 12, 20, 15, 21, 13, 288000).replace(tzinfo=datetime.timezone.utc),
            "client_order_id": "8174E476E48943AE95D005BAFA473792",
            "limit_price": Decimal("14000"),
            "amount": Decimal("0.001"),
            "amount_filled": Decimal("0"),
            "quote_amount_filled": Decimal("0"),
            "status": "NEW",
            "time_in_force": "GTC",
            "is_open": True,
        },
        [],
    ),
])
def test_create_order(
        create_order_fun, response_payload, expected_attrs, expected_fills, binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.post(
        re.compile(r"http://binance.mock/sapi/v1/margin/order\\?.*"), status=200, payload=response_payload
    )

    async def test_main():
        order_created = await create_order_fun(binance_exchange)
        assert order_created is not None
        helpers.assert_expected_attrs(order_created, expected_attrs)
        assert len(order_created.fills) == len(expected_fills)
        for fill, expected_fill in zip(order_created.fills, expected_fills):
            helpers.assert_expected_attrs(fill, expected_fill)

    asyncio.run(test_main())
