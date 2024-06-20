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
import copy
import datetime

import aiohttp
import aioresponses
import pytest

from basana.core import pair, helpers, token_bucket
from basana.external.bitstamp import exchange
from .helpers import assert_expected_attrs


ORDER_STATUS_1536137941123072 = {
    "status": "Canceled",
    "id": 1536137941123072,
    "amount_remaining": "0.00000",
}

ORDER_TRANSACTIONS_1536137941123072 = [
    {
        "usd": "4899.79261051",
        "price": "0.99993",
        "datetime": "2022-09-22 17:44:11.689000",
        "usdt": "4900.13562",
        "fee": "0.00000",
        "tid": 248447671,
        "type": 2
    },
    {
        "usd": "2500.03351899",
        "price": "0.99994",
        "datetime": "2022-09-22 17:44:11.689000",
        "usdt": "2500.18353",
        "fee": "0.00000",
        "tid": 248447672,
        "type": 2
    },
    {
        "usd": "2306.24286734",
        "price": "0.99999",
        "datetime": "2022-09-22 17:44:11.689000",
        "usdt": "2306.26593",
        "fee": "0.00000",
        "tid": 248447673,
        "type": 2
    },
    {
        "usd": "5315.35100298",
        "price": "1.00001",
        "datetime": "2022-09-22 17:44:11.689000",
        "usdt": "5315.29785",
        "fee": "0.00000",
        "tid": 248447674,
        "type": 2
    }
]

ORDER_STATUS_99999999 = {
    "status": "Finished",
    "id": 99999999,
    "amount_remaining": "0.00000",
}

ORDER_TRANSACTIONS_99999999 = [
    {
        "usd": "4899.79261051",
        "price": "0.99993",
        "datetime": "2022-09-22 17:44:11.689000",
        "usdt": "4900.13562",
        "fee": "0.12000",
        "tid": 1,
        "type": 2
    },
]


@pytest.fixture()
def bitstamp_http_api_mock():
    def order_status(url, **kwargs):
        order_status, order_transactions = {
            1536137941123072: (ORDER_STATUS_1536137941123072, ORDER_TRANSACTIONS_1536137941123072),
            99999999: (ORDER_STATUS_99999999, ORDER_TRANSACTIONS_99999999),
        }.get(kwargs["data"]["id"], (None, None))

        if order_status:
            status = 200
            response = copy.copy(order_status)
            if not kwargs["data"].get("omit_transactions"):
                response["transactions"] = order_transactions
        else:
            status = 200
            response = {"status": "error", "reason": "Order not found."}

        return aioresponses.CallbackResult(status=status, payload=response)

    def cancel_order(url, **kwargs):
        if int(kwargs["data"]["id"]) == 1538604691881987:
            status = 200
            response = {
                "id": 1538604691881987,
                "amount": 0.00319028,
                "price": 17500,
                "type": 0
            }
        else:
            status = 200
            response = {"error": "Order not found."}

        return aioresponses.CallbackResult(status=status, payload=response)

    with aioresponses.aioresponses() as m:
        m.post(
            "http://bitstamp.mock/api/v2/account_balances/", status=200, payload=[
                {"available": "0.01374", "currency": "pax", "total": "0.01374", "reserved": "0.00000"},
                {"available": "28.92", "currency": "usd", "total": "28.92", "reserved": "0.00"},
                {"available": "0.97899", "currency": "usdc", "total": "0.97899", "reserved": "0.00000"}
            ]
        )
        m.post(
            "http://bitstamp.mock/api/v2/account_balances/usd/", status=200, payload={
                "available": "28.92", "currency": "usd", "total": "28.92", "reserved": "0.00"
            }
        )
        m.post(
            "http://bitstamp.mock/api/v2/open_orders/all/", status=200, payload=[
                {
                    "price": "1200.0",
                    "currency_pair": "ETH/USD",
                    "datetime": "2022-09-20 16:11:06",
                    "amount": "0.01204166",
                    "amount_at_create": "0.01204166",
                    "type": "0",
                    "id": "1535407273615360"
                }
            ]
        )
        m.post(
            "http://bitstamp.mock/api/v2/open_orders/ethusd/", status=200, payload=[
                {
                    "price": "1200.0",
                    "currency_pair": "ETH/USD",
                    "datetime": "2022-09-20 16:11:06",
                    "amount": "0.01204166",
                    "amount_at_create": "0.01204166",
                    "type": "0",
                    "id": "1535407273615360"
                }
            ]
        )
        m.post("http://bitstamp.mock/api/v2/order_status/", callback=order_status)
        m.post("http://bitstamp.mock/api/v2/cancel_order/", callback=cancel_order)

        m.post(
            "http://bitstamp.mock/api/v2/buy/market/btcpax/", status=200, payload={
                "id": "1539419698798592",
                "datetime": "2022-09-30 16:47:12.583000",
                "type": "0",
                "amount": "0.00100000",
                "price": "19381",
                "client_order_id": "51557545381C4997BC452AE1E48E0D88",
            }
        )
        m.post(
            "http://bitstamp.mock/api/v2/buy/btcpax/", status=200, payload={
                "id": "1538955091660800",
                "datetime": "2022-09-30 16:47:12.583000",
                "type": "0",
                "amount": "1.00000000",
                "price": "10",
                "client_order_id": "51557545381C4997BC452AE1E48E0D88",
            }
        )
        m.post(
            "http://bitstamp.mock/api/v2/sell/instant/btcpax/", status=200, payload={
                "id": "1540371958628352",
                "datetime": "2022-09-30 16:47:12.583000",
                "type": "1",
                "amount": "13.00000000",
                "price": "19954",
                "client_order_id": "51557545381C4997BC452AE1E48E0D88",
            }
        )

        m.get(
            "http://bitstamp.mock/api/v2/ticker/btcusd/", status=200, payload={
                "timestamp": "1666808829",
                "open": "20096",
                "high": "21012",
                "low": "19971",
                "last": "20800",
                "volume": "3166.27982724",
                "vwap": "20487",
                "bid": "20781",
                "ask": "20782",
                "open_24": "20250",
                "percent_change_24": "2.72"
            }
        )

        m.get("http://bitstamp.mock/api/v2/trading-pairs-info/", status=200, payload=[
            {
                "name": "BTC/USD",
                "url_symbol": "btcusd",
                "base_decimals": 8,
                "counter_decimals": 0,
                "instant_order_counter_decimals": 2,
                "minimum_order": "10 USD",
                "trading": "Enabled",
                "instant_and_market_orders": "Enabled",
                "description": "Bitcoin / U.S. dollar"
            },
        ])

        yield m


@pytest.fixture()
def bitstamp_exchange(realtime_dispatcher):
    return exchange.Exchange(
        realtime_dispatcher, "api_key", "api_secret", tb=token_bucket.TokenBucketLimiter(10, 1, 10),
        config_overrides={"api": {"http": {"base_url": "http://bitstamp.mock/"}}}
    )


def test_balances(bitstamp_http_api_mock, bitstamp_exchange):
    async def test_main():
        balance = await bitstamp_exchange.get_balance("USD")
        assert balance.available == Decimal("28.92")
        assert balance.total == Decimal("28.92")
        assert balance.reserved == Decimal("0")

        balances = await bitstamp_exchange.get_balances()
        balance = balances["PAX"]
        assert balance.available == Decimal("0.01374")
        assert balance.total == Decimal("0.01374")
        assert balance.reserved == Decimal("0")

    asyncio.run(test_main())


def test_open_orders(bitstamp_http_api_mock, bitstamp_exchange):
    async def test_main():
        for p in [None, pair.Pair("ETH", "USD")]:
            open_orders = await bitstamp_exchange.get_open_orders(p)
            assert len(open_orders) == 1
            open_order = open_orders[0]
            assert open_order.id == "1535407273615360"
            assert open_order.datetime == datetime.datetime(2022, 9, 20, 16, 11, 6).replace(
                tzinfo=datetime.timezone.utc
            )
            assert open_order.operation == exchange.OrderOperation.BUY
            assert open_order.limit_price == Decimal(1200)
            assert open_order.amount == Decimal("0.01204166")
            assert open_order.amount_filled == Decimal("0.01204166")
            assert open_order.pair == pair.Pair("ETH", "USD")
            assert open_order.client_order_id is None

    asyncio.run(test_main())


@pytest.mark.parametrize("order_id, expected_attrs", [
    (1536137941123072, {
        "id": "1536137941123072",
        "is_open": False,
        "amount_filled": Decimal("15021.88293"),
        "amount_remaining": Decimal("0"),
        "fill_price": Decimal("0.99996"),
        "fees": {},
    }),
    (99999999, {
        "id": "99999999",
        "is_open": False,
        "amount_filled": Decimal("4900.13562"),
        "amount_remaining": Decimal("0"),
        "fill_price": Decimal("0.99993"),
        "fees": {"USD": Decimal("0.12")},
    }),
])
def test_order_info(order_id, expected_attrs, bitstamp_http_api_mock, bitstamp_exchange):
    async def test_main():
        order_info = await bitstamp_exchange.get_order_info(pair.Pair("USDT", "USD"), order_id=order_id)
        assert order_info is not None

        if "fill_price" in expected_attrs:
            assert helpers.truncate_decimal(order_info.fill_price, 5) == expected_attrs["fill_price"]
            del expected_attrs["fill_price"]  # So its not checked inside assert_expected_attrs
        assert_expected_attrs(order_info, expected_attrs)

    asyncio.run(test_main())


def test_order_status(bitstamp_http_api_mock, bitstamp_exchange):
    async def test_main():
        order_status = await bitstamp_exchange.get_order_status(order_id="1536137941123072")
        assert order_status is not None
        assert order_status.id == "1536137941123072"
        assert order_status.status == "Canceled"
        assert order_status.amount_remaining == Decimal("0")
        assert order_status.client_order_id is None

        assert len(order_status.transactions) == 4
        assert order_status.transactions[0].tid == "248447671"
        assert order_status.transactions[0].price == Decimal("0.99993")
        assert order_status.transactions[0].fee == Decimal("0")
        assert order_status.transactions[0].type == exchange.TransactionType.MARKET_TRADE
        assert order_status.transactions[0].usd == Decimal("4899.79261051")
        assert order_status.transactions[0].usdt == Decimal("4900.13562")
        with pytest.raises(AttributeError):
            assert order_status.transactions[0].pax

    asyncio.run(test_main())


def test_order_status_without_transactions(bitstamp_http_api_mock, bitstamp_exchange):
    async def test_main():
        order_status = await bitstamp_exchange.get_order_status(order_id="1536137941123072", omit_transactions=True)
        assert order_status is not None
        assert order_status.id == "1536137941123072"
        assert order_status.status == "Canceled"
        assert order_status.amount_remaining == Decimal("0")
        assert order_status.client_order_id is None

        assert len(order_status.transactions) == 0

    asyncio.run(test_main())


def test_order_status_not_found(bitstamp_http_api_mock, bitstamp_exchange):
    async def test_main():
        with pytest.raises(exchange.Error) as excinfo:
            await bitstamp_exchange.get_order_status(order_id="1234")
        assert str(excinfo.value) == "Order not found."
        assert excinfo.value.json_response["reason"] == "Order not found."

    asyncio.run(test_main())


def test_explicit_session(bitstamp_http_api_mock, realtime_dispatcher):
    async def test_main():
        async with aiohttp.ClientSession() as session:
            e = exchange.Exchange(
                realtime_dispatcher, "api_key", "api_secret", session=session,
                config_overrides={"api": {"http": {"base_url": "http://bitstamp.mock/"}}}
            )

            balance = await e.get_balance("USD")
            assert balance.available == Decimal("28.92")
            assert balance.total == Decimal("28.92")
            assert balance.reserved == Decimal("0")

            balances = await e.get_balances()
            balance = balances["PAX"]
            assert balance.available == Decimal("0.01374")
            assert balance.total == Decimal("0.01374")
            assert balance.reserved == Decimal("0")

    asyncio.run(test_main())


def test_cancel_order(bitstamp_http_api_mock, bitstamp_exchange):
    async def test_main():
        canceled_order = await bitstamp_exchange.cancel_order(1538604691881987)
        assert canceled_order is not None
        assert canceled_order.id == "1538604691881987"
        assert helpers.truncate_decimal(canceled_order.amount, 8) == Decimal("0.00319028")
        assert canceled_order.limit_price == Decimal("17500")
        assert canceled_order.operation == exchange.OrderOperation.BUY

    asyncio.run(test_main())


@pytest.mark.parametrize("create_order_fun, expected_op, expected_amount, expected_price, expected_id", [
    (
        lambda e: e.create_market_order(
            exchange.OrderOperation.BUY, pair.Pair("BTC", "PAX"), Decimal("0.00100000"),
            client_order_id="51557545381C4997BC452AE1E48E0D88"
        ),
        exchange.OrderOperation.BUY, Decimal("0.00100000"), Decimal("19381"), "1539419698798592"
    ),
    (
        lambda e: e.create_limit_order(
            exchange.OrderOperation.BUY, pair.Pair("BTC", "PAX"), Decimal("1"), Decimal("10"),
            client_order_id="51557545381C4997BC452AE1E48E0D88"
        ),
        exchange.OrderOperation.BUY, Decimal("1"), Decimal("10"), "1538955091660800"
    ),
    (
        lambda e: e.create_instant_order(
            exchange.OrderOperation.SELL, pair.Pair("BTC", "PAX"), Decimal("13"), amount_in_counter=True,
            client_order_id="51557545381C4997BC452AE1E48E0D88"
        ),
        exchange.OrderOperation.SELL, Decimal("13"), Decimal("19954"), "1540371958628352"
    ),
])
def test_order_requests(
        create_order_fun, expected_op, expected_amount, expected_price, expected_id,
        bitstamp_http_api_mock, bitstamp_exchange
):
    async def test_main():
        order_created = await create_order_fun(bitstamp_exchange)
        assert order_created is not None
        assert order_created.id == expected_id
        assert order_created.datetime == datetime.datetime(2022, 9, 30, 16, 47, 12, 583000).replace(
            tzinfo=datetime.timezone.utc
        )
        assert order_created.operation == expected_op
        assert order_created.amount == expected_amount
        assert order_created.price == expected_price
        assert order_created.client_order_id == "51557545381C4997BC452AE1E48E0D88"

    asyncio.run(test_main())


def test_cancel_inexistent_order(bitstamp_http_api_mock, bitstamp_exchange):
    async def test_main():
        with pytest.raises(exchange.Error) as excinfo:
            await bitstamp_exchange.cancel_order(1234)
        assert str(excinfo.value) == "Order not found."
        assert excinfo.value.json_response["error"] == "Order not found."

    asyncio.run(test_main())


def test_bid_ask(bitstamp_http_api_mock, bitstamp_exchange):
    async def test_main():
        bid, ask = await bitstamp_exchange.get_bid_ask(pair.Pair("BTC", "USD"))
        assert bid == Decimal("20781")
        assert ask == Decimal("20782")

    asyncio.run(test_main())


def test_get_pair_info(bitstamp_http_api_mock, bitstamp_exchange):
    async def impl():
        assert await bitstamp_exchange.get_pair_info(pair.Pair("BTC", "USD")) == pair.PairInfo(8, 0)

    asyncio.run(impl())
