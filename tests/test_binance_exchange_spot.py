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

from basana.core import pair
from basana.external.binance import exchange
from tests import helpers


CANCELED_OCO_ORDER_RESPONSE = {
    "orderListId": 77862615,
    "contingencyType": "OCO",
    "listStatusType": "ALL_DONE",
    "listOrderStatus": "ALL_DONE",
    "listClientOrderId": "0A8oF9T3k96l6lqzqGOIfB",
    "transactionTime": 1668527583935,
    "symbol": "BTCUSDT",
    "orders": [
        {"symbol": "BTCUSDT", "orderId": 15558250268, "clientOrderId": "ZDlLvguLRpgGRfcwLLeATp"},
        {"symbol": "BTCUSDT", "orderId": 15558250269, "clientOrderId": "AdLeXmt1ChmIloCJJKC0xu"}
    ],
    "orderReports": [
        {
            "symbol": "BTCUSDT",
            "origClientOrderId": "ZDlLvguLRpgGRfcwLLeATp",
            "orderId": 15558250268,
            "orderListId": 77862615,
            "clientOrderId": "GDrsTf3T6HptxXNQSswwIE",
            "price": "10000.00000000",
            "origQty": "0.00100000",
            "executedQty": "0.00000000",
            "cummulativeQuoteQty": "0.00000000",
            "status": "CANCELED",
            "timeInForce": "GTC",
            "type": "STOP_LOSS_LIMIT",
            "side": "SELL",
            "stopPrice": "10000.00000000"
        },
        {
            "symbol": "BTCUSDT",
            "origClientOrderId": "AdLeXmt1ChmIloCJJKC0xu",
            "orderId": 15558250269,
            "orderListId": 77862615,
            "clientOrderId": "GDrsTf3T6HptxXNQSswwIE",
            "price": "23000.00000000",
            "origQty": "0.00100000",
            "executedQty": "0.00000000",
            "cummulativeQuoteQty": "0.00000000",
            "status": "CANCELED",
            "timeInForce": "GTC",
            "type": "LIMIT_MAKER",
            "side": "SELL"
        }
    ]
}

OCO_ORDER_INFO_RESPONSE = {
    "orderListId": 77826706,
    "contingencyType": "OCO",
    "listStatusType": "ALL_DONE",
    "listOrderStatus": "ALL_DONE",
    "listClientOrderId": "9wQuvisHlbJPQOwrJLhqgn",
    "transactionTime": 1668478867902,
    "symbol": "BTCUSDT", "orders": [
        {"symbol": "BTCUSDT", "orderId": 15535167360, "clientOrderId": "bUDxpWHguNICQ7MGe4bAxK"},
        {"symbol": "BTCUSDT", "orderId": 15535167361, "clientOrderId": "Eq4WPPmCLE0Vp1P4XuXG3G"}
    ]
}


def test_account_balances(binance_http_api_mock, binance_exchange):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/account\\?.*"), status=200,
        payload={
            "makerCommission": 10,
            "takerCommission": 10,
            "buyerCommission": 0,
            "sellerCommission": 0,
            "canTrade": True,
            "canWithdraw": True,
            "canDeposit": True,
            "brokered": False,
            "updateTime": 1667518291196,
            "accountType": "SPOT",
            "balances": [
                {
                    "asset": "BTC",
                    "free": "0.00000053",
                    "locked": "1.00000000"
                },
                {
                    "asset": "LTC",
                    "free": "0.00000000",
                    "locked": "0.00000000"
                },
            ],
            "permissions": [
                "SPOT"
            ]
        }
    )

    async def test_main():
        balances = await binance_exchange.spot_account.get_balances()

        btc_balance = balances["BTC"]
        assert btc_balance.available == Decimal("0.00000053")
        assert btc_balance.total == Decimal("1.00000053")
        assert btc_balance.locked == Decimal("1")

    asyncio.run(test_main())


@pytest.mark.parametrize("create_order_fun, response_payload, expected_attrs, expected_fills", [
    (
        lambda e: e.spot_account.create_market_order(
            exchange.OrderOperation.BUY, pair.Pair("BTC", "USDT"), amount=Decimal("0.001")
        ),
        {
            "symbol": "BTCUSDT",
            "orderId": 15374780716,
            "orderListId": -1,
            "clientOrderId": "w4PHTG4wsaN6bEKoBMNK7O",
            "transactTime": 1668129070269,
            "price": "0.00000000",
            "origQty": "0.00100000",
            "executedQty": "0.00100000",
            "cummulativeQuoteQty": "17.62721000",
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": "BUY",
            "fills": [
                {
                    "price": "17627.21000000",
                    "qty": "0.00100000",
                    "commission": "0.00000000",
                    "commissionAsset": "BNB",
                    "tradeId": 2156194389
                }
            ]
        },
        {
            "id": "15374780716",
            "datetime": datetime.datetime(2022, 11, 11, 1, 11, 10, 269000).replace(tzinfo=datetime.timezone.utc),
            "client_order_id": "w4PHTG4wsaN6bEKoBMNK7O",
            "limit_price": None,
            "amount": Decimal("0.001"),
            "amount_filled": Decimal("0.001"),
            "quote_amount_filled": Decimal("17.62721"),
            "status": "FILLED",
            "time_in_force": "GTC",
            "order_list_id": None,
            "is_open": False,
        },
        [
            {
                "price": Decimal("17627.21"),
                "amount": Decimal("0.001"),
                "commission": Decimal("0"),
                "commission_asset": "BNB",
                "trade_id": "2156194389",
            }
        ],
    ),
    (
        lambda e: e.spot_account.create_market_order(
            exchange.OrderOperation.BUY, pair.Pair("BTC", "USDT"), quote_amount=Decimal("30")
        ),
        {
            "symbol": "BTCUSDT",
            "orderId": 15455625561,
            "orderListId": -1,
            "clientOrderId": "kTjLDEuQdFO5VZmWTbAT7b",
            "transactTime": 1668306875519,
            "price": "0.00000000",
            "origQty": "0.00177000",
            "executedQty": "0.00177000",
            "cummulativeQuoteQty": "29.87237850",
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": "BUY",
            "fills": [
                {
                    "price": "16877.05000000",
                    "qty": "0.00177000",
                    "commission": "0.00000000",
                    "commissionAsset": "BNB",
                    "tradeId": 2171115716
                }
            ]
        },
        {
            "id": "15455625561",
            "datetime": datetime.datetime(2022, 11, 13, 2, 34, 35, 519000).replace(tzinfo=datetime.timezone.utc),
            "client_order_id": "kTjLDEuQdFO5VZmWTbAT7b",
            "limit_price": None,
            "amount": Decimal("0.00177"),
            "amount_filled": Decimal("0.00177"),
            "quote_amount_filled": Decimal("29.87237850"),
            "status": "FILLED",
            "time_in_force": "GTC",
            "order_list_id": None,
            "is_open": False,
        },
        [
            {
                "price": Decimal("16877.05"),
                "amount": Decimal("0.00177"),
                "commission": Decimal("0"),
                "commission_asset": "BNB",
                "trade_id": "2171115716",
            }
        ],
    ),
    (
        lambda e: e.spot_account.create_limit_order(
            exchange.OrderOperation.SELL, pair.Pair("BTC", "USDT"), Decimal("0.001"), Decimal("17498")
        ),
        {
            "symbol": "BTCUSDT",
            "orderId": 15456494165,
            "orderListId": -1,
            "clientOrderId": "x7frvT9IHW5ogIKYUnrH6L",
            "transactTime": 1668309613411,
            "price": "17498.00000000",
            "origQty": "0.00100000",
            "executedQty": "0.00000000",
            "cummulativeQuoteQty": "0.00000000",
            "status": "NEW",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "SELL",
            "fills": [],
            "is_open": True,
        },
        {
            "id": "15456494165",
            "datetime": datetime.datetime(2022, 11, 13, 3, 20, 13, 411000).replace(tzinfo=datetime.timezone.utc),
            "client_order_id": "x7frvT9IHW5ogIKYUnrH6L",
            "limit_price": Decimal("17498"),
            "amount": Decimal("0.001"),
            "amount_filled": Decimal("0"),
            "quote_amount_filled": Decimal("0"),
            "status": "NEW",
            "time_in_force": "GTC",
            "order_list_id": None,
        },
        [],
    ),
    (
        lambda e: e.spot_account.create_stop_limit_order(
            exchange.OrderOperation.SELL, pair.Pair("BTC", "USDT"), Decimal("0.001"), Decimal("15000"), Decimal("20000")
        ),
        {
            "symbol": "BTCUSDT",
            "orderId": 15533662777,
            "orderListId": -1,
            "clientOrderId": "guCHQcaPSzFFEk5Dr7Fsgq",
            "transactTime": 1668475978860
        },
        {
            "id": "15533662777",
            "client_order_id": "guCHQcaPSzFFEk5Dr7Fsgq",
        },
        [],
    )
])
def test_create_order(
        create_order_fun, response_payload, expected_attrs, expected_fills, binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.post(
        re.compile(r"http://binance.mock/api/v3/order\\?.*"), status=200, payload=response_payload
    )

    async def test_main():
        order_created = await create_order_fun(binance_exchange)
        assert order_created is not None
        helpers.assert_expected_attrs(order_created, expected_attrs)
        assert len(order_created.fills) == len(expected_fills)
        for fill, expected_fill in zip(order_created.fills, expected_fills):
            helpers.assert_expected_attrs(fill, expected_fill)

    asyncio.run(test_main())


@pytest.mark.parametrize(
    "pair, order_id, client_order_id, order_payload, trades_payload, expected_attrs, expected_first_trade",
    [
        (
            pair.Pair("BTC", "USDT"), "15456494165", None,
            {
                "symbol": "BTCUSDT",
                "orderId": 15456494165,
                "orderListId": -1,
                "clientOrderId": "x7frvT9IHW5ogIKYUnrH6L",
                "price": "17498.00000000",
                "origQty": "0.00100000",
                "executedQty": "0.00000000",
                "cummulativeQuoteQty": "0.00000000",
                "status": "NEW",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "SELL",
                "stopPrice": "0.00000000",
                "icebergQty": "0.00000000",
                "time": 1668309613411,
                "updateTime": 1668309613411,
                "isWorking": True,
                "origQuoteOrderQty": "0.00000000"
            },
            [],
            {
                "id": "15456494165",
                "is_open": True,
                "operation": exchange.OrderOperation.SELL,
                "amount": Decimal("0.001"),
                "amount_filled": Decimal("0"),
                "amount_remaining": Decimal("0.001"),
                "limit_price": Decimal("17498"),
                "fill_price": None,
                "fees": {},
            },
            None,
        ),
        (
            pair.Pair("BTC", "USDT"), None, "kTjLDEuQdFO5VZmWTbAT7b",
            {
                "symbol": "BTCUSDT",
                "orderId": 15455625561,
                "orderListId": -1,
                "clientOrderId": "kTjLDEuQdFO5VZmWTbAT7b",
                "price": "0.00000000",
                "origQty": "0.00177000",
                "executedQty": "0.00177000",
                "cummulativeQuoteQty": "29.87237850",
                "status": "FILLED",
                "timeInForce": "GTC",
                "type": "MARKET",
                "side": "BUY",
                "stopPrice": "0.00000000",
                "icebergQty": "0.00000000",
                "time": 1668306875519,
                "updateTime": 1668306875519,
                "isWorking": True,
                "origQuoteOrderQty": "30.00000000"
            },
            [
                {
                    "commission": "0.01000000",
                    "commissionAsset": "BNB",
                    "id": 2171115716,
                    "isBestMatch": True,
                    "isBuyer": True,
                    "isMaker": False,
                    "orderId": 15455625561,
                    "orderListId": -1,
                    "price": "16877.05000000",
                    "qty": "0.00177000",
                    "quoteQty": "29.87237850",
                    "symbol": "BTCUSDT",
                    "time": 1668306875519
                }
            ],
            {
                "id": "15455625561",
                "is_open": False,
                "operation": exchange.OrderOperation.BUY,
                "amount": Decimal("0.00177"),
                "amount_filled": Decimal("0.00177"),
                "amount_remaining": Decimal("0"),
                "stop_price": None,
                "limit_price": None,
                "fill_price": Decimal("16877.05"),
                "fees": {"BNB": Decimal("0.01")}
            },
            {
                "id": "2171115716",
                "order_id": "15455625561",
                "order_list_id": None,
                "commission": Decimal("0.01"),
                "commission_asset": "BNB",
                "is_best_match": True,
                "is_buyer": True,
                "is_maker": False,
                "price": Decimal("16877.05"),
                "amount": Decimal("0.00177"),
                "quote_amount": Decimal("29.8723785"),
                "datetime": datetime.datetime(2022, 11, 13, 2, 34, 35, 519000).replace(tzinfo=datetime.timezone.utc),
            },
        ),
    ]
)
def test_order_info(
        pair, order_id, client_order_id, order_payload, trades_payload, expected_attrs, expected_first_trade,
        binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/order\\?.*"), status=200, payload=order_payload
    )
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/myTrades\\?.*"), status=200, payload=trades_payload
    )

    async def test_main():
        order_info = await binance_exchange.spot_account.get_order_info(
            pair, order_id=order_id, client_order_id=client_order_id
        )
        assert order_info is not None
        helpers.assert_expected_attrs(order_info, expected_attrs)
        if expected_first_trade:
            helpers.assert_expected_attrs(order_info.trades[0], expected_first_trade)

    asyncio.run(test_main())


def test_error_retrieving_order_info(binance_http_api_mock, binance_exchange):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/order\\?.*"), status=500,
        payload={}
    )

    async def test_main():
        with pytest.raises(exchange.Error) as excinfo:
            await binance_exchange.spot_account.get_order_info(pair.Pair("BTC", "USDT"), order_id="1234")
        assert str(excinfo.value) == "500 Internal Server Error"
        assert excinfo.value.http_status == 500
        assert excinfo.value.http_reason == "Internal Server Error"

    asyncio.run(test_main())


@pytest.mark.parametrize("pair, order_id, client_order_id, response_payload, expected_attrs", [
    (
        pair.Pair("BTC", "USDT"), "15456494165", None,
        {
            "symbol": "BTCUSDT",
            "origClientOrderId": "x7frvT9IHW5ogIKYUnrH6L",
            "orderId": 15456494165,
            "orderListId": -1,
            "clientOrderId": "OS6KUQ0RDJ9DVrxRYAqYn9",
            "price": "17498.00000000",
            "origQty": "0.00100000",
            "executedQty": "0.00000000",
            "cummulativeQuoteQty": "0.00000000",
            "status": "CANCELED",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "SELL"
        },
        {
            "id": "15456494165",
            "is_open": False,
        }
    ),
    (
        pair.Pair("BTC", "USDT"), None, "wmJ3mL6libCN8N7rEjCTIk",
        {
            "symbol": "BTCUSDT",
            "origClientOrderId": "wmJ3mL6libCN8N7rEjCTIk",
            "orderId": 15523683385,
            "orderListId": -1,
            "clientOrderId": "cVks1NPde0Lt9XFZYdCIOQ",
            "price": "18998.00000000",
            "origQty": "0.00100000",
            "executedQty": "0.00000000",
            "cummulativeQuoteQty": "0.00000000",
            "status": "CANCELED",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "SELL"
        },
        {
            "id": "15523683385",
            "is_open": False,
            "order_list_id": None,
            "limit_price": Decimal(18998),
            "amount": Decimal("0.001"),
            "amount_filled": Decimal("0"),
            "quote_amount_filled": Decimal("0"),
            "status": "CANCELED",
            "time_in_force": "GTC",
            "operation": exchange.OrderOperation.SELL,
            "type": "LIMIT",
        }
    )
])
def test_cancel_order(
        pair, order_id, client_order_id, response_payload, expected_attrs, binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.delete(
        re.compile(r"http://binance.mock/api/v3/order\\?.*"), status=200, payload=response_payload
    )

    async def test_main():
        canceled_order = await binance_exchange.spot_account.cancel_order(
            pair, order_id=order_id, client_order_id=client_order_id
        )
        assert canceled_order is not None
        helpers.assert_expected_attrs(canceled_order, expected_attrs)

    asyncio.run(test_main())


@pytest.mark.parametrize(
    "pair, open_orders_payload, expected_first_open_order",
    [
        (
            pair.Pair("BTC", "USDT"),
            [
                {
                    "clientOrderId": "web_6bda0ea9d8f34d1dbc2956e1f4d33cd2",
                    "cummulativeQuoteQty": "0.00000000",
                    "executedQty": "0.00000000",
                    "icebergQty": "0.00000000",
                    "isWorking": True,
                    "orderId": 18687117506,
                    "orderListId": -1,
                    "origQty": "0.01023000",
                    "origQuoteOrderQty": "0.00000000",
                    "price": "25000.00000000",
                    "selfTradePreventionMode": "NONE",
                    "side": "SELL",
                    "status": "NEW",
                    "stopPrice": "0.00000000",
                    "symbol": "BTCUSDT",
                    "time": 1676482455273,
                    "timeInForce": "GTC",
                    "type": "LIMIT",
                    "updateTime": 1676482455273,
                    "workingTime": 1676482455273
                }
            ],
            {
                "id": "18687117506",
                "client_order_id": "web_6bda0ea9d8f34d1dbc2956e1f4d33cd2",
                "quote_amount_filled": Decimal(0),
                "amount_filled": Decimal(0),
                "order_list_id": None,
                "amount": Decimal("0.01023000"),
                "quote_amount": None,
                "limit_price": Decimal("25000"),
                "stop_price": None,
                "operation": exchange.OrderOperation.SELL,
                "status": "NEW",
                "time_in_force": "GTC",
                "datetime": datetime.datetime(2023, 2, 15, 17, 34, 15, 273000).replace(tzinfo=datetime.timezone.utc),
                "type": "LIMIT",
            },
        ),
    ]
)
def test_get_open_orders(
        pair, open_orders_payload, expected_first_open_order, binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/openOrders\\?.*"), status=200, payload=open_orders_payload
    )

    async def test_main():
        open_orders = await binance_exchange.spot_account.get_open_orders(pair)
        helpers.assert_expected_attrs(open_orders[0], expected_first_open_order)

    asyncio.run(test_main())


@pytest.mark.parametrize("create_order_fun, response_payload, expected_attrs", [
    (
        lambda e: e.spot_account.create_oco_order(
            exchange.OrderOperation.SELL, pair.Pair("BTC", "USDT"), Decimal("0.001"), Decimal("23000"),
            Decimal("10000"), stop_limit_price=Decimal("10000")
        ),
        {
            "orderListId": 77826706,
            "contingencyType": "OCO",
            "listStatusType": "EXEC_STARTED",
            "listOrderStatus": "EXECUTING",
            "listClientOrderId": "9wQuvisHlbJPQOwrJLhqgn",
            "transactionTime": 1668478867902,
            "symbol": "BTCUSDT",
            "orders": [
                {"symbol": "BTCUSDT", "orderId": 15535167360, "clientOrderId": "bUDxpWHguNICQ7MGe4bAxK"},
                {"symbol": "BTCUSDT", "orderId": 15535167361, "clientOrderId": "Eq4WPPmCLE0Vp1P4XuXG3G"}
            ],
            "orderReports": [
                {
                    "symbol": "BTCUSDT",
                    "orderId": 15535167360,
                    "orderListId": 77826706,
                    "clientOrderId": "bUDxpWHguNICQ7MGe4bAxK",
                    "transactTime": 1668478867902,
                    "price": "10000.00000000",
                    "origQty": "0.00100000",
                    "executedQty": "0.00000000",
                    "cummulativeQuoteQty": "0.00000000",
                    "status": "NEW",
                    "timeInForce": "GTC",
                    "type": "STOP_LOSS_LIMIT",
                    "side": "SELL",
                    "stopPrice": "10000.00000000"
                },
                {
                    "symbol": "BTCUSDT",
                    "orderId": 15535167361,
                    "orderListId": 77826706,
                    "clientOrderId": "Eq4WPPmCLE0Vp1P4XuXG3G",
                    "transactTime": 1668478867902,
                    "price": "23000.00000000",
                    "origQty": "0.00100000",
                    "executedQty": "0.00000000",
                    "cummulativeQuoteQty": "0.00000000",
                    "status": "NEW",
                    "timeInForce": "GTC",
                    "type": "LIMIT_MAKER",
                    "side": "SELL"
                }
            ]
        },
        {
            "order_list_id": "77826706",
            "client_order_list_id": "9wQuvisHlbJPQOwrJLhqgn",
            "datetime": datetime.datetime(2022, 11, 15, 2, 21, 7, 902000).replace(tzinfo=datetime.timezone.utc),
            "is_open": True,
            "limit_order_id": "15535167361",
            "stop_loss_order_id": "15535167360",
        },
    ),
])
def test_create_oco_order(
        create_order_fun, response_payload, expected_attrs, binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.post(
        re.compile(r"http://binance.mock/api/v3/order/oco\\?.*"), status=200, payload=response_payload
    )

    async def test_main():
        order_created = await create_order_fun(binance_exchange)
        assert order_created is not None
        helpers.assert_expected_attrs(order_created, expected_attrs)

    asyncio.run(test_main())


@pytest.mark.parametrize("pair, order_list_id, client_order_list_id, response_payload, expected_attrs", [
    (
        pair.Pair("BTC", "USDT"), "77862615", None,
        CANCELED_OCO_ORDER_RESPONSE,
        {
            "order_list_id": "77862615",
            "client_order_list_id": "0A8oF9T3k96l6lqzqGOIfB",
            "datetime": datetime.datetime(2022, 11, 15, 15, 53, 3, 935000).replace(tzinfo=datetime.timezone.utc),
            "is_open": False,
        }
    ),
    (
        pair.Pair("BTC", "USDT"), None, "0A8oF9T3k96l6lqzqGOIfB",
        CANCELED_OCO_ORDER_RESPONSE,
        {
            "order_list_id": "77862615",
            "client_order_list_id": "0A8oF9T3k96l6lqzqGOIfB",
            "datetime": datetime.datetime(2022, 11, 15, 15, 53, 3, 935000).replace(tzinfo=datetime.timezone.utc),
            "is_open": False,
        }
    )
])
def test_cancel_oco_order(
        pair, order_list_id, client_order_list_id, response_payload, expected_attrs,
        binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.delete(
        re.compile(r"http://binance.mock/api/v3/orderList\\?.*"), status=200, payload=response_payload
    )

    async def test_main():
        canceled_order = await binance_exchange.spot_account.cancel_oco_order(
            pair, order_list_id=order_list_id, client_order_list_id=client_order_list_id
        )
        assert canceled_order is not None
        helpers.assert_expected_attrs(canceled_order, expected_attrs)

    asyncio.run(test_main())


@pytest.mark.parametrize("order_list_id, client_order_list_id, response_payload, expected_attrs", [
    (
        "77826706", None,
        OCO_ORDER_INFO_RESPONSE,
        {
            "order_list_id": "77826706",
            "client_order_list_id": "9wQuvisHlbJPQOwrJLhqgn",
            "is_open": False,
        }
    ),
    (
        None, "9wQuvisHlbJPQOwrJLhqgn",
        OCO_ORDER_INFO_RESPONSE,
        {
            "order_list_id": "77826706",
            "client_order_list_id": "9wQuvisHlbJPQOwrJLhqgn",
            "is_open": False,
        }
    ),
])
def test_oco_order_info(
        order_list_id, client_order_list_id, response_payload, expected_attrs, binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/api/v3/orderList\\?.*"), status=200, payload=response_payload
    )

    async def test_main():
        order_info = await binance_exchange.spot_account.get_oco_order_info(
            order_list_id=order_list_id, client_order_list_id=client_order_list_id
        )
        assert order_info is not None
        helpers.assert_expected_attrs(order_info, expected_attrs)

    asyncio.run(test_main())
