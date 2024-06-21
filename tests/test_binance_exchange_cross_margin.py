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


CREATE_ORDER_RESPONSE_3286471275 = {
    "symbol": "DOGEUSDT",
    "orderId": 3286471275,
    "clientOrderId": "76168451F9EE4C9DAADC16D74B874E4B",
    "transactTime": 1671232394634,
    "isIsolated": False
}

ORDER_INFO_3286471275 = {
    "symbol": "DOGEUSDT",
    "orderId": 3286471275,
    "clientOrderId": "76168451F9EE4C9DAADC16D74B874E4B",
    "price": "0.05",
    "origQty": "1000",
    "executedQty": "0",
    "cummulativeQuoteQty": "0",
    "status": "CANCELED",
    "timeInForce": "GTC",
    "type": "STOP_LOSS_LIMIT",
    "side": "SELL",
    "stopPrice": "0.05",
    "icebergQty": "0",
    "time": 1671232394634,
    "updateTime": 1671232429037,
    "isWorking": False,
    "accountId": 207887936,
    "isIsolated": False
}

CREATE_ORDER_RESPONSE_12010477623 = {
    "symbol": "ETHUSDT",
    "orderId": 12010477623,
    "clientOrderId": "E1547A6979EA49A9A849A457BD751D38",
    "transactTime": 1671217574815,
    "price": "0",
    "origQty": "0.2503",
    "executedQty": "0.2503",
    "cummulativeQuoteQty": "299.962023",
    "status": "FILLED",
    "timeInForce": "GTC",
    "type": "MARKET",
    "side": "BUY",
    "fills": [
        {
            "price": "1198.41",
            "qty": "0.2503",
            "commission": "0.0002503",
            "commissionAsset": "ETH"
        }
    ],
    "marginBuyBorrowAsset": "USDT",
    "marginBuyBorrowAmount": "299.9548032",
    "isIsolated": False
}


ORDER_INFO_12010477623 = {
    "accountId": 207887936,
    "clientOrderId": "E1547A6979EA49A9A849A457BD751D38",
    "cummulativeQuoteQty": "299.962023",
    "executedQty": "0.2503",
    "icebergQty": "0",
    "isIsolated": False,
    "isWorking": True,
    "orderId": 12010477623,
    "origQty": "0.2503",
    "price": "0",
    "side": "BUY",
    "status": "FILLED",
    "stopPrice": "0",
    "symbol": "ETHUSDT",
    "time": 1671217574815,
    "timeInForce": "GTC",
    "type": "MARKET",
    "updateTime": 1671217574815
}


TRADES_ORDER_12010477623 = [
    {
        "symbol": "ETHUSDT",
        "id": 1043271755,
        "orderId": 12010477623,
        "price": "1198.41",
        "qty": "0.2503",
        "quoteQty": "299.962023",
        "commission": "0.0002503",
        "commissionAsset": "ETH",
        "time": 1671217574815,
        "isBuyer": True,
        "isMaker": False,
        "isBestMatch": True,
        "isIsolated": False
    }
]

CREATE_ORDER_RESPONSE_16422505508 = {
    "clientOrderId": "B28B24EE482A425EA1A07F343FB2F3EE",
    "cummulativeQuoteQty": "298.9864344",
    "executedQty": "0.01682",
    "fills": [
        {"commission": "0", "commissionAsset": "BNB", "price": "17775.62", "qty": "0.00417"},
        {"commission": "0", "commissionAsset": "BNB", "price": "17775.66", "qty": "0.00298"},
        {"commission": "0", "commissionAsset": "BNB", "price": "17775.66", "qty": "0.00967"}
    ],
    "isIsolated": False,
    "marginBuyBorrowAmount": "100.0869656",
    "marginBuyBorrowAsset": "USDT",
    "orderId": 16422505508,
    "origQty": "0.01682",
    "price": "17841.08",
    "side": "BUY",
    "status": "FILLED",
    "symbol": "BTCUSDT",
    "timeInForce": "GTC",
    "transactTime": 1670986059521,
    "type": "LIMIT"
}

ORDER_INFO_16422505508 = {
    "symbol": "BTCUSDT",
    "orderId": 16422505508,
    "clientOrderId": "B28B24EE482A425EA1A07F343FB2F3EE",
    "price": "17841.08",
    "origQty": "0.01682",
    "executedQty": "0.01682",
    "cummulativeQuoteQty": "298.9864344",
    "status": "FILLED",
    "timeInForce": "GTC",
    "type": "LIMIT",
    "side": "BUY",
    "stopPrice": "0",
    "icebergQty": "0",
    "time": 1670986059521,
    "updateTime": 1670986059521,
    "isWorking": True,
    "accountId": 207887936,
    "isIsolated": False
}

TRADES_ORDER_16422505508 = [
    {
        "symbol": "BTCUSDT",
        "id": 2327639624,
        "orderId": 16422505508,
        "price": "17775.62",
        "qty": "0.00417",
        "quoteQty": "74.1243354",
        "commission": "0",
        "commissionAsset": "BNB",
        "time": 1670986059521,
        "isBuyer": True,
        "isMaker": False,
        "isBestMatch": True,
        "isIsolated": False
    },
    {
        "symbol": "BTCUSDT",
        "id": 2327639625,
        "orderId": 16422505508,
        "price": "17775.66",
        "qty": "0.00298",
        "quoteQty": "52.9714668",
        "commission": "0",
        "commissionAsset": "BNB",
        "time": 1670986059521,
        "isBuyer": True,
        "isMaker": False,
        "isBestMatch": True,
        "isIsolated": False
    },
    {
        "symbol": "BTCUSDT",
        "id": 2327639626,
        "orderId": 16422505508,
        "price": "17775.66",
        "qty": "0.00967",
        "quoteQty": "171.8906322",
        "commission": "0",
        "commissionAsset": "BNB",
        "time": 1670986059521,
        "isBuyer": True,
        "isMaker": False,
        "isBestMatch": True,
        "isIsolated": False
    }
]

OCO_ORDER_INFO_79592962 = {
    "orderListId": 79592962,
    "contingencyType": "OCO",
    "listStatusType": "ALL_DONE",
    "listOrderStatus": "ALL_DONE",
    "listClientOrderId": "3hwrDggYdWhtiNAzm5bNDf",
    "transactionTime": 1671242905365,
    "symbol": "CHZUSDT",
    "isIsolated": False,
    "orders": [
        {
            "symbol": "CHZUSDT",
            "orderId": 1420705016,
            "clientOrderId": "MXTA6ncsHoWNyViZ5FQ2Cj"
        },
        {
            "symbol": "CHZUSDT",
            "orderId": 1420705017,
            "clientOrderId": "0EfhMRl4mHgY2yqvsJRV7e"
        }
    ]
}

CANCEL_ORDER_RESPONSE_16582318135 = {
    "orderId": "16582318135",
    "symbol": "BTCUSDT",
    "origClientOrderId": "FCE43038586A45EBB0DBF8AD0F360E5A",
    "clientOrderId": "4doKFpBuPJ1CEX8OSRa9qv",
    "price": "15000",
    "origQty": "0.001",
    "executedQty": "0",
    "cummulativeQuoteQty": "0",
    "status": "CANCELED",
    "timeInForce": "GTC",
    "type": "LIMIT",
    "side": "BUY",
    "isIsolated": False
}

CANCEL_OCO_ORDER_RESPONSE_79680111 = {
    "orderListId": 79680111,
    "contingencyType": "OCO",
    "listStatusType": "ALL_DONE",
    "listOrderStatus": "ALL_DONE",
    "listClientOrderId": "B3331893C53A4F1487EB78F2E16D4FDD",
    "transactionTime": 1671463475963,
    "symbol": "BTCUSDT",
    "isIsolated": False,
    "orders": [
        {
            "symbol": "BTCUSDT",
            "orderId": 16583687189,
            "clientOrderId": "dyLZCprGrE0we3SAdg1tqP"
        },
        {
            "symbol": "BTCUSDT",
            "orderId": 16583687190,
            "clientOrderId": "LhuSdrzXqqtsZsGNFrCRhw"
        }
    ],
    "orderReports": [
        {
            "symbol": "BTCUSDT",
            "origClientOrderId": "dyLZCprGrE0we3SAdg1tqP",
            "orderId": 16583687189,
            "clientOrderId": "JKrKa1n87ZxYISKH4ujAyR",
            "price": "19000.00000000",
            "origQty": "0.00100000",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "status": "CANCELED",
            "timeInForce": "GTC",
            "type": "STOP_LOSS_LIMIT",
            "side": "BUY",
            "stopPrice": "19000.00000000"
        },
        {
            "symbol": "BTCUSDT",
            "origClientOrderId": "LhuSdrzXqqtsZsGNFrCRhw",
            "orderId": 16583687190,
            "clientOrderId": "JKrKa1n87ZxYISKH4ujAyR",
            "price": "14000.00000000",
            "origQty": "0.00100000",
            "executedQty": "0",
            "cummulativeQuoteQty": "0",
            "status": "CANCELED",
            "timeInForce": "GTC",
            "type": "LIMIT_MAKER",
            "side": "BUY"
        }
    ]
}


def test_account_balances(binance_http_api_mock, binance_exchange):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/sapi/v1/margin/account\\?.*"), status=200,
        payload=helpers.load_json("binance_cross_margin_account_details.json")
    )

    async def test_main():
        balances = await binance_exchange.cross_margin_account.get_balances()

        balance = balances["USDT"]
        assert balance.available == Decimal("205.87545172")
        assert balance.total == Decimal("205.87545172")
        assert balance.locked == Decimal("0")

    asyncio.run(test_main())


@pytest.mark.parametrize("create_order_fun, response_payload, expected_attrs, expected_fills", [
    (
        lambda e: e.cross_margin_account.create_limit_order(
            exchange.OrderOperation.BUY, Pair("BTC", "USDT"), amount=Decimal("0.01682"),
            limit_price=Decimal("17841.08"), side_effect_type="MARGIN_BUY",
            client_order_id="B28B24EE482A425EA1A07F343FB2F3EE"
        ),
        CREATE_ORDER_RESPONSE_16422505508,
        {
            "id": "16422505508",
            "datetime": datetime.datetime(2022, 12, 14, 2, 47, 39, 521000).replace(tzinfo=datetime.timezone.utc),
            "client_order_id": "B28B24EE482A425EA1A07F343FB2F3EE",
            "limit_price": Decimal("17841.08"),
            "amount": Decimal("0.01682"),
            "amount_filled": Decimal("0.01682"),
            "quote_amount_filled": Decimal("298.9864344"),
            "status": "FILLED",
            "time_in_force": "GTC",
            "is_open": False,
        },
        [
            {
                "price": Decimal("17775.62"),
                "amount": Decimal("0.00417"),
                "commission": Decimal("0"),
                "commission_asset": "BNB",
            },
            {
                "price": Decimal("17775.66"),
                "amount": Decimal("0.00298"),
                "commission": Decimal("0"),
                "commission_asset": "BNB",
            },
            {
                "price": Decimal("17775.66"),
                "amount": Decimal("0.00967"),
                "commission": Decimal("0"),
                "commission_asset": "BNB",
            },
        ],
    ),
    (
        lambda e: e.cross_margin_account.create_market_order(
            exchange.OrderOperation.BUY, Pair("ETH", "USDT"), quote_amount=Decimal("300"),
            side_effect_type="MARGIN_BUY",
            client_order_id="E1547A6979EA49A9A849A457BD751D38"
        ),
        CREATE_ORDER_RESPONSE_12010477623,
        {
            "id": "12010477623",
            "datetime": datetime.datetime(2022, 12, 16, 19, 6, 14, 815000).replace(tzinfo=datetime.timezone.utc),
            "client_order_id": "E1547A6979EA49A9A849A457BD751D38",
            "limit_price": None,
            "amount": Decimal("0.2503"),
            "amount_filled": Decimal("0.2503"),
            "quote_amount_filled": Decimal("299.962023"),
            "status": "FILLED",
            "is_open": False,
        },
        [
            {
                "price": Decimal("1198.41"),
                "amount": Decimal("0.2503"),
                "commission": Decimal("0.0002503"),
                "commission_asset": "ETH",
            },
        ],
    ),
    (
        lambda e: e.cross_margin_account.create_stop_limit_order(
            exchange.OrderOperation.SELL, Pair("DOGE", "USDT"), Decimal("1000"), Decimal("0.05"), Decimal("0.05"),
            side_effect_type="MARGIN_BUY", client_order_id="76168451F9EE4C9DAADC16D74B874E4B"
        ),
        CREATE_ORDER_RESPONSE_3286471275,
        {
            "id": "3286471275",
            "datetime": datetime.datetime(2022, 12, 16, 23, 13, 14, 634000).replace(tzinfo=datetime.timezone.utc),
            "client_order_id": "76168451F9EE4C9DAADC16D74B874E4B",
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


@pytest.mark.parametrize(
    "pair, order_id, client_order_id, order_payload, trades_payload, expected_attrs, expected_first_trade",
    [
        (
            Pair("BTC", "USDT"), "16422505508", None,
            ORDER_INFO_16422505508,
            TRADES_ORDER_16422505508,
            {
                "id": "16422505508",
                "is_open": False,
                "amount": Decimal("0.01682"),
                "amount_filled": Decimal("0.01682"),
                "amount_remaining": Decimal("0"),
                "limit_price": Decimal("17841.08"),
                "fill_price": Decimal("17775.65008323424494649227111"),
                "fees": {},
            },
            {
                "id": "2327639624",
                "order_id": "16422505508",
                "price": Decimal("17775.62"),
                "amount": Decimal("0.00417"),
                "quote_amount": Decimal("74.1243354"),
                "commission": Decimal("0"),
                "commission_asset": "BNB",
                "datetime": datetime.datetime(2022, 12, 14, 2, 47, 39, 521000).replace(tzinfo=datetime.timezone.utc),
                "is_buyer": True,
                "is_maker": False,
                "is_best_match": True,
                "is_isolated": False,
            },
        ),
        (
            Pair("ETH", "USDT"), None, "E1547A6979EA49A9A849A457BD751D38",
            ORDER_INFO_12010477623,
            TRADES_ORDER_12010477623,
            {
                "id": "12010477623",
                "is_open": False,
                "amount": Decimal("0.2503"),
                "amount_filled": Decimal("0.2503"),
                "amount_remaining": Decimal("0"),
                "stop_price": None,
                "limit_price": None,
                "fill_price": Decimal("1198.41"),
                "fees": {"ETH": Decimal("0.0002503")},
            },
            {
                "id": "1043271755",
                "order_id": "12010477623",
                "price": Decimal("1198.41"),
                "amount": Decimal("0.2503"),
                "quote_amount": Decimal("299.962023"),
                "commission": Decimal("0.0002503"),
                "commission_asset": "ETH",
                "datetime": datetime.datetime(2022, 12, 16, 19, 6, 14, 815000).replace(tzinfo=datetime.timezone.utc),
                "is_buyer": True,
                "is_maker": False,
                "is_best_match": True,
                "is_isolated": False
            }
        ),
    ]
)
def test_order_info(
        pair, order_id, client_order_id, order_payload, trades_payload, expected_attrs, expected_first_trade,
        binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/sapi/v1/margin/order\\?.*"), status=200, payload=order_payload
    )
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/sapi/v1/margin/myTrades\\?.*"), status=200, payload=trades_payload
    )

    async def test_main():
        order_info = await binance_exchange.cross_margin_account.get_order_info(
            pair, order_id=order_id, client_order_id=client_order_id
        )
        assert order_info is not None
        helpers.assert_expected_attrs(order_info, expected_attrs)
        if expected_first_trade:
            helpers.assert_expected_attrs(order_info.trades[0], expected_first_trade)

    asyncio.run(test_main())


@pytest.mark.parametrize(
    "pair, open_orders_payload, expected_first_open_order",
    [
        (
            Pair("BTC", "USDT"),
            [
                {
                    "accountId": 207887936,
                    "clientOrderId": "web_19b6ce1dad4d45a0bb16a1599914ea14",
                    "cummulativeQuoteQty": "0",
                    "executedQty": "0",
                    "icebergQty": "0",
                    "isIsolated": False,
                    "isWorking": True,
                    "orderId": 18690114806,
                    "origQty": "0.01",
                    "price": "25000",
                    "side": "SELL",
                    "status": "NEW",
                    "stopPrice": "0",
                    "symbol": "BTCUSDT",
                    "time": 1676487828466,
                    "timeInForce": "GTC",
                    "type": "LIMIT",
                    "updateTime": 1676487828466
                }
            ],
            {
                "id": "18690114806",
                "client_order_id": "web_19b6ce1dad4d45a0bb16a1599914ea14",
                "quote_amount_filled": Decimal(0),
                "amount_filled": Decimal(0),
                "amount": Decimal("0.01"),
                "limit_price": Decimal("25000"),
                "stop_price": None,
                "operation": exchange.OrderOperation.SELL,
                "status": "NEW",
                "time_in_force": "GTC",
                "datetime": datetime.datetime(2023, 2, 15, 19, 3, 48, 466000).replace(tzinfo=datetime.timezone.utc),
                "type": "LIMIT",
            },
        ),
    ]
)
def test_get_open_orders(
        pair, open_orders_payload, expected_first_open_order, binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/sapi/v1/margin/openOrders\\?.*"), status=200, payload=open_orders_payload
    )

    async def test_main():
        open_orders = await binance_exchange.cross_margin_account.get_open_orders(pair)
        helpers.assert_expected_attrs(open_orders[0], expected_first_open_order)

    asyncio.run(test_main())


@pytest.mark.parametrize("pair, order_id, client_order_id, response_payload, expected_attrs", [
    (
        Pair("BTC", "USDT"), "16582318135", None,
        CANCEL_ORDER_RESPONSE_16582318135,
        {
            "id": "16582318135",
            "is_open": False,
            "order_list_id": None,
            "limit_price": Decimal(15000),
            "amount": Decimal("0.001"),
            "amount_filled": Decimal("0"),
            "quote_amount_filled": Decimal("0"),
            "status": "CANCELED",
            "time_in_force": "GTC",
            "operation": exchange.OrderOperation.BUY,
            "type": "LIMIT",
        }
    ),
    (
        Pair("BTC", "USDT"), None, "FCE43038586A45EBB0DBF8AD0F360E5A",
        CANCEL_ORDER_RESPONSE_16582318135,
        {
            "id": "16582318135",
            "is_open": False,
        }
    )
])
def test_cancel_order(
        pair, order_id, client_order_id, response_payload, expected_attrs, binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.delete(
        re.compile(r"http://binance.mock/sapi/v1/margin/order\\?.*"), status=200, payload=response_payload
    )

    async def test_main():
        canceled_order = await binance_exchange.cross_margin_account.cancel_order(
            pair, order_id=order_id, client_order_id=client_order_id
        )
        assert canceled_order is not None
        helpers.assert_expected_attrs(canceled_order, expected_attrs)

    asyncio.run(test_main())


@pytest.mark.parametrize("create_order_fun, response_payload, expected_attrs", [
    (
        lambda e: e.cross_margin_account.create_oco_order(
            exchange.OrderOperation.SELL, Pair("BTC", "USDT"), Decimal("0.01682"), Decimal("18125.4"),
            Decimal("17592.3"), stop_limit_price=Decimal("15833.07"), side_effect_type="AUTO_REPAY"
        ),
        {
            "contingencyType": "OCO",
            "isIsolated": False,
            "listClientOrderId": "8FFCC716C91841F199AD1D5DD14752E5",
            "listOrderStatus": "EXECUTING",
            "listStatusType": "EXEC_STARTED",
            "orderListId": 79453975,
            "orderReports": [
                {
                    "clientOrderId": "AjmD6hDJzPVeS6nmmE1ZDo",
                    "cummulativeQuoteQty": "0",
                    "executedQty": "0",
                    "orderId": 16422653053,
                    "orderListId": 79453975,
                    "origQty": "0.01682000",
                    "price": "15833.07000000",
                    "side": "SELL",
                    "status": "NEW",
                    "stopPrice": "17592.30000000",
                    "symbol": "BTCUSDT",
                    "timeInForce": "GTC",
                    "transactTime": 1670986467587,
                    "type": "STOP_LOSS_LIMIT"
                },
                {
                    "clientOrderId": "O5xo6nf8Ua0HbNMtpyqvv3",
                    "cummulativeQuoteQty": "0",
                    "executedQty": "0",
                    "orderId": 16422653054,
                    "orderListId": 79453975,
                    "origQty": "0.01682000",
                    "price": "18125.40000000",
                    "side": "SELL",
                    "status": "NEW",
                    "symbol": "BTCUSDT",
                    "timeInForce": "GTC",
                    "transactTime": 1670986467587,
                    "type": "LIMIT_MAKER"
                }
            ],
            "orders": [
                {"clientOrderId": "AjmD6hDJzPVeS6nmmE1ZDo", "orderId": 16422653053, "symbol": "BTCUSDT"},
                {"clientOrderId": "O5xo6nf8Ua0HbNMtpyqvv3", "orderId": 16422653054, "symbol": "BTCUSDT"}
            ],
            "symbol": "BTCUSDT",
            "transactionTime": 1670986467587
        },
        {
            "order_list_id": "79453975",
            "client_order_list_id": "8FFCC716C91841F199AD1D5DD14752E5",
            "datetime": datetime.datetime(2022, 12, 14, 2, 54, 27, 587000).replace(tzinfo=datetime.timezone.utc),
            "is_open": True,
            "limit_order_id": "16422653054",
            "stop_loss_order_id": "16422653053",
        },
    ),
])
def test_create_oco_order(
        create_order_fun, response_payload, expected_attrs, binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.post(
        re.compile(r"http://binance.mock/sapi/v1/margin/order/oco\\?.*"), status=200, payload=response_payload
    )

    async def test_main():
        order_created = await create_order_fun(binance_exchange)
        assert order_created is not None
        helpers.assert_expected_attrs(order_created, expected_attrs)

    asyncio.run(test_main())


@pytest.mark.parametrize("order_list_id, client_order_list_id, response_payload, expected_attrs", [
    (
        "79592962", None,
        OCO_ORDER_INFO_79592962,
        {
            "order_list_id": "79592962",
            "client_order_list_id": "3hwrDggYdWhtiNAzm5bNDf",
            "is_open": False,
        }
    ),
    (
        None, "3hwrDggYdWhtiNAzm5bNDf",
        OCO_ORDER_INFO_79592962,
        {
            "order_list_id": "79592962",
            "client_order_list_id": "3hwrDggYdWhtiNAzm5bNDf",
            "is_open": False,
        }
    ),
])
def test_oco_order_info(
        order_list_id, client_order_list_id, response_payload, expected_attrs, binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.get(
        re.compile(r"http://binance.mock/sapi/v1/margin/orderList\\?.*"), status=200, payload=response_payload
    )

    async def test_main():
        order_info = await binance_exchange.cross_margin_account.get_oco_order_info(
            order_list_id=order_list_id, client_order_list_id=client_order_list_id
        )
        assert order_info is not None
        helpers.assert_expected_attrs(order_info, expected_attrs)

    asyncio.run(test_main())


@pytest.mark.parametrize("pair, order_list_id, client_order_list_id, response_payload, expected_attrs", [
    (
        Pair("BTC", "USDT"), "79680111", None,
        CANCEL_OCO_ORDER_RESPONSE_79680111,
        {
            "order_list_id": "79680111",
            "client_order_list_id": "B3331893C53A4F1487EB78F2E16D4FDD",
            "datetime": datetime.datetime(2022, 12, 19, 15, 24, 35, 963000).replace(tzinfo=datetime.timezone.utc),
            "is_open": False,
        }
    ),
    (
        Pair("BTC", "USDT"), None, "B3331893C53A4F1487EB78F2E16D4FDD",
        CANCEL_OCO_ORDER_RESPONSE_79680111,
        {
            "order_list_id": "79680111",
            "client_order_list_id": "B3331893C53A4F1487EB78F2E16D4FDD",
            "datetime": datetime.datetime(2022, 12, 19, 15, 24, 35, 963000).replace(tzinfo=datetime.timezone.utc),
            "is_open": False,
        }
    )
])
def test_cancel_oco_order(
        pair, order_list_id, client_order_list_id, response_payload, expected_attrs,
        binance_http_api_mock, binance_exchange
):
    binance_http_api_mock.delete(
        re.compile(r"http://binance.mock/sapi/v1/margin/orderList\\?.*"), status=200, payload=response_payload
    )

    async def test_main():
        canceled_order = await binance_exchange.cross_margin_account.cancel_oco_order(
            pair, order_list_id=order_list_id, client_order_list_id=client_order_list_id
        )
        assert canceled_order is not None
        helpers.assert_expected_attrs(canceled_order, expected_attrs)

    asyncio.run(test_main())


def test_transfer(binance_http_api_mock, binance_exchange):
    async def test_main():
        binance_http_api_mock.post(
            re.compile(r"http://binance.mock/sapi/v1/margin/transfer\\?.*"), status=200,
            payload={"clientTag": "", "tranId": 124735427615}, repeat=True
        )

        assert (await binance_exchange.cross_margin_account.transfer_from_spot_account(
            "USDT", Decimal("100")))["tranId"] == 124735427615
        assert (await binance_exchange.cross_margin_account.transfer_to_spot_account(
            "USDT", Decimal("100")))["tranId"] == 124735427615

    asyncio.run(test_main())
