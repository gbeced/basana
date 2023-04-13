# Basana
#
# Copyright 2022-2023 Gabriel Martin Becedillas Ruiz
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
from typing import Dict, Optional, Sequence
import collections
import datetime

from . import helpers
from basana.core.enums import OrderOperation


class Balance:
    def __init__(self, json: dict):
        self.json = json

    @property
    def available(self) -> Decimal:
        """The available balance."""
        return Decimal(self.json["free"])

    @property
    def total(self) -> Decimal:
        """The total balance (available + locked)."""
        return self.available + self.locked

    @property
    def locked(self) -> Decimal:
        """The locked balance."""
        return Decimal(self.json["locked"])


class Trade:
    def __init__(self, json: dict):
        self.json = json

    @property
    def id(self) -> str:
        """The trade id."""
        return str(self.json["id"])

    @property
    def order_id(self) -> str:
        """The order id."""
        return str(self.json["orderId"])

    @property
    def datetime(self) -> datetime.datetime:
        return helpers.timestamp_to_datetime(self.json["time"])

    @property
    def is_best_match(self) -> bool:
        return self.json["isBestMatch"]

    @property
    def is_buyer(self) -> bool:
        return self.json["isBuyer"]

    @property
    def is_maker(self) -> bool:
        return self.json["isMaker"]

    @property
    def price(self) -> Decimal:
        return Decimal(self.json["price"])

    @property
    def amount(self) -> Decimal:
        return Decimal(self.json["qty"])

    @property
    def quote_amount(self) -> Decimal:
        return Decimal(self.json["quoteQty"])

    @property
    def commission(self) -> Decimal:
        return Decimal(self.json["commission"])

    @property
    def commission_asset(self) -> str:
        return self.json["commissionAsset"]


class OrderWrapper:
    def __init__(self, json: dict):
        self.json = json

    @property
    def id(self) -> str:
        """The order id."""
        return str(self.json["orderId"])

    @property
    def client_order_id(self) -> str:
        """The client order id."""
        return self.json["clientOrderId"]

    @property
    def order_list_id(self) -> Optional[str]:
        """The order list id."""
        ret = self.json.get("orderListId")
        ret = None if ret in [None, -1] else str(ret)
        return ret

    @property
    def status(self) -> str:
        """The status.

        Check **Order status** in https://binance-docs.github.io/apidocs/spot/en/#public-api-definitions.
        """
        return self.json["status"]

    @property
    def is_open(self) -> bool:
        """True if the order is open, False otherwise."""
        return helpers.order_status_is_open(self.status)

    @property
    def amount(self) -> Decimal:
        """The amount."""
        return Decimal(self.json["origQty"])

    @property
    def amount_filled(self) -> Decimal:
        """The amount filled."""
        return Decimal(self.json["executedQty"])

    @property
    def quote_amount_filled(self) -> Decimal:
        """The amount filled in quote units."""
        return Decimal(self.json["cummulativeQuoteQty"])

    @property
    def limit_price(self) -> Optional[Decimal]:
        """The limit price."""
        return helpers.get_optional_decimal(self.json, "price", True)

    @property
    def stop_price(self) -> Optional[Decimal]:
        """The stop price."""
        return helpers.get_optional_decimal(self.json, "stopPrice", True)

    @property
    def time_in_force(self) -> Optional[str]:
        """The time in force.

        Check **Time in force** in https://binance-docs.github.io/apidocs/spot/en/#public-api-definitions.
        """
        return self.json.get("timeInForce")


class OrderInfo(OrderWrapper):
    def __init__(self, json: dict, trades: Sequence[Trade]):
        super().__init__(json)
        self.trades = trades
        self._fees: Dict[str, Decimal] = collections.defaultdict(Decimal)
        for trade in trades:
            if trade.commission:
                self._fees[trade.commission_asset] += trade.commission

    @property
    def amount_remaining(self) -> Decimal:
        """The amount remaining to be filled."""
        return self.amount - self.amount_filled

    @property
    def fill_price(self) -> Optional[Decimal]:
        """The fill price."""
        ret = None
        if self.amount_filled:
            ret = self.quote_amount_filled / self.amount_filled
        return ret

    @property
    def fees(self) -> Dict[str, Decimal]:
        """The fees."""
        return self._fees


class Fill:
    def __init__(self, json: dict):
        self.json = json

    @property
    def price(self) -> Decimal:
        """The price."""
        return Decimal(self.json["price"])

    @property
    def amount(self) -> Decimal:
        """The amount."""
        return Decimal(self.json["qty"])

    @property
    def commission(self) -> Decimal:
        """The commission."""
        return Decimal(self.json["commission"])

    @property
    def commission_asset(self) -> str:
        """The commission asset."""
        return self.json["commissionAsset"]


class CreatedOrder:
    def __init__(self, json: dict):
        self.json = json

    @property
    def id(self) -> str:
        """The order id."""
        return str(self.json["orderId"])

    @property
    def datetime(self) -> datetime.datetime:
        """The creation datetime."""
        return helpers.timestamp_to_datetime(self.json["transactTime"])

    @property
    def client_order_id(self) -> str:
        """The client order id."""
        return self.json["clientOrderId"]

    @property
    def limit_price(self) -> Optional[Decimal]:
        """The limit price.

        Only available for RESULT / FULL responses.
        """
        return helpers.get_optional_decimal(self.json, "price", True)

    @property
    def amount(self) -> Optional[Decimal]:
        """The amount.

        Only available for RESULT / FULL responses.
        """
        return helpers.get_optional_decimal(self.json, "origQty", False)

    @property
    def amount_filled(self) -> Optional[Decimal]:
        """The amount filled.

        Only available for RESULT / FULL responses.
        """
        return helpers.get_optional_decimal(self.json, "executedQty", False)

    @property
    def quote_amount_filled(self) -> Optional[Decimal]:
        """The amount filled in quote units.

        Only available for RESULT / FULL responses.
        """
        return helpers.get_optional_decimal(self.json, "cummulativeQuoteQty", False)

    @property
    def status(self) -> Optional[str]:
        """The status.

        Only available for RESULT / FULL responses.
        """
        return self.json.get("status")

    @property
    def time_in_force(self) -> Optional[str]:
        """The time in force.

        Only available for RESULT / FULL responses.
        """
        return self.json.get("timeInForce")

    @property
    def is_open(self) -> bool:
        """True if the order is open, False otherwise.

        Only available for RESULT / FULL responses.
        """
        assert self.status is not None, "status not set"
        return helpers.order_status_is_open(self.status)


class CanceledOrder(OrderWrapper):
    @property
    def operation(self) -> OrderOperation:
        """The operation."""
        return helpers.side_to_order_operation(self.json["side"])

    @property
    def type(self) -> str:
        """The type of order.

        Check **Order types** in https://binance-docs.github.io/apidocs/spot/en/#public-api-definitions.
        """
        return self.json["type"]


class OpenOrder(OrderWrapper):
    @property
    def datetime(self) -> datetime.datetime:
        """The creation datetime."""
        return helpers.timestamp_to_datetime(self.json["time"])

    @property
    def operation(self) -> OrderOperation:
        """The operation."""
        return helpers.side_to_order_operation(self.json["side"])

    @property
    def type(self) -> str:
        """The type of order.

        Check **Order types** in https://binance-docs.github.io/apidocs/spot/en/#public-api-definitions.
        """
        return self.json["type"]


class OCOOrderWrapper:
    def __init__(self, json: dict):
        self.json = json

    @property
    def order_list_id(self) -> str:
        """The order list id."""
        return str(self.json["orderListId"])

    @property
    def client_order_list_id(self) -> str:
        """A client id for the order list."""
        return str(self.json["listClientOrderId"])

    @property
    def datetime(self) -> datetime.datetime:
        """The creation datetime."""
        return helpers.timestamp_to_datetime(self.json["transactionTime"])

    @property
    def is_open(self) -> bool:
        """True if the order is open, False otherwise."""
        return helpers.oco_order_status_is_open(self.json["listOrderStatus"])

    @property
    def limit_order_id(self) -> str:
        """The id for the limit order."""
        order_ids = [
            str(order["orderId"])
            for order in self.json.get("orderReports", [])
            if order["type"] in ("LIMIT", "LIMIT_MAKER")
        ]
        return order_ids[0]

    @property
    def stop_loss_order_id(self) -> str:
        """The id for the stop loss order."""
        order_ids = [
            str(order["orderId"])
            for order in self.json.get("orderReports", [])
            if order["type"] in ("STOP_LOSS", "STOP_LOSS_LIMIT")
        ]
        return order_ids[0]


class CreatedOCOOrder(OCOOrderWrapper):
    pass


class OCOOrderInfo(OCOOrderWrapper):
    pass


class CanceledOCOOrder(OCOOrderWrapper):
    pass
