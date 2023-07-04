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
from typing import Any, Dict, List, Optional


from . import base


# https://binance-docs.github.io/apidocs/spot/en/#spot-account-trade
class SpotAccount:
    def __init__(self, client: base.BaseClient):
        self._client = client

    async def get_account_information(self) -> dict:
        return await self._client.make_request("GET", "/api/v3/account", send_sig=True)

    async def create_order(
            self, symbol: str, side: str, type: str, time_in_force: Optional[str] = None,
            quantity: Optional[Decimal] = None, quote_order_qty: Optional[Decimal] = None,
            price: Optional[Decimal] = None, stop_price: Optional[Decimal] = None,
            new_client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> dict:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": type,
        }
        base.set_optional_params(params, (
            ("timeInForce", time_in_force),
            ("quantity", quantity),
            ("quoteOrderQty", quote_order_qty),
            ("price", price),
            ("stopPrice", stop_price),
            ("newClientOrderId", new_client_order_id),
        ))
        params.update(kwargs)
        return await self._client.make_request("POST", "/api/v3/order", data=params, send_sig=True)

    async def query_order(
            self, symbol: str, order_id: Optional[int] = None, orig_client_order_id: Optional[str] = None
    ) -> dict:
        assert (order_id is not None) ^ (orig_client_order_id is not None), \
            "Either order_id or orig_client_order_id should be set"

        params: Dict[str, Any] = {"symbol": symbol}
        base.set_optional_params(params, (
            ("orderId", order_id),
            ("origClientOrderId", orig_client_order_id),
        ))
        return await self._client.make_request("GET", "/api/v3/order", qs_params=params, send_sig=True)

    async def get_open_orders(
            self, symbol: Optional[str] = None
    ) -> dict:
        params: Dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol
        return await self._client.make_request("GET", "/api/v3/openOrders", qs_params=params, send_sig=True)

    async def cancel_order(
            self, symbol: str, order_id: Optional[int] = None, orig_client_order_id: Optional[str] = None
    ) -> dict:
        assert (order_id is not None) ^ (orig_client_order_id is not None), \
            "Either order_id or orig_client_order_id should be set"

        params: Dict[str, Any] = {"symbol": symbol}
        base.set_optional_params(params, (
            ("orderId", order_id),
            ("origClientOrderId", orig_client_order_id),
        ))
        return await self._client.make_request("DELETE", "/api/v3/order", qs_params=params, send_sig=True)

    async def get_trades(self, symbol: str, order_id: Optional[int] = None) -> List[dict]:
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        return await self._client.make_request("GET", "/api/v3/myTrades", qs_params=params, send_sig=True)

    async def create_oco(
            self, symbol: str, side: str, quantity: Decimal, price: Decimal, stop_price: Decimal,
            stop_limit_price: Optional[Decimal] = None, stop_limit_time_in_force: Optional[str] = None,
            list_client_order_id: Optional[str] = None, limit_client_order_id: Optional[str] = None,
            stop_client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> dict:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "quantity": str(quantity),
            "price": str(price),
            "stopPrice": str(stop_price),
        }
        base.set_optional_params(params, (
            ("listClientOrderId", list_client_order_id),
            ("stopLimitPrice", stop_limit_price),
            ("stopLimitTimeInForce", stop_limit_time_in_force),
            ("limitClientOrderId", limit_client_order_id),
            ("stopClientOrderId", stop_client_order_id),
        ))
        params.update(kwargs)
        return await self._client.make_request("POST", "/api/v3/order/oco", data=params, send_sig=True)

    async def cancel_oco_order(
            self, symbol: str, order_list_id: Optional[int] = None, client_order_list_id: Optional[str] = None
    ) -> dict:
        assert (order_list_id is not None) ^ (client_order_list_id is not None), \
            "Either order_list_id or client_order_list_id should be set"

        params: Dict[str, Any] = {
            "symbol": symbol
        }
        base.set_optional_params(params, (
            ("orderListId", order_list_id),
            ("origClientOrderId", client_order_list_id),
        ))
        return await self._client.make_request("DELETE", "/api/v3/orderList", data=params, send_sig=True)

    async def query_oco_order(
            self, order_list_id: Optional[int] = None, client_order_list_id: Optional[str] = None
    ) -> dict:
        assert (order_list_id is not None) ^ (client_order_list_id is not None), \
            "Either order_list_id or client_order_list_id should be set"

        params: Dict[str, Any] = {}
        base.set_optional_params(params, (
            ("orderListId", order_list_id),
            ("origClientOrderId", client_order_list_id),
        ))
        return await self._client.make_request("GET", "/api/v3/orderList", qs_params=params, send_sig=True)
