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
from typing import Any, Dict, List, Optional
import abc
import json

from . import base


class MarginAccount(metaclass=abc.ABCMeta):
    def __init__(self, client: base.BaseClient):
        self._client = client

    @property
    @abc.abstractmethod
    def is_isolated(self) -> bool:
        raise NotImplementedError()

    async def create_order(
            self, symbol: str, side: str, type: str, time_in_force: Optional[str] = None,
            quantity: Optional[Decimal] = None, quote_order_qty: Optional[Decimal] = None,
            price: Optional[Decimal] = None, stop_price: Optional[Decimal] = None,
            new_client_order_id: Optional[str] = None, side_effect_type: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> dict:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "isIsolated": self.is_isolated,
            "type": type,
        }
        base.set_optional_params(params, (
            ("timeInForce", time_in_force),
            ("quantity", quantity),
            ("quoteOrderQty", quote_order_qty),
            ("price", price),
            ("stopPrice", stop_price),
            ("newClientOrderId", new_client_order_id),
            ("sideEffectType", side_effect_type),
        ))
        params.update(kwargs)
        return await self._client.make_request("POST", "/sapi/v1/margin/order", data=params, send_sig=True)

    async def query_order(
            self, symbol: str, order_id: Optional[int] = None, orig_client_order_id: Optional[str] = None
    ) -> dict:
        assert (order_id is not None) ^ (orig_client_order_id is not None), \
            "Either order_id or orig_client_order_id should be set"

        params: Dict[str, Any] = {
            "symbol": symbol,
            "isIsolated": json.dumps(self.is_isolated),
        }
        base.set_optional_params(params, (
            ("orderId", order_id),
            ("origClientOrderId", orig_client_order_id),
        ))
        return await self._client.make_request("GET", "/sapi/v1/margin/order", qs_params=params, send_sig=True)

    async def get_open_orders(
            self, symbol: Optional[str] = None
    ) -> dict:
        params: Dict[str, Any] = {"isIsolated": json.dumps(self.is_isolated)}
        if symbol is not None:
            params["symbol"] = symbol
        return await self._client.make_request("GET", "/sapi/v1/margin/openOrders", qs_params=params, send_sig=True)

    async def cancel_order(
            self, symbol: str, order_id: Optional[int] = None, orig_client_order_id: Optional[str] = None
    ) -> dict:
        assert (order_id is not None) ^ (orig_client_order_id is not None), \
            "Either order_id or orig_client_order_id should be set"

        params: Dict[str, Any] = {
            "symbol": symbol,
            "isIsolated": json.dumps(self.is_isolated),
        }
        base.set_optional_params(params, (
            ("orderId", order_id),
            ("origClientOrderId", orig_client_order_id),
        ))
        return await self._client.make_request("DELETE", "/sapi/v1/margin/order", qs_params=params, send_sig=True)

    async def get_trades(self, symbol: str, order_id: Optional[int] = None) -> List[dict]:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "isIsolated": json.dumps(self.is_isolated),
        }
        if order_id is not None:
            params["orderId"] = order_id
        return await self._client.make_request("GET", "/sapi/v1/margin/myTrades", qs_params=params, send_sig=True)

    async def create_oco(
            self, symbol: str, side: str, quantity: Decimal, price: Decimal, stop_price: Decimal,
            stop_limit_price: Optional[Decimal] = None, stop_limit_time_in_force: Optional[str] = None,
            list_client_order_id: Optional[str] = None, side_effect_type: Optional[str] = None,
            limit_client_order_id: Optional[str] = None, stop_client_order_id: Optional[str] = None,
            **kwargs: Dict[str, Any]
    ) -> dict:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "quantity": str(quantity),
            "price": str(price),
            "stopPrice": str(stop_price),
            "isIsolated": self.is_isolated,
        }
        base.set_optional_params(params, (
            ("listClientOrderId", list_client_order_id),
            ("stopLimitPrice", stop_limit_price),
            ("stopLimitTimeInForce", stop_limit_time_in_force),
            ("sideEffectType", side_effect_type),
            ("limitClientOrderId", limit_client_order_id),
            ("stopClientOrderId", stop_client_order_id),
        ))
        params.update(kwargs)
        return await self._client.make_request("POST", "/sapi/v1/margin/order/oco", data=params, send_sig=True)

    async def query_oco_order(
            self, order_list_id: Optional[int] = None, client_order_list_id: Optional[str] = None
    ) -> dict:
        assert (order_list_id is not None) ^ (client_order_list_id is not None), \
            "Either order_list_id or client_order_list_id should be set"

        params: Dict[str, Any] = {
            "isIsolated": json.dumps(self.is_isolated),
        }
        base.set_optional_params(params, (
            ("orderListId", order_list_id),
            ("origClientOrderId", client_order_list_id),
        ))
        return await self._client.make_request("GET", "/sapi/v1/margin/orderList", qs_params=params, send_sig=True)

    async def cancel_oco_order(
            self, symbol: str, order_list_id: Optional[int] = None, client_order_list_id: Optional[str] = None
    ) -> dict:
        assert (order_list_id is not None) ^ (client_order_list_id is not None), \
            "Either order_list_id or client_order_list_id should be set"

        params: Dict[str, Any] = {
            "symbol": symbol,
            "isIsolated": json.dumps(self.is_isolated),
        }
        base.set_optional_params(params, (
            ("orderListId", order_list_id),
            ("origClientOrderId", client_order_list_id),
        ))
        return await self._client.make_request("DELETE", "/sapi/v1/margin/orderList", data=params, send_sig=True)


class CrossMarginAccount(MarginAccount):
    @property
    def is_isolated(self) -> bool:
        return False

    async def transfer_from_spot_account(self, asset: str, amount: Decimal) -> dict:
        params: Dict[str, Any] = {
            "asset": asset,
            "amount": amount,
            "type": 1,
        }
        return await self._client.make_request("POST", "/sapi/v1/margin/transfer", send_sig=True, data=params)

    async def transfer_to_spot_account(self, asset: str, amount: Decimal) -> dict:
        params: Dict[str, Any] = {
            "asset": asset,
            "amount": amount,
            "type": 2,
        }
        return await self._client.make_request("POST", "/sapi/v1/margin/transfer", send_sig=True, data=params)

    async def get_account_information(self) -> dict:
        return await self._client.make_request("GET", "/sapi/v1/margin/account", send_sig=True)

    async def create_listen_key(self) -> dict:
        return await self._client.make_request("POST", "/sapi/v1/userDataStream", send_key=True)

    async def keep_alive_listen_key(self, listen_key: str) -> dict:
        params: Dict[str, Any] = {
            "listenKey": listen_key,
        }
        return await self._client.make_request("PUT", "/sapi/v1/userDataStream", send_key=True, data=params)


class IsolatedMarginAccount(MarginAccount):
    @property
    def is_isolated(self) -> bool:
        return True

    async def transfer_from_spot_account(self, asset: str, symbol: str, amount: Decimal) -> dict:
        params: Dict[str, Any] = {
            "asset": asset,
            "symbol": symbol,
            "amount": str(amount),
            "transFrom": "SPOT",
            "transTo": "ISOLATED_MARGIN",
        }
        return await self._client.make_request("POST", "/sapi/v1/margin/isolated/transfer", send_sig=True, data=params)

    async def transfer_to_spot_account(self, asset: str, symbol: str, amount: Decimal) -> dict:
        params: Dict[str, Any] = {
            "asset": asset,
            "symbol": symbol,
            "amount": str(amount),
            "transFrom": "ISOLATED_MARGIN",
            "transTo": "SPOT",
        }
        return await self._client.make_request("POST", "/sapi/v1/margin/isolated/transfer", send_sig=True, data=params)

    async def get_account_information(self) -> dict:
        return await self._client.make_request("GET", "/sapi/v1/margin/isolated/account", send_sig=True)

    async def create_listen_key(self, symbol: str) -> dict:
        params: Dict[str, Any] = {
            "symbol": symbol,
        }
        return await self._client.make_request("POST", "/sapi/v1/userDataStream/isolated", send_key=True, data=params)

    async def keep_alive_listen_key(self, symbol: str, listen_key: str) -> dict:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "listenKey": listen_key,
        }
        return await self._client.make_request("PUT", "/sapi/v1/userDataStream/isolated", send_key=True, data=params)
