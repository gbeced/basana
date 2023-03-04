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

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

from decimal import Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin
import abc
import asyncio
import copy
import json
import time

import aiohttp

from . import config, helpers
from basana.core import token_bucket, helpers as core_helpers
from basana.core.config import get_config_value


class Error(Exception):
    def __init__(self, msg: str, code: Optional[int], resp: aiohttp.ClientResponse, json_response):
        super().__init__(msg)
        self.msg = msg
        self.code = code
        self.http_status = resp.status
        self.http_reason = resp.reason
        self.json_response = json_response


def raise_for_error(resp: aiohttp.ClientResponse, json_response):
    msg = None
    code = None
    if isinstance(json_response, dict):
        msg = json_response.get("msg")
        code = json_response.get("code")
    if msg is None and not resp.ok:
        msg = "{} {}".format(resp.status, resp.reason)
    if msg is not None:
        raise Error(msg, code, resp, json_response)


class APIClient:
    def __init__(
            self, api_key: Optional[str] = None, api_secret: Optional[str] = None,
            session: Optional[aiohttp.ClientSession] = None, tb: Optional[token_bucket.TokenBucketLimiter] = None,
            config_overrides: dict = {}
    ):
        assert not ((api_key is None) ^ (api_secret is None)), \
            "Both api_key and api_secret should be set, or none of them"

        self._api_key = api_key
        self._api_secret = api_secret
        self._session = session
        self._tb = tb
        self._config_overrides = config_overrides

    async def _make_request(
            self, method: str, path: str, send_key: bool = False, send_sig: bool = False,
            qs_params: Dict[str, Any] = {}, data: Dict[str, Any] = {}
    ) -> Any:
        if self._tb and (sleep_time := self._tb.consume()):
            await asyncio.sleep(sleep_time)

        async with core_helpers.use_or_create_session(session=self._session) as session:
            headers = {}
            session_method = {
                "GET": session.get,
                "POST": session.post,
                "DELETE": session.delete,
            }.get(method)
            assert session_method is not None

            base_url = get_config_value(config.DEFAULTS, "api.http.base_url", overrides=self._config_overrides)
            timeout = get_config_value(config.DEFAULTS, "api.http.timeout", overrides=self._config_overrides)
            url = urljoin(base_url, path)

            if send_key or send_sig:
                assert self._api_key, "api_key not set"

                headers["X-MBX-APIKEY"] = self._api_key

            if send_sig:
                assert self._api_secret, "api_secret not set"

                qs_params = copy.copy(qs_params)
                # Signature and timestamp should go in the query string, and the timestamp should be included in the
                # signature.
                qs_params["timestamp"] = int(round(time.time() * 1000))
                qs_params["signature"] = helpers.get_signature(self._api_secret, qs_params=qs_params, data=data)

            form_data = None if not data else aiohttp.FormData(data)
            async with session_method(url, headers=headers, params=qs_params, data=form_data, timeout=timeout) as resp:
                json_response = None
                if (ct := resp.headers.get("Content-Type")) and ct.lower().find("application/json") == 0:
                    json_response = await resp.json()
                raise_for_error(resp, json_response)
                # print(await resp.text())
                return json_response

    async def get_exchange_info(self, symbol: Optional[str] = None) -> dict:
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await self._make_request("GET", "/api/v3/exchangeInfo", qs_params=params)

    @property
    def spot_account(self) -> "SpotAccount":
        return SpotAccount(self)

    @property
    def cross_margin_account(self) -> "CrossMarginAccount":
        return CrossMarginAccount(self)

    @property
    def isolated_margin_account(self) -> "IsolatedMarginAccount":
        return IsolatedMarginAccount(self)

    async def get_order_book(self, symbol: str, limit: Optional[int] = None) -> dict:
        params: Dict[str, Any] = {"symbol": symbol}
        if limit is not None:
            params["limit"] = limit
        return await self._make_request("GET", "/api/v3/depth", qs_params=params)


# https://binance-docs.github.io/apidocs/spot/en/#spot-account-trade
class SpotAccount:
    def __init__(self, client: APIClient):
        self._client = client

    async def get_account_information(self) -> dict:
        return await self._client._make_request("GET", "/api/v3/account", send_sig=True)

    async def create_order(
            self, symbol: str, side: str, type: str, time_in_force: Optional[str] = None,
            quantity: Optional[Decimal] = None, quote_order_qty: Optional[Decimal] = None,
            price: Optional[Decimal] = None, stop_price: Optional[Decimal] = None,
            new_client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> dict:
        params: Dict[str, Any] = {}
        params.update(kwargs)
        params.update({
            "symbol": symbol,
            "side": side,
            "type": type,
        })
        for name, value in {
            "timeInForce": time_in_force,
            "quantity": quantity,
            "quoteOrderQty": quote_order_qty,
            "price": price,
            "stopPrice": stop_price,
            "newClientOrderId": new_client_order_id
        }.items():
            if value is None:
                continue
            if isinstance(value, Decimal):
                value = str(value)
            params[name] = value
        return await self._client._make_request("POST", "/api/v3/order", data=params, send_sig=True)

    async def query_order(
            self, symbol: str, order_id: Optional[int] = None, orig_client_order_id: Optional[str] = None
    ) -> dict:
        assert (order_id is not None) ^ (orig_client_order_id is not None), \
            "Either order_id or orig_client_order_id should be set"

        params: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id
        return await self._client._make_request("GET", "/api/v3/order", qs_params=params, send_sig=True)

    async def get_open_orders(
            self, symbol: Optional[str] = None
    ) -> dict:
        params: Dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = symbol
        return await self._client._make_request("GET", "/api/v3/openOrders", qs_params=params, send_sig=True)

    async def cancel_order(
            self, symbol: str, order_id: Optional[int] = None, orig_client_order_id: Optional[str] = None
    ) -> dict:
        assert (order_id is not None) ^ (orig_client_order_id is not None), \
            "Either order_id or orig_client_order_id should be set"

        params: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id
        return await self._client._make_request("DELETE", "/api/v3/order", qs_params=params, send_sig=True)

    async def get_trades(self, symbol: str, order_id: Optional[int] = None) -> List[dict]:
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        return await self._client._make_request("GET", "/api/v3/myTrades", qs_params=params, send_sig=True)

    async def create_oco(
            self, symbol: str, side: str, quantity: Decimal, price: Decimal, stop_price: Decimal,
            stop_limit_price: Optional[Decimal] = None, stop_limit_time_in_force: Optional[str] = None,
            list_client_order_id: Optional[str] = None, limit_client_order_id: Optional[str] = None,
            stop_client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> dict:
        params: Dict[str, Any] = {}
        params.update(kwargs)
        params.update({
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "stopPrice": stop_price,
        })
        for name, value in {
            "listClientOrderId": list_client_order_id,
            "stopLimitPrice": stop_limit_price,
            "stopLimitTimeInForce": stop_limit_time_in_force,
            "limitClientOrderId": limit_client_order_id,
            "stopClientOrderId": stop_client_order_id,
        }.items():
            if value is None:
                continue
            if isinstance(value, Decimal):
                value = str(value)
            params[name] = value
        return await self._client._make_request("POST", "/api/v3/order/oco", data=params, send_sig=True)

    async def cancel_oco_order(
            self, symbol: str, order_list_id: Optional[int] = None, client_order_list_id: Optional[str] = None
    ) -> dict:
        assert (order_list_id is not None) ^ (client_order_list_id is not None), \
            "Either order_list_id or client_order_list_id should be set"

        params: Dict[str, Any] = {"symbol": symbol}
        if order_list_id is not None:
            params["orderListId"] = order_list_id
        if client_order_list_id:
            params["origClientOrderId"] = client_order_list_id
        return await self._client._make_request("DELETE", "/api/v3/orderList", data=params, send_sig=True)

    async def query_oco_order(
            self, order_list_id: Optional[int] = None, client_order_list_id: Optional[str] = None
    ) -> dict:
        assert (order_list_id is not None) ^ (client_order_list_id is not None), \
            "Either order_list_id or client_order_list_id should be set"

        params: Dict[str, Any] = {}
        if order_list_id is not None:
            params["orderListId"] = order_list_id
        if client_order_list_id:
            params["origClientOrderId"] = client_order_list_id
        return await self._client._make_request("GET", "/api/v3/orderList", qs_params=params, send_sig=True)


class MarginAccount(metaclass=abc.ABCMeta):
    def __init__(self, client: APIClient):
        self._client = client

    @property
    @abc.abstractmethod
    def is_isolated(self) -> bool:  # pragma: no cover
        raise NotImplementedError()

    async def create_order(
            self, symbol: str, side: str, type: str, time_in_force: Optional[str] = None,
            quantity: Optional[Decimal] = None, quote_order_qty: Optional[Decimal] = None,
            price: Optional[Decimal] = None, stop_price: Optional[Decimal] = None,
            new_client_order_id: Optional[str] = None, side_effect_type: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> dict:
        params: Dict[str, Any] = {}
        params.update(kwargs)
        params.update({
            "symbol": symbol,
            "side": side,
            "isIsolated": self.is_isolated,
            "type": type,
        })
        for name, value in {
            "timeInForce": time_in_force,
            "quantity": quantity,
            "quoteOrderQty": quote_order_qty,
            "price": price,
            "stopPrice": stop_price,
            "newClientOrderId": new_client_order_id,
            "sideEffectType": side_effect_type,
        }.items():
            if value is None:
                continue
            if isinstance(value, Decimal):
                value = str(value)
            params[name] = value
        return await self._client._make_request("POST", "/sapi/v1/margin/order", data=params, send_sig=True)

    async def query_order(
            self, symbol: str, order_id: Optional[int] = None, orig_client_order_id: Optional[str] = None
    ) -> dict:
        assert (order_id is not None) ^ (orig_client_order_id is not None), \
            "Either order_id or orig_client_order_id should be set"

        params: Dict[str, Any] = {
            "symbol": symbol,
            "isIsolated": json.dumps(self.is_isolated),
        }
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id
        return await self._client._make_request("GET", "/sapi/v1/margin/order", qs_params=params, send_sig=True)

    async def get_open_orders(
            self, symbol: Optional[str] = None
    ) -> dict:
        params: Dict[str, Any] = {"isIsolated": json.dumps(self.is_isolated)}
        if symbol is not None:
            params["symbol"] = symbol
        return await self._client._make_request("GET", "/sapi/v1/margin/openOrders", qs_params=params, send_sig=True)

    async def cancel_order(
            self, symbol: str, order_id: Optional[int] = None, orig_client_order_id: Optional[str] = None
    ) -> dict:
        assert (order_id is not None) ^ (orig_client_order_id is not None), \
            "Either order_id or orig_client_order_id should be set"

        params: Dict[str, Any] = {
            "symbol": symbol,
            "isIsolated": json.dumps(self.is_isolated),
        }
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id
        return await self._client._make_request("DELETE", "/sapi/v1/margin/order", qs_params=params, send_sig=True)

    async def get_trades(self, symbol: str, order_id: Optional[int] = None) -> List[dict]:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "isIsolated": json.dumps(self.is_isolated),
        }
        if order_id is not None:
            params["orderId"] = order_id
        return await self._client._make_request("GET", "/sapi/v1/margin/myTrades", qs_params=params, send_sig=True)

    async def create_oco(
            self, symbol: str, side: str, quantity: Decimal, price: Decimal, stop_price: Decimal,
            stop_limit_price: Optional[Decimal] = None, stop_limit_time_in_force: Optional[str] = None,
            list_client_order_id: Optional[str] = None, side_effect_type: Optional[str] = None,
            limit_client_order_id: Optional[str] = None, stop_client_order_id: Optional[str] = None,
            **kwargs: Dict[str, Any]
    ) -> dict:
        params: Dict[str, Any] = {}
        params.update(kwargs)
        params.update({
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "stopPrice": stop_price,
            "isIsolated": self.is_isolated,
        })
        for name, value in {
            "listClientOrderId": list_client_order_id,
            "stopLimitPrice": stop_limit_price,
            "stopLimitTimeInForce": stop_limit_time_in_force,
            "sideEffectType": side_effect_type,
            "limitClientOrderId": limit_client_order_id,
            "stopClientOrderId": stop_client_order_id,

        }.items():
            if value is None:
                continue
            if isinstance(value, Decimal):
                value = str(value)
            params[name] = value
        return await self._client._make_request("POST", "/sapi/v1/margin/order/oco", data=params, send_sig=True)

    async def query_oco_order(
            self, order_list_id: Optional[int] = None, client_order_list_id: Optional[str] = None
    ) -> dict:
        assert (order_list_id is not None) ^ (client_order_list_id is not None), \
            "Either order_list_id or client_order_list_id should be set"

        params: Dict[str, Any] = {
            "isIsolated": json.dumps(self.is_isolated),
        }
        if order_list_id is not None:
            params["orderListId"] = order_list_id
        if client_order_list_id:
            params["origClientOrderId"] = client_order_list_id
        return await self._client._make_request("GET", "/sapi/v1/margin/orderList", qs_params=params, send_sig=True)

    async def cancel_oco_order(
            self, symbol: str, order_list_id: Optional[int] = None, client_order_list_id: Optional[str] = None
    ) -> dict:
        assert (order_list_id is not None) ^ (client_order_list_id is not None), \
            "Either order_list_id or client_order_list_id should be set"

        params: Dict[str, Any] = {
            "symbol": symbol,
            "isIsolated": json.dumps(self.is_isolated),
        }
        if order_list_id is not None:
            params["orderListId"] = order_list_id
        if client_order_list_id:
            params["origClientOrderId"] = client_order_list_id
        return await self._client._make_request("DELETE", "/sapi/v1/margin/orderList", data=params, send_sig=True)


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
        return await self._client._make_request("POST", "/sapi/v1/margin/transfer", send_sig=True, data=params)

    async def transfer_to_spot_account(self, asset: str, amount: Decimal) -> dict:
        params: Dict[str, Any] = {
            "asset": asset,
            "amount": amount,
            "type": 2,
        }
        return await self._client._make_request("POST", "/sapi/v1/margin/transfer", send_sig=True, data=params)

    async def get_account_information(self) -> dict:
        return await self._client._make_request("GET", "/sapi/v1/margin/account", send_sig=True)


class IsolatedMarginAccount(MarginAccount):
    @property
    def is_isolated(self) -> bool:
        return True

    async def transfer_from_spot_account(self, asset: str, symbol: str, amount: Decimal) -> dict:
        params: Dict[str, Any] = {
            "asset": asset,
            "symbol": symbol,
            "amount": amount,
            "transFrom": "SPOT",
            "transTo": "ISOLATED_MARGIN",
        }
        return await self._client._make_request("POST", "/sapi/v1/margin/isolated/transfer", send_sig=True, data=params)

    async def transfer_to_spot_account(self, asset: str, symbol: str, amount: Decimal) -> dict:
        params: Dict[str, Any] = {
            "asset": asset,
            "symbol": symbol,
            "amount": amount,
            "transFrom": "ISOLATED_MARGIN",
            "transTo": "SPOT",
        }
        return await self._client._make_request("POST", "/sapi/v1/margin/isolated/transfer", send_sig=True, data=params)

    async def get_account_information(self) -> dict:
        return await self._client._make_request("GET", "/sapi/v1/margin/isolated/account", send_sig=True)
