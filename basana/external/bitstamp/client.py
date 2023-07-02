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
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import urlparse, urljoin
import asyncio

import aiohttp

from basana.core import token_bucket, helpers as core_helpers
from basana.core.config import get_config_value
from basana.external.bitstamp import config, helpers


class Error(Exception):
    def __init__(self, msg: str, resp: aiohttp.ClientResponse, json_response):
        super().__init__(msg)
        self.status = resp.status
        self.reason = resp.reason
        self.json_response = json_response


def raise_for_error(resp: aiohttp.ClientResponse, json_response):
    msg = None
    if isinstance(json_response, dict):
        if json_response.get("status") == "error":
            msg = json_response.get("reason", "")
        elif error := json_response.get("error"):
            msg = error
        elif (code := json_response.get("code")) and json_response.get("errors"):
            msg = code
    if msg is None and not resp.ok:
        msg = "{} {}".format(resp.status, resp.reason)
    if msg is not None:
        raise Error(msg, resp, json_response)


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
            self, method: str, path: str, authenticate: bool, qs_params: Dict[str, Any] = {},
            data: Dict[str, Any] = {}
    ) -> Any:
        # Throttling enabled ? go sleep if necessary.
        if self._tb and (sleep_time := self._tb.consume()):
            await asyncio.sleep(sleep_time)

        async with core_helpers.use_or_create_session(session=self._session) as session:
            headers = {}
            skip_auto_headers = ["Content-Type"]
            session_method = {
                "GET": session.get,
                "POST": session.post,
            }[method]
            base_url = get_config_value(config.DEFAULTS, "api.http.base_url", overrides=self._config_overrides)
            timeout = get_config_value(config.DEFAULTS, "api.http.timeout", overrides=self._config_overrides)
            url = urljoin(base_url, path)

            if authenticate:
                assert self._api_key is not None and self._api_secret is not None, "Missing credentials"
                hostname = urlparse(base_url).hostname
                assert hostname is not None, "hostname not set"
                nonce = helpers.generate_nonce()
                headers = helpers.get_auth_headers(
                    hostname, self._api_key, self._api_secret, nonce, method, path, qs_params=qs_params, data=data
                )
            async with session_method(
                    url, headers=headers, skip_auto_headers=skip_auto_headers, params=qs_params, data=data,
                    timeout=timeout
            ) as resp:
                json_response = None
                if (ct := resp.headers.get("Content-Type")) and ct.lower().find("application/json") == 0:
                    json_response = await resp.json()
                raise_for_error(resp, json_response)
                return json_response

    async def get_trading_pairs_info(self) -> List[Dict]:
        return await self._make_request("GET", "/api/v2/trading-pairs-info/", False)

    async def get_order_book(self, currency_pair: str, group: Optional[int] = None) -> dict:
        params = {}
        if group is not None:
            params = {"group": group}
        return await self._make_request("GET", f"/api/v2/order_book/{currency_pair}/", False, qs_params=params)

    async def get_ticker(self, currency_pair: str) -> dict:
        return await self._make_request("GET", f"/api/v2/ticker/{currency_pair}/", False)

    async def get_ohlc_data(
            self, currency_pair: str, step: int, limit: int, start: Optional[int] = None, end: Optional[int] = None,
            exclude_current_candle: bool = False
    ) -> dict:
        assert start is None or end is None, "both start and end should not be set"

        params: Dict[str, Any] = {
            "step": step,
            "limit": limit,
        }
        set_optional_params(params, (
            ("start", start),
            ("end", end),
        ))
        if exclude_current_candle:
            params["exclude_current_candle"] = "true"
        return await self._make_request("GET", f"/api/v2/ohlc/{currency_pair}/", False, qs_params=params)

    async def get_websocket_auth_token(self) -> dict:
        return await self._make_request("POST", "/api/v2/websockets_token/", True)

    async def get_account_balances(self) -> List[dict]:
        return await self._make_request("POST", "/api/v2/account_balances/", True)

    async def get_account_balance(self, currency: str) -> dict:
        return await self._make_request("POST", f"/api/v2/account_balances/{currency}/", True)

    async def get_open_orders(self, currency_pair: Optional[str] = None) -> List[dict]:
        url = "/api/v2/open_orders/{}/".format("all" if currency_pair is None else currency_pair)
        return await self._make_request("POST", url, True)

    async def get_order_status(
            self, id: Optional[Union[str, int]] = None, client_order_id: Optional[str] = None,
            omit_transactions: Optional[bool] = None
    ) -> dict:
        assert (id is not None) ^ (client_order_id is not None), "Either id or client_order_id should be set"

        data: Dict[str, Any] = {"id": id} if id else {"client_order_id": client_order_id}
        if omit_transactions:
            data["omit_transactions"] = omit_transactions
        return await self._make_request("POST", "/api/v2/order_status/", True, data=data)

    async def cancel_order(self, id: Union[str, int]) -> dict:
        return await self._make_request("POST", "/api/v2/cancel_order/", True, data={"id": id})

    async def create_market_order(
            self, action: str, currency_pair: str, amount: Decimal, client_order_id: Optional[str] = None,
            **kwargs: Dict[str, Any]
    ) -> dict:
        assert action in ["buy", "sell"], "Invalid action"

        data: Dict[str, Any] = {
            "amount": str(amount),
        }
        if client_order_id:
            data["client_order_id"] = client_order_id
        data.update(kwargs)
        return await self._make_request("POST", f"/api/v2/{action}/market/{currency_pair}/", True, data=data)

    async def create_limit_order(
            self, action: str, currency_pair: str, amount: Decimal, price: Decimal,
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> dict:
        assert action in ["buy", "sell"], "Invalid action"

        data: Dict[str, Any] = {
            "amount": str(amount),
            "price": str(price),
        }
        if client_order_id:
            data["client_order_id"] = client_order_id
        data.update(kwargs)
        return await self._make_request("POST", f"/api/v2/{action}/{currency_pair}/", True, data=data)

    async def create_instant_order(
            self, action: str, currency_pair: str, amount: Decimal, amount_in_counter: bool = False,
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> dict:
        assert action in ["buy", "sell"], "Invalid action"
        assert amount_in_counter is False or action == "sell", "amount_in_counter only supported for sell orders"

        data: Dict[str, Any] = {
            "amount": str(amount),
        }
        set_optional_params(data, (
            ("client_order_id", client_order_id),
            ("amount_in_counter", amount_in_counter),
        ))
        data.update(kwargs)
        return await self._make_request("POST", f"/api/v2/{action}/instant/{currency_pair}/", True, data=data)


def set_optional_params(params: Dict[str, Any], tuples: Sequence[Tuple[str, Any]]):
    for k, v in tuples:
        if v is None:
            continue
        # if isinstance(v, Decimal):
        #     v = str(v)
        params[k] = v
