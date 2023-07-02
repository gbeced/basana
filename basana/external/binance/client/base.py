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
from typing import Any, Dict, Optional, Sequence, Tuple
from urllib.parse import urljoin
import asyncio
import copy
import time

import aiohttp

from basana.core import token_bucket, helpers as core_helpers
from basana.core.config import get_config_value
from basana.external.binance import config, helpers


class Error(Exception):
    """
    An error returned by the exchange.

    :param msg: The error message, if available.
    :param code: The error code, if available.
    :param resp: The response.
    :param json_response: The response body, if it was a JSON.
    """
    def __init__(self, msg: str, code: Optional[int], resp: aiohttp.ClientResponse, json_response: Optional[Any]):
        super().__init__(msg)
        #: The error message.
        self.msg = msg
        #: The error code, if available.
        self.code = code
        #: The HTTP status code.
        self.http_status = resp.status
        #: The HTTP reason.
        self.http_reason = resp.reason
        #: The response body, if it was a JSON.
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


class BaseClient:
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

    async def make_request(
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
                # print(await resp.text())
                json_response = None
                if (ct := resp.headers.get("Content-Type")) and ct.lower().find("application/json") == 0:
                    json_response = await resp.json()
                raise_for_error(resp, json_response)
                return json_response


def set_optional_params(params: Dict[str, Any], tuples: Sequence[Tuple[str, Any]]):
    for k, v in tuples:
        if v is None:
            continue
        if isinstance(v, Decimal):
            v = str(v)
        params[k] = v
