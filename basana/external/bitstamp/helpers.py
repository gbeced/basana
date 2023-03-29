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

from typing import Dict
from urllib.parse import urlencode
import hashlib
import hmac
import time
import uuid

from basana.core import pair
from basana.core.enums import OrderOperation


def generate_nonce() -> str:
    return str(uuid.uuid4())


def get_auth_headers(
        hostname: str, api_key: str, api_secret: str, nonce: str, method: str, path: str,
        qs_params: Dict[str, str] = {}, data: Dict[str, str] = {}
) -> Dict[str, str]:
    assert path[0] == "/", "Leading slash is missing from path"

    timestamp = str(int(round(time.time() * 1000)))
    headers = {
        "X-Auth": "BITSTAMP {}".format(api_key),
        "X-Auth-Nonce": nonce,
        "X-Auth-Timestamp": timestamp,
        "X-Auth-Version": "v2",
    }
    if data:
        headers["Content-Type"] = "application/x-www-form-urlencoded"

    message = headers["X-Auth"]
    message += method.upper()
    message += hostname
    message += path
    if qs_params:  # pragma: no cover
        message += urlencode(qs_params)
    message += headers.get("Content-Type", "")
    message += headers["X-Auth-Nonce"]
    message += headers["X-Auth-Timestamp"]
    message += headers["X-Auth-Version"]
    if data:
        message += urlencode(data)

    headers["X-Auth-Signature"] = hmac.new(
        api_secret.encode(), msg=message.encode("utf-8"), digestmod=hashlib.sha256
    ).hexdigest()
    return headers


def pair_to_currency_pair(pair: pair.Pair) -> str:
    return "{}{}".format(pair.base_symbol.lower(), pair.quote_symbol.lower())


def order_type_to_order_operation(order_type: int) -> OrderOperation:
    ret = {
        0: OrderOperation.BUY,
        1: OrderOperation.SELL,
    }.get(order_type)
    assert ret is not None, f"Invalid order type {order_type}"
    return ret
