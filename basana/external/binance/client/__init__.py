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

from typing import Any, Dict, Optional

import aiohttp

from . import base, margin, spot
from basana.core import token_bucket


Error = base.Error


class APIClient:
    def __init__(
            self, api_key: Optional[str] = None, api_secret: Optional[str] = None,
            session: Optional[aiohttp.ClientSession] = None, tb: Optional[token_bucket.TokenBucketLimiter] = None,
            config_overrides: dict = {}
    ):
        self._client = base.BaseClient(
            api_key=api_key, api_secret=api_secret, session=session, tb=tb, config_overrides=config_overrides
        )

    async def get_exchange_info(self, symbol: Optional[str] = None) -> dict:
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await self._client.make_request("GET", "/api/v3/exchangeInfo", qs_params=params)

    @property
    def spot_account(self) -> spot.SpotAccount:
        return spot.SpotAccount(self._client)

    @property
    def cross_margin_account(self) -> margin.CrossMarginAccount:
        return margin.CrossMarginAccount(self._client)

    @property
    def isolated_margin_account(self) -> margin.IsolatedMarginAccount:
        return margin.IsolatedMarginAccount(self._client)

    async def get_order_book(self, symbol: str, limit: Optional[int] = None) -> dict:
        params: Dict[str, Any] = {"symbol": symbol}
        if limit is not None:
            params["limit"] = limit
        return await self._client.make_request("GET", "/api/v3/depth", qs_params=params)

    async def get_candlestick_data(
            self, symbol: str, interval: str, start_time: Optional[int] = None, end_time: Optional[int] = None,
            limit: Optional[int] = None
    ) -> list:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
        }
        base.set_optional_params(params, (
            ("startTime", start_time),
            ("endTime", end_time),
            ("limit", limit),
        ))
        return await self._client.make_request("GET", "/api/v3/klines", qs_params=params)
