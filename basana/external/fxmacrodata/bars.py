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
from typing import Any, Optional, Sequence
import datetime
import os

import aiohttp

from basana.core import bar, event
from basana.core.pair import Pair


API_BASE_URL = "https://fxmacrodata.com/api/v1"
DEFAULT_TIMEOUT = 30
DAILY_BAR_DURATION = datetime.timedelta(days=1)


class Error(Exception):
    pass


def _format_date(value: datetime.date | datetime.datetime | str) -> str:
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    return str(value)


def _row_to_bar_event(pair: Pair, row: dict, duration: datetime.timedelta = DAILY_BAR_DURATION) -> bar.BarEvent:
    date_value = row.get("date")
    value = row.get("val")
    if date_value is None or value is None:
        raise Error("FXMacroData rows must include 'date' and 'val'")

    begin = datetime.datetime.fromisoformat(str(date_value)).replace(tzinfo=datetime.timezone.utc)
    price = Decimal(str(value))
    # FXMacroData serves daily spot/reference values, not exchange traded candles.
    # Expose them as close-only OHLCV bars so Basana strategies can consume them.
    daily_bar = bar.Bar(begin, pair, price, price, price, price, Decimal("0"), duration)
    return bar.BarEvent(begin + duration, daily_bar)


def rows_to_bar_events(
        pair: Pair, rows: Sequence[dict], duration: datetime.timedelta = DAILY_BAR_DURATION
) -> Sequence[bar.BarEvent]:
    events = []
    for row in rows:
        events.append(_row_to_bar_event(pair, row, duration=duration))
    return sorted(events, key=lambda ev: ev.when)


class BarSource(event.FifoQueueEventSource, event.Producer):
    """Daily FX bar source backed by the FXMacroData REST API.

    FXMacroData returns one daily spot/reference value per currency pair. This
    source converts each row into a close-only :class:`basana.BarEvent` where
    open, high, low, and close are equal and volume is 0.
    """

    def __init__(
            self,
            pair: Pair,
            start_date: datetime.date | datetime.datetime | str,
            end_date: datetime.date | datetime.datetime | str,
            api_key: Optional[str] = None,
            base_url: str = API_BASE_URL,
            timeout: float = DEFAULT_TIMEOUT,
    ):
        super().__init__(producer=self)
        self._pair = pair
        self._start_date = start_date
        self._end_date = end_date
        self._api_key = api_key
        self._base_url = base_url
        self._timeout = timeout

    async def initialize(self):
        params = {
            "start_date": _format_date(self._start_date),
            "end_date": _format_date(self._end_date),
        }
        api_key = self._api_key or os.getenv("FXMACRODATA_API_KEY") or os.getenv("FXMD_API_KEY")
        if api_key:
            params["api_key"] = api_key

        url = "{}/forex/{}/{}".format(
            self._base_url.rstrip("/"),
            self._pair.base_symbol.lower(),
            self._pair.quote_symbol.lower(),
        )
        payload = await self._fetch_json(url, params)
        rows = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise Error("FXMacroData response did not include a data list")

        for bar_event in rows_to_bar_events(self._pair, rows):
            self.push(bar_event)

    async def _fetch_json(self, url: str, params: dict) -> Any:
        timeout = aiohttp.ClientTimeout(total=self._timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params, headers={"Accept": "application/json"}) as response:
                response.raise_for_status()
                return await response.json()
