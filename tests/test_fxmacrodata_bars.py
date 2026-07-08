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
import datetime

from basana.core.pair import Pair
from basana.external.fxmacrodata import bars


def test_rows_to_bar_events_creates_close_only_daily_bars():
    pair = Pair("EUR", "USD")
    events = bars.rows_to_bar_events(
        pair,
        [
            {"date": "2024-01-03", "val": "1.0920"},
            {"date": "2024-01-01", "val": 1.1038},
        ],
    )

    assert len(events) == 2
    assert events[0].when == datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)
    assert events[0].bar.datetime == datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    assert events[0].bar.pair == pair
    assert events[0].bar.open == Decimal("1.1038")
    assert events[0].bar.high == Decimal("1.1038")
    assert events[0].bar.low == Decimal("1.1038")
    assert events[0].bar.close == Decimal("1.1038")
    assert events[0].bar.volume == Decimal("0")


async def test_bar_source_fetches_fxmacrodata_rows(monkeypatch):
    captured = {}

    async def fake_fetch_json(self, url, params):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = self._timeout
        return {"data": [{"date": "2024-01-01", "val": "1.1038"}]}

    monkeypatch.setattr(bars.BarSource, "_fetch_json", fake_fetch_json)

    src = bars.BarSource(
        Pair("EUR", "USD"),
        datetime.date(2024, 1, 1),
        "2024-01-31",
        api_key="test-key",
        timeout=12,
    )
    await src.initialize()

    event = src.pop()
    assert event.bar.close == Decimal("1.1038")
    assert src.pop() is None
    assert captured == {
        "url": "https://fxmacrodata.com/api/v1/forex/eur/usd",
        "params": {
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "api_key": "test-key",
        },
        "timeout": 12,
    }
