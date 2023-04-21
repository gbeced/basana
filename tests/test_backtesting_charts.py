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
import asyncio
import datetime
import os
import tempfile

import pytest

from .helpers import abs_data_path
from basana.backtesting import charts, exchange
from basana.core.pair import Pair
from basana.external.yahoo import bars


@pytest.mark.parametrize("order_plan", [
    {
        datetime.date(2000, 1, 3): [
            # Buy market.
            lambda e: e.create_market_order(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal("2")),
        ],
        datetime.date(2000, 1, 14): [
            # Sell market.
            lambda e: e.create_market_order(exchange.OrderOperation.SELL, Pair("ORCL", "USD"), Decimal("1")),
        ],
    },
])
def test_save_line_chart(order_plan, backtesting_dispatcher):
    e = exchange.Exchange(
        backtesting_dispatcher,
        {
            "USD": Decimal("1e6"),
        },
    )
    pair = Pair("ORCL", "USD")
    chart = charts.LineChart(e, [pair], ["USD"])

    async def on_bar(bar_event):
        order_requests = order_plan.get(bar_event.when.date(), [])
        for create_order_fun in order_requests:
            created_order = await create_order_fun(e)
            assert created_order is not None

    async def impl():
        e.add_bar_source(bars.CSVBarSource(pair, abs_data_path("orcl-2000-yahoo-sorted.csv")))
        e.subscribe_to_bar_events(pair, on_bar)

        await backtesting_dispatcher.run()

        # On Windows the name can't used to open the file a second time. That is why we're using this only to generate
        # the file name.
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp_file:
            pass
        try:
            chart.save(tmp_file.name)
            assert os.stat(tmp_file.name).st_size > 100
        finally:
            os.remove(tmp_file.name)

    asyncio.run(impl())
