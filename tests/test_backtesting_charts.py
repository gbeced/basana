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
import os

import pytest

from . import helpers
from basana.backtesting import charts, exchange
from basana.core.pair import Pair
from basana.external.yahoo import bars

order_plan = {
    datetime.date(2000, 1, 4): [
        # Buy market.
        lambda e: e.create_market_order(exchange.OrderOperation.BUY, Pair("ORCL", "USD"), Decimal("2")),
    ],
    datetime.date(2000, 1, 14): [
        # Sell market.
        lambda e: e.create_market_order(exchange.OrderOperation.SELL, Pair("ORCL", "USD"), Decimal("1")),
    ],
}

@pytest.mark.parametrize("order_plan, candlesticks, include_buys, include_sells", [
    (order_plan, True, True, True),
    (order_plan, False, True, True),
    (order_plan, True, False, False),
])
async def test_save_line_chart(order_plan, candlesticks, include_buys, include_sells, backtesting_dispatcher, caplog):
    e = exchange.Exchange(
        backtesting_dispatcher,
        {
            "USD": Decimal("1e6"),
            "BTC": Decimal("0"),
        },
    )
    pair = Pair("ORCL", "USD")
    line_charts = charts.LineCharts(e)
    line_charts.add_pair(pair, include_buys=include_buys, include_sells=include_sells, candlesticks=candlesticks)
    line_charts.add_balance("USD")
    line_charts.add_pair_indicator(
        "CONSTANT", pair, charts.DataPointFromSequence([100]), marker={"symbol": "arrow"}
    )
    # Add an indicator with no marker (empty dict) and one that returns None.
    line_charts.add_pair_indicator("NO_MARKER", pair, charts.DataPointFromSequence([50]))
    line_charts.add_pair_indicator("SOMETIMES_NONE", pair, charts.DataPointFromSequence([]))
    line_charts.add_portfolio_value("USD")
    line_charts.add_portfolio_value("INVALID")
    # Add a custom chart with a no-marker line and one that returns None.
    line_charts.add_custom("CUSTOM", "line_name", lambda _: 3, marker={"symbol": "arrow"})
    line_charts.add_custom("CUSTOM", "no_marker_line", lambda _: 1)
    line_charts.add_custom("CUSTOM", "sometimes_none", lambda _: None)

    async def on_bar(bar_event):
        order_requests = order_plan.get(bar_event.when.date(), [])
        for create_order_fun in order_requests:
            created_order = await create_order_fun(e)
            assert created_order is not None

    e.add_bar_source(bars.CSVBarSource(pair, helpers.abs_data_path("orcl-2000-yahoo-sorted.csv")))
    e.subscribe_to_bar_events(pair, on_bar)

    await backtesting_dispatcher.run()

    with helpers.temp_file_name(suffix=".png") as tmp_file_name:
        line_charts.save(tmp_file_name)
        assert os.stat(tmp_file_name).st_size > 100


def test_data_point_from_empty_sequence():
    dp = charts.DataPointFromSequence([])
    assert dp(datetime.datetime.now()) is None


def test_build_figure_no_charts(backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {})
    line_charts = charts.LineCharts(e)
    # When no charts are added, _build_figure returns None and save is a no-op.
    assert line_charts._build_figure() is None
    with helpers.temp_file_name(suffix=".png", delete=False) as tmp_file_name:
        # Delete the file first so we can verify save() doesn't create it.
        os.unlink(tmp_file_name)
        line_charts.save(tmp_file_name)
        assert not os.path.exists(tmp_file_name)