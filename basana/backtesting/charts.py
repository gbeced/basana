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

from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, Sequence, Union

from basana.backtesting.exchange import Exchange
from basana.core import bar, helpers
from basana.core.pair import Pair

import plotly.graph_objects as go  # type: ignore
import plotly.subplots  # type: ignore


portfolio_key = "portfolio"


class TimeSeries:
    def __init__(self):
        self._values = {}

    def add_value(self, dt: datetime, value: Decimal):
        self._values[dt] = value

    def get_x_y(self):
        return zip(*sorted(self._values.items()))


class LineChart:
    """A line chart that shows the evolution of pair prices and account balances over time.

    :param exchange: The backtesting exchange.
    :param pairs: The trading pairs to include in the chart.
    :param balances: The balances to include in the chart.
    """
    def __init__(self, exchange: Exchange, pairs: Sequence[Pair], balances: Sequence[str]):
        self._exchange = exchange
        self._pair_values: Dict[Pair, TimeSeries] = {pair: TimeSeries() for pair in pairs}
        self._balance_values: Dict[str, TimeSeries] = {symbol: TimeSeries() for symbol in balances}

        for pair in pairs:
            exchange.subscribe_to_bar_events(pair, self._on_bar_event)

    async def _add_balances(self, dt: datetime):
        balances = await self._exchange.get_balances()
        for symbol, balance in balances.items():
            if symbol in self._balance_values:
                self._balance_values[symbol].add_value(dt, balance.available)

    async def _on_bar_event(self, bar_event: bar.BarEvent):
        self._pair_values[bar_event.bar.pair].add_value(bar_event.when, bar_event.bar.close)
        await self._add_balances(bar_event.when)

    def show(self, show_legend: bool = True):  # pragma: no cover
        """Shows the chart using either the default renderer(s).

        Check https://plotly.com/python-api-reference/generated/plotly.graph_objects.Figure.html#plotly.graph_objects.Figure.show
        for more information.

        :param show_legend: True if legends should be visible, False otherwise.
        """  # noqa: E501

        fig = self._build_figure(show_legend=show_legend)
        fig.show()

    def save(
            self, path: str, width: Optional[int] = None, height: Optional[int] = None,
            scale: Optional[Union[int, float]] = None, show_legend: bool = True
    ):
        """Saves the chart to a file.

        :param path: The path to the file to save the image.
        :param width: The width of the exported image in layout pixels.
        :param height: The height of the exported image in layout pixels.
        :param scale: The scale factor to use when exporting the figure.
        :param show_legend: True if legends should be visible, False otherwise.

        .. note::

            * The Supported file formats are png, jpg/jpeg, webp, svg and pdf
        """

        fig = self._build_figure(show_legend=show_legend)
        fig.write_image(path, width=width, height=height, scale=scale)

    def _build_figure(self, show_legend: bool = True) -> go.Figure:
        # Organize fill prices by pair and datetime.
        buy_prices: Dict[Pair, TimeSeries] = {}
        sell_prices: Dict[Pair, TimeSeries] = {}
        for order in self._exchange._get_all_orders():
            for fill in order.fills:
                base_amount = fill.balance_updates[order.pair.base_symbol]
                quote_amount = fill.balance_updates[order.pair.quote_symbol]
                price = -helpers.truncate_decimal(quote_amount / base_amount, 2)
                fills = buy_prices if base_amount > 0 else sell_prices
                fills.setdefault(order.pair, TimeSeries()).add_value(
                    fill.when,
                    helpers.truncate_decimal(price, 2)
                )

        # Create subplots.
        subplot_titles = [str(pair) for pair in self._pair_values.keys()] + \
            [f"{symbol} balance" for symbol in self._balance_values.keys()]
        fig = plotly.subplots.make_subplots(
            rows=len(self._pair_values) + len(self._balance_values), cols=1, shared_xaxes=True,
            subplot_titles=subplot_titles
        )

        row = 1
        # Add pair values along with order fills.
        for pair, timeseries in self._pair_values.items():
            # Add a trace with the pair values.
            x, y = timeseries.get_x_y()
            fig.add_trace(go.Scatter(x=x, y=y, name=str(pair)), row=row, col=1)
            # Add a trace with the buy prices and another one with the sell prices.
            for fill_prices, name, symbol in (
                    (buy_prices, "Buy", "arrow-up"),
                    (sell_prices, "Sell", "arrow-down"),
            ):
                x, y = fill_prices[pair].get_x_y()
                fig.add_trace(
                    go.Scatter(x=x, y=y, name=name, mode="markers", marker=dict(symbol=symbol)),
                    row=row, col=1
                )
            row += 1

        # Add balances.
        for symbol, timeseries in self._balance_values.items():
            # Add a trace with the balance values.
            x, y = timeseries.get_x_y()
            fig.add_trace(go.Scatter(x=x, y=y, name=symbol), row=row, col=1)
            row += 1

        fig.layout.update(showlegend=show_legend)

        return fig
