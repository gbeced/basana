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
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union
import abc
import collections

from basana.backtesting.exchange import Exchange
from basana.core import bar, event, helpers
from basana.core.pair import Pair

import plotly.graph_objects as go  # type: ignore
import plotly.subplots  # type: ignore


ChartDataPointFn = Callable[[datetime], Optional[Decimal]]


class TimeSeries:
    def __init__(self):
        self._values = {}

    def add_value(self, dt: datetime, value: Decimal):
        self._values[dt] = value

    def get_x_y(self):
        return zip(*sorted(self._values.items())) if self._values else ([], [])


class LineChart(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_title(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def add_traces(self, figure: go.Figure, row: int):
        raise NotImplementedError()


class PairLineChart(LineChart):
    def __init__(self, pair: Pair, exchange: Exchange):
        self._pair = pair
        self._exchange = exchange
        self._ts = TimeSeries()
        self._indicators: Dict[str, Tuple[ChartDataPointFn, TimeSeries]] = {}

        exchange.subscribe_to_bar_events(pair, self._on_bar_event)

    def get_title(self) -> str:
        return str(self._pair)

    def add_traces(self, figure: go.Figure, row: int):
        # Add a trace with the pair values.
        x, y = self._ts.get_x_y()
        figure.add_trace(go.Scatter(x=x, y=y, name=str(self._pair)), row=row, col=1)

        # Create a timeseries with buy prices and another one with sell prices.
        buy_prices = TimeSeries()
        sell_prices = TimeSeries()
        for order in filter(lambda order: order.pair == self._pair, self._exchange._get_all_orders()):
            for fill in order.fills:
                base_amount = fill.balance_updates[order.pair.base_symbol]
                quote_amount = fill.balance_updates[order.pair.quote_symbol]
                price = -helpers.truncate_decimal(quote_amount / base_amount, 2)
                fills = buy_prices if base_amount > 0 else sell_prices
                fills.add_value(fill.when, helpers.truncate_decimal(price, 2))

        # Add a trace with the buy prices and another one with the sell prices.
        for fill_prices, name, symbol in (
                (buy_prices, "Buy", "arrow-up"),
                (sell_prices, "Sell", "arrow-down"),
        ):
            x, y = fill_prices.get_x_y()
            figure.add_trace(
                go.Scatter(x=x, y=y, name=name, mode="markers", marker=dict(symbol=symbol)),
                row=row, col=1
            )

        # Add one trace per indicator.
        for name, (_, ts) in self._indicators.items():
            x, y = ts.get_x_y()
            figure.add_trace(go.Scatter(x=x, y=y, name=name), row=row, col=1)

    def add_indicator(self, name: str, get_data_point: ChartDataPointFn):
        assert name not in self._indicators
        self._indicators[name] = (get_data_point, TimeSeries())

    async def _on_bar_event(self, bar_event: bar.BarEvent):
        dt = bar_event.when
        value = bar_event.bar.close
        # Add the value to the main time series.
        self._ts.add_value(dt, value)
        # Add the value indicators
        for extractor, ts in self._indicators.values():
            indicator_value = extractor(dt)
            if indicator_value is not None:
                ts.add_value(dt, indicator_value)


class AccountBalanceLineChart(LineChart):
    def __init__(self, symbol: str, exchange: Exchange):
        self._symbol = symbol
        self._exchange = exchange
        self._ts = TimeSeries()

        # Initially I thought of having the exchange emit an event when any balance got updated, but I then realized
        # that it would be too much overhead if charts are not used.
        exchange._get_dispatcher().subscribe_all(self._on_any_event)

    def get_title(self) -> str:
        return f"{self._symbol} balance"

    def add_traces(self, figure: go.Figure, row: int):
        # Add a trace with the pair values.
        x, y = self._ts.get_x_y()
        figure.add_trace(go.Scatter(x=x, y=y, name=self._symbol), row=row, col=1)

    async def _on_any_event(self, event: event.Event):
        balance = await self._exchange.get_balance(self._symbol)
        self._ts.add_value(event.when, balance.total)


class DataPointFromSequence:
    """Callable that returns the last value of a sequence if its not empty.

    :param seq: The sequence that will be used to get the value.
    """
    def __init__(self, seq: Sequence[Any]):
        self._seq = seq

    def __call__(self, dt: datetime) -> Optional[Decimal]:
        ret = None
        if self._seq:
            ret = self._seq[-1]
        return Decimal(ret) if ret is not None else ret


class LineCharts:
    """A set of line charts that show the evolution of pair prices and account balances over time.

    :param exchange: The backtesting exchange.
    :param pairs: The trading pairs to include in the chart.
    :param balance_symbols: The symbols for the balances to include in the chart.
    """
    def __init__(self, exchange: Exchange, pairs: Sequence[Pair], balance_symbols: Sequence[str]):
        self._balance_charts: List[LineChart] = [
            AccountBalanceLineChart(symbol, exchange) for symbol in balance_symbols
        ]
        self._pair_charts: Dict[Pair, PairLineChart] = collections.OrderedDict()
        for pair in pairs:
            self._pair_charts[pair] = PairLineChart(pair, exchange)

    def add_pair_indicator(self, name: str, pair: Pair, get_data_point: ChartDataPointFn):
        """Adds a technical indicator to a pair's chart.

        :param name: The name of the indicator.
        :param pair: The pair chart to add the indicator to.
        :param get_data_point: A callable that will be used to get a data point on each bar.
        """
        assert pair in self._pair_charts
        self._pair_charts[pair].add_indicator(name, get_data_point)

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

            * The Supported file formats are png, jpg/jpeg, webp, svg and pdf.
        """

        fig = self._build_figure(show_legend=show_legend)
        fig.write_image(path, width=width, height=height, scale=scale)

    def _build_figure(self, show_legend: bool = True) -> go.Figure:
        charts: List[LineChart] = []
        charts.extend(self._pair_charts.values())
        charts.extend(self._balance_charts)

        subplot_titles = [chart.get_title() for chart in charts]
        figure = plotly.subplots.make_subplots(
            rows=len(charts), cols=1, shared_xaxes=True, subplot_titles=subplot_titles
        )

        row = 1
        for chart in charts:
            chart.add_traces(figure, row)
            row += 1

        figure.layout.update(showlegend=show_legend)

        return figure
