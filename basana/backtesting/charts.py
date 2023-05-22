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
import logging

from basana.backtesting.exchange import Exchange
from basana.core import bar, event, logs, helpers
from basana.core.enums import OrderOperation
from basana.core.pair import Pair

import plotly.graph_objects as go  # type: ignore
import plotly.subplots  # type: ignore


ChartDataPointFn = Callable[[datetime], Optional[Decimal]]
logger = logging.getLogger(__name__)


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
    def __init__(self, pair: Pair, include_buys: bool, include_sells: bool, exchange: Exchange, precision: int = 2):
        self._pair = pair
        self._include_buys = include_buys
        self._include_sells = include_sells
        self._exchange = exchange
        self._precision = precision
        self._ts = TimeSeries()
        self._indicators: Dict[str, Tuple[ChartDataPointFn, TimeSeries]] = {}

        exchange.subscribe_to_bar_events(pair, self._on_bar_event)

    def get_title(self) -> str:
        return str(self._pair)

    def add_traces(self, figure: go.Figure, row: int):
        # Add a trace with the pair values.
        x, y = self._ts.get_x_y()
        figure.add_trace(go.Scatter(x=x, y=y, name=str(self._pair)), row=row, col=1)

        # Add a trace with buy prices.
        if self._include_buys:
            x, y = self._get_order_fills(OrderOperation.BUY).get_x_y()
            figure.add_trace(
                go.Scatter(x=x, y=y, name="Buy", mode="markers", marker=dict(symbol="arrow-up")),
                row=row, col=1
            )

        # Add a trace with sell prices.
        if self._include_sells:
            x, y = self._get_order_fills(OrderOperation.SELL).get_x_y()
            figure.add_trace(
                go.Scatter(x=x, y=y, name="Sell", mode="markers", marker=dict(symbol="arrow-down")),
                row=row, col=1
            )

        # Add one trace per indicator.
        for name, (_, ts) in self._indicators.items():
            x, y = ts.get_x_y()
            figure.add_trace(go.Scatter(x=x, y=y, name=name), row=row, col=1)

    def add_indicator(self, name: str, get_data_point: ChartDataPointFn):
        assert name not in self._indicators
        self._indicators[name] = (get_data_point, TimeSeries())

    def _get_order_fills(self, op: OrderOperation) -> TimeSeries:
        ret = TimeSeries()
        orders = filter(
            lambda order: order.pair == self._pair and order.operation == op,
            self._exchange._get_all_orders()
        )
        for order in orders:
            for fill in order.fills:
                base_amount = fill.balance_updates[order.pair.base_symbol]
                quote_amount = fill.balance_updates[order.pair.quote_symbol]
                price = -helpers.truncate_decimal(quote_amount / base_amount, self._precision)
                ret.add_value(fill.when, price)
        return ret

    async def _on_bar_event(self, bar_event: bar.BarEvent):
        dt = bar_event.when
        value = bar_event.bar.close
        # Add the value to the main time series.
        self._ts.add_value(dt, value)
        # Add the custom indicator values.
        for get_data_point, ts in self._indicators.values():
            indicator_value = get_data_point(dt)
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


class PortfolioValueLineChart(LineChart):
    def __init__(self, symbol: str, exchange: Exchange, precision: int = 2):
        self._symbol = symbol
        self._exchange = exchange
        self._ts = TimeSeries()
        self._precision = precision

        # Initially I thought of having the exchange emit an event when any balance got updated, but I then realized
        # that it would be too much overhead if charts are not used.
        exchange._get_dispatcher().subscribe_all(self._on_any_event)

    def get_title(self) -> str:
        return f"Portfolio value in {self._symbol}"

    def add_traces(self, figure: go.Figure, row: int):
        # Add a trace with the portfolio values.
        x, y = self._ts.get_x_y()
        figure.add_trace(go.Scatter(x=x, y=y, name=f"Portfolio ({self._symbol})"), row=row, col=1)

    async def _on_any_event(self, event: event.Event):
        portfolio_value = Decimal(0)
        balances = await self._exchange.get_balances()
        for symbol, balance in balances.items():
            if balance.total == 0:
                continue

            rate: Optional[Decimal] = Decimal(1)
            if symbol != self._symbol:
                rate, _ = await self._exchange.get_bid_ask(Pair(symbol, self._symbol))

            if rate:
                portfolio_value += rate * balance.total
            else:
                logger.error(logs.StructuredMessage("Price missing", pair=Pair(symbol, self._symbol)))

        self._ts.add_value(event.when, helpers.round_decimal(portfolio_value, self._precision))


class CustomLineChart(LineChart):
    def __init__(self, name: str, exchange: Exchange):
        self._name = name
        self._exchange = exchange
        self._data_point_fns: Dict[str, Tuple[ChartDataPointFn, TimeSeries]] = {}

        exchange._get_dispatcher().subscribe_all(self._on_any_event)

    def get_title(self) -> str:
        return self._name

    def add_traces(self, figure: go.Figure, row: int):
        for name, (_, ts) in self._data_point_fns.items():
            x, y = ts.get_x_y()
            figure.add_trace(go.Scatter(x=x, y=y, name=name), row=row, col=1)

    def add_data_point_fn(self, name: str, get_data_point: ChartDataPointFn):
        assert name not in self._data_point_fns
        self._data_point_fns[name] = (get_data_point, TimeSeries())

    async def _on_any_event(self, event: event.Event):
        dt = event.when
        for get_data_point, ts in self._data_point_fns.values():
            value = get_data_point(dt)
            if value is not None:
                ts.add_value(dt, value)


class LineCharts:
    """A set of line charts that show the evolution of pair prices and account balances over time.

    :param exchange: The backtesting exchange.
    """
    def __init__(self, exchange: Exchange):
        self._exchange = exchange
        self._balance_charts: Dict[str, AccountBalanceLineChart] = collections.OrderedDict()
        self._pair_charts: Dict[Pair, PairLineChart] = collections.OrderedDict()
        self._portfolio_charts: Dict[str, PortfolioValueLineChart] = collections.OrderedDict()
        self._custom_charts: Dict[str, CustomLineChart] = collections.OrderedDict()

    def add_balance(self, symbol: str):
        """Adds a chart with an account's balance.

        :param symbol: The currency symbol.
        """
        self._balance_charts[symbol] = AccountBalanceLineChart(symbol, self._exchange)

    def add_portfolio_value(self, symbol: str):
        """Adds a chart with the portfolio value in a given currency.

        :param symbol: The currency symbol.

        .. note::

            * If the portfolio value can't be calculated at any given point, for example because there is no price for
              a given instrument, an error will be logged.
        """
        self._portfolio_charts[symbol] = PortfolioValueLineChart(symbol, self._exchange)

    def add_pair(self, pair: Pair, include_buys: bool = True, include_sells: bool = True):
        """Adds a chart with the pair values.

        :param pair: The pair.
        :param include_buys: True to include buy prices.
        :param include_sells: True to include sell prices.
        """
        self._pair_charts[pair] = PairLineChart(pair, include_buys, include_sells, self._exchange)

    def add_pair_indicator(self, name: str, pair: Pair, get_data_point: ChartDataPointFn):
        """Adds a technical indicator to a pair's chart.

        :param name: The name of the indicator.
        :param pair: The pair chart to add the indicator to.
        :param get_data_point: A callable that will be used to get the data point on each bar.
        """
        assert pair in self._pair_charts, f"{pair} was not added"
        self._pair_charts[pair].add_indicator(name, get_data_point)

    def add_custom(self, name: str, line: str, get_data_point: ChartDataPointFn):
        """Adds a custom chart.

        :param name: The name of the chart.
        :param line: The name of the line.
        :param get_data_point: A callable that will be used to get the line data points.
        """
        if (chart := self._custom_charts.get(name)) is None:
            chart = CustomLineChart(name, self._exchange)
            self._custom_charts[name] = chart
        chart.add_data_point_fn(line, get_data_point)

    def show(self, show_legend: bool = True):  # pragma: no cover
        """Shows the chart using either the default renderer(s).

        Check https://plotly.com/python-api-reference/generated/plotly.graph_objects.Figure.html#plotly.graph_objects.Figure.show
        for more information.

        :param show_legend: True if legends should be visible, False otherwise.
        """  # noqa: E501

        if fig := self._build_figure(show_legend=show_legend):
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

            * The supported file formats are png, jpg/jpeg, webp, svg and pdf.
        """

        if fig := self._build_figure(show_legend=show_legend):
            fig.write_image(path, width=width, height=height, scale=scale)

    def _build_figure(self, show_legend: bool = True) -> Optional[go.Figure]:
        charts: List[LineChart] = []
        charts.extend(self._pair_charts.values())
        charts.extend(self._balance_charts.values())
        charts.extend(self._portfolio_charts.values())
        charts.extend(self._custom_charts.values())

        figure = None
        if charts:
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
