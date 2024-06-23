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

# Bars can be downloaded using this command:
# python -m basana.external.binance.tools.download_bars -c BTC/USDT -p 1d -s 2021-01-01 -e 2021-12-31 \
# -o binance_btcusdt_day.csv

from decimal import Decimal
import asyncio
import datetime
import logging

from basana.backtesting import charts, lending
from basana.core.logs import StructuredMessage
from basana.external.binance import csv
import basana as bs
import basana.backtesting.exchange as backtesting_exchange

from samples.backtesting import position_manager
from samples.strategies import bbands


async def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s")

    event_dispatcher = bs.backtesting_dispatcher()
    pair = bs.Pair("BTC", "USDT")
    position_amount = Decimal(1000)
    stop_loss_pct = Decimal(5)

    # We'll be opening short positions so we need to set a lending strategy when initializing the exchange.
    lending_strategy = lending.MarginLoans(pair.quote_symbol, default_conditions=lending.MarginLoanConditions(
        interest_symbol=pair.quote_symbol, interest_percentage=Decimal("7"),
        interest_period=datetime.timedelta(days=365), min_interest=Decimal("0.01"),
        margin_requirement=Decimal("0.5")
    ))
    exchange = backtesting_exchange.Exchange(
        event_dispatcher,
        initial_balances={pair.quote_symbol: Decimal(1200)},
        lending_strategy=lending_strategy,
    )
    exchange.set_symbol_precision(pair.base_symbol, 8)
    exchange.set_symbol_precision(pair.quote_symbol, 2)
    exchange.add_bar_source(csv.BarSource(pair, "binance_btcusdt_day.csv", "1d"))

    # Connect the strategy to the bar events from the exchange.
    strategy = bbands.Strategy(event_dispatcher, period=30, std_dev=2)
    exchange.subscribe_to_bar_events(pair, strategy.on_bar_event)

    # Connect the position manager to the strategy signals and to bar events.
    position_mgr = position_manager.PositionManager(
        exchange, position_amount, pair.quote_symbol, stop_loss_pct
    )
    strategy.subscribe_to_trading_signals(position_mgr.on_trading_signal)
    exchange.subscribe_to_bar_events(pair, position_mgr.on_bar_event)

    # Setup chart.
    chart = charts.LineCharts(exchange)
    chart.add_pair(pair)
    chart.add_pair_indicator(
        "Upper", pair, lambda _: strategy.bb[-1].ub if len(strategy.bb) and strategy.bb[-1] else None
    )
    chart.add_pair_indicator(
        "Central", pair, lambda _: strategy.bb[-1].cb if len(strategy.bb) and strategy.bb[-1] else None
    )
    chart.add_pair_indicator(
        "Lower", pair, lambda _: strategy.bb[-1].lb if len(strategy.bb) and strategy.bb[-1] else None
    )
    chart.add_balance(pair.base_symbol)
    chart.add_balance(pair.quote_symbol)
    chart.add_portfolio_value(pair.quote_symbol)

    # Run the backtest.
    await event_dispatcher.run()

    # Log balances.
    balances = await exchange.get_balances()
    for currency, balance in balances.items():
        logging.info(StructuredMessage(f"{currency} balance", available=balance.available))

    chart.show()


if __name__ == "__main__":
    asyncio.run(main())
