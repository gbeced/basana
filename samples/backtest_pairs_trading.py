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

# Bars can be downloaded using these commands:
# python -m basana.external.binance.tools.download_bars -c BCH/USDT -p 1h -s 2021-12-01 -e 2021-12-26 \
# -o binance_bchusdt_hourly.csv
# python -m basana.external.binance.tools.download_bars -c CVC/USDT -p 1h -s 2021-12-01 -e 2021-12-26 \
# -o binance_cvcusdt_hourly.csv

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
from samples.strategies import pairs_trading


async def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s")

    event_dispatcher = bs.backtesting_dispatcher()
    quote_symbol = "USDT"
    pair_1 = bs.Pair("BCH", quote_symbol)
    pair_2 = bs.Pair("CVC", quote_symbol)

    # We'll be opening short positions so we need to set a lending strategy when initializing the exchange.
    lending_strategy = lending.MarginLoans(quote_symbol, default_conditions=lending.MarginLoanConditions(
        interest_symbol=quote_symbol, interest_percentage=Decimal("10"),
        interest_period=datetime.timedelta(days=365), min_interest=Decimal("0.01"),
        margin_requirement=Decimal("0.5")
    ))
    exchange = backtesting_exchange.Exchange(
        event_dispatcher,
        initial_balances={quote_symbol: Decimal(1200)},
        lending_strategy=lending_strategy,
    )
    exchange.set_symbol_precision(pair_1.base_symbol, 8)
    exchange.set_symbol_precision(pair_2.base_symbol, 8)
    exchange.set_symbol_precision(quote_symbol, 2)

    # When opening positions, either long or short, size each order to 1000 USDT and close the position if the loss
    # is greater than, or equal to, 5%.
    position_mgr = position_manager.PositionManager(
        exchange, position_amount=Decimal(1000), quote_symbol=quote_symbol, stop_loss_pct=Decimal(5)
    )

    # The pairs trading strategy.
    p_value_threshold = 0.01
    trading_strategy = pairs_trading.Strategy(
        event_dispatcher, pair_1, pair_2, window_size=24 * 10, z_score_window_size=24 * 10,
        p_value_threshold=p_value_threshold, z_score_entry_ge=2.3, z_score_exit_lt=1.5
    )
    # Connect the position manager to the strategy signals.
    trading_strategy.subscribe_to_trading_signals(position_mgr.on_trading_signal)

    chart = charts.LineCharts(exchange)

    for pair in [pair_1, pair_2]:
        # Load bars from the CSV file.
        bars_file_name = "binance_{}{}_hourly.csv".format(pair.base_symbol.lower(), pair.quote_symbol.lower())
        exchange.add_bar_source(csv.BarSource(pair, bars_file_name, "1h"))
        # Connect the strategy to the bar events from the exchange.
        exchange.subscribe_to_bar_events(pair, trading_strategy.on_bar_event)
        # Connect the position manager to the bar events from the exchange.
        exchange.subscribe_to_bar_events(pair, position_mgr.on_bar_event)

        chart.add_pair(pair)

    chart.add_custom("P-Value", "P-Value", lambda _: trading_strategy.p_value)
    chart.add_custom("P-Value", "Threshold", lambda _: p_value_threshold)
    chart.add_custom("Z-Score", "Z-Score", lambda _: trading_strategy.z_score)
    chart.add_portfolio_value(quote_symbol)

    # Run the backtest.
    await event_dispatcher.run()

    # Log balances.
    balances = await exchange.get_balances()
    for currency, balance in balances.items():
        logging.info(StructuredMessage(
            f"{currency} balance", available=balance.available
        ))

    chart.show()


if __name__ == "__main__":
    asyncio.run(main())
