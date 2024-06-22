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
import asyncio
import logging

from basana.external.binance import exchange as binance_exchange
import basana as bs

from samples.binance import position_manager
from samples.strategies import bbands


async def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s")

    event_dispatcher = bs.realtime_dispatcher()
    pair = bs.Pair("ETH", "USDT")
    position_amount = Decimal(100)
    stop_loss_pct = Decimal(5)
    checkpoint_fname = "binance_bbands_positions.json"
    api_key = "YOUR_API_KEY"
    api_secret = "YOUR_API_SECRET"

    exchange = binance_exchange.Exchange(event_dispatcher, api_key=api_key, api_secret=api_secret)

    # Connect the strategy to the bar events from the exchange.
    strategy = bbands.Strategy(event_dispatcher, period=20, std_dev=1.5)
    exchange.subscribe_to_bar_events(pair, "1m", strategy.on_bar_event)

    # We'll be using the spot account, so there will be no short positions opened.
    position_mgr = position_manager.SpotAccountPositionManager(
        exchange, position_amount, pair.quote_symbol, stop_loss_pct, checkpoint_fname
    )
    # Connect the position manager to the strategy signals and to bar events just for logging.
    strategy.subscribe_to_trading_signals(position_mgr.on_trading_signal)
    exchange.subscribe_to_bar_events(pair, "1m", position_mgr.on_bar_event)

    await event_dispatcher.run()


if __name__ == "__main__":
    asyncio.run(main())
