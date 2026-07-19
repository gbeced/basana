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

from basana.core.logs import StructuredMessage
from basana.external.binance import exchange as binance_exchange
import basana as bs

from samples.binance import position_manager
from samples.strategies import bbands


async def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s")

    # To skip demo account, just set `config_overrides = {}`
    # Check https://demo.binance.com/en/my/settings/api-management
    config_overrides = {
        "api": {
            "http": {"base_url": "https://demo-api.binance.com/"},
            "ws_stream": {"base_url": "wss://demo-stream.binance.com/"},
            "ws_api": {"base_url": "wss://demo-ws-api.binance.com/"},
        }
    }

    event_dispatcher = bs.realtime_dispatcher()
    pair = bs.Pair("ETH", "USDT")
    position_amount = Decimal(100)
    stop_loss_pct = Decimal(5)
    api_key = "YOUR_API_KEY"
    api_secret = "YOUR_API_SECRET"

    exchange = binance_exchange.Exchange(
        event_dispatcher, api_key=api_key, api_secret=api_secret,
        config_overrides=config_overrides
    )

    # Check initial balances.
    balances = await exchange.spot_account.get_balances()
    base_balance = balances[pair.base_symbol]
    quote_balance = balances[pair.quote_symbol]
    logging.info(StructuredMessage(
        "Initial balances",
        **{
            pair.base_symbol: base_balance.available,
            pair.quote_symbol: quote_balance.available
        }
    ))

    # Connect the strategy to the bar events from the exchange.
    strategy = bbands.Strategy(event_dispatcher, period=20, std_dev=1.5)
    exchange.subscribe_to_bar_events(pair, "1m", strategy.on_bar_event)

    # We'll be using the spot account, so there will be no short positions opened.
    position_mgr = position_manager.SpotAccountPositionManager(
        exchange, position_amount, pair.quote_symbol, stop_loss_pct
    )
    # Connect the position manager to different types of events.
    strategy.subscribe_to_trading_signals(position_mgr.on_trading_signal)
    exchange.subscribe_to_bar_events(pair, "1m", position_mgr.on_bar_event)
    exchange.spot_account.subscribe_to_order_events(position_mgr.on_order_event)

    await event_dispatcher.run()


if __name__ == "__main__":
    asyncio.run(main())
