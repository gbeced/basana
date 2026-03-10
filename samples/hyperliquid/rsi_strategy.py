# Basana
#
# Copyright 2026 Christian Pojoni
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
Example: RSI strategy on Hyperliquid perpetuals using Basana.

Subscribes to ETH/USD 1h bars, calculates RSI(14) with TALIpp,
and opens/closes a long position based on RSI thresholds.

Requirements:
    pip install basana talipp hyperliquid-python-sdk

Usage (paper trading - no key needed for bar data):
    python3 rsi_strategy.py

Usage (live trading):
    HYPERLIQUID_KEY=0x... python3 rsi_strategy.py
"""

import asyncio
import logging
import os
from decimal import Decimal

from talipp.indicators import RSI

import basana as b
from basana.external.hyperliquid import Exchange
from basana.core.enums import OrderOperation

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

COIN = "ETH"
INTERVAL = "1h"
RSI_PERIOD = 14
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70
POSITION_SIZE = Decimal("0.01")  # ETH


class RSIStrategy:
    def __init__(self, exchange: Exchange):
        self.exchange = exchange
        self.closes: list[float] = []
        self.in_position = False

    async def on_bar(self, event: b.BarEvent) -> None:
        bar = event.bar
        self.closes.append(float(bar.close))
        logger.info("Bar: %s close=%.4f", COIN, bar.close)

        if len(self.closes) < RSI_PERIOD + 1:
            return  # Not enough data

        rsi_vals = RSI(RSI_PERIOD, self.closes)
        rsi = rsi_vals[-1]
        logger.info("RSI(14)=%.1f | in_position=%s", rsi, self.in_position)

        if not self.in_position and rsi < RSI_OVERSOLD:
            logger.info("RSI oversold (%.1f) - opening LONG %s", rsi, COIN)
            try:
                order = await self.exchange.perps_account.market_open(COIN, OrderOperation.BUY, POSITION_SIZE)
                logger.info("Opened: oid=%s filled=%s", order.oid, order.filled)
                self.in_position = True
            except Exception as e:
                logger.error("Failed to open position: %s", e)

        elif self.in_position and rsi > RSI_OVERBOUGHT:
            logger.info("RSI overbought (%.1f) - closing LONG %s", rsi, COIN)
            try:
                order = await self.exchange.perps_account.market_close(COIN)
                logger.info("Closed: oid=%s filled=%s", order.oid, order.filled)
                self.in_position = False
            except Exception as e:
                logger.error("Failed to close position: %s", e)


async def main():
    private_key = os.environ.get("HYPERLIQUID_KEY")  # Optional
    if not private_key:
        logger.info("No HYPERLIQUID_KEY set - running in read-only mode (no trading)")

    d = b.realtime_dispatcher()
    exchange = Exchange(dispatcher=d, private_key=private_key)

    strategy = RSIStrategy(exchange)
    exchange.subscribe_to_bar_events(COIN, INTERVAL, strategy.on_bar)

    logger.info("Strategy running. Ctrl+C to stop.")
    try:
        await d.run()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    asyncio.run(main())
