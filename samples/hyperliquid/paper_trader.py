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
Autonomous paper trader using Basana + Hyperliquid.

Architecture (follows Basana patterns):
  - RealtimeDispatcher drives the event loop
  - Hyperliquid connector provides live bar events via WebSocket
  - RSI strategy (TradingSignalSource) emits LONG/SHORT/NEUTRAL signals
  - LunarCrush pre-filter gates which coins are eligible
  - PaperPositionManager executes trades at live Hyperliquid mid-prices
    and tracks P&L in JSON (no real money involved)

Requirements:
  pip install basana talipp hyperliquid-python-sdk

Usage:
  python3 paper_trader.py                    # all default coins
  python3 paper_trader.py --coins ETH BTC    # specific coins
  python3 paper_trader.py --interval 1h      # different bar interval
"""

import argparse
import asyncio
import dataclasses
import json
import logging
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

from talipp.indicators import RSI, MACD, BB

import basana as bs
from basana.external.hyperliquid import Exchange as HLExchange
from basana.core.enums import OrderOperation

# ── Config ────────────────────────────────────────────────────────────────────
WORKSPACE = Path(__file__).parent.parent.parent.parent / ".openclaw/workspace"
TRADES_FILE = WORKSPACE / "memory/paper_trades.json"
PORTFOLIO_FILE = WORKSPACE / "memory/paper_portfolio.json"

LC_KEY = "YOUR_LC_KEY_HERE"
STARTING_BALANCE = 10_000.0
POSITION_SIZE_USD = 1_000.0
MAX_POSITIONS = 5

RSI_PERIOD = 14
RSI_OVERSOLD = 32
RSI_OVERBOUGHT = 68
MACD_FAST, MACD_SLOW, MACD_SIGNAL = 12, 26, 9

STOP_LOSS_PCT = Decimal("-15")
TAKE_PROFIT_PCT = Decimal("30")

DEFAULT_COINS = ["ASTER", "AERO", "PYTH", "XRP", "DOGE", "POL", "TIA", "RYO"]
DEFAULT_INTERVAL = "1h"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("paper_trader")


# ── Portfolio persistence ─────────────────────────────────────────────────────

def _load(path: Path) -> dict:
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def _save(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


@dataclasses.dataclass
class PaperPortfolio:
    """In-memory paper portfolio. Persists to JSON on every change."""

    balance: Decimal = Decimal(STARTING_BALANCE)
    positions: Dict[str, dict] = dataclasses.field(default_factory=dict)
    closed_trades: List[dict] = dataclasses.field(default_factory=list)
    realized_pnl: Decimal = Decimal(0)

    @classmethod
    def load(cls) -> "PaperPortfolio":
        trades = _load(TRADES_FILE)
        portfolio = _load(PORTFOLIO_FILE)
        inst = cls(
            balance=Decimal(str(portfolio.get("balance_usd", STARTING_BALANCE))),
            positions=trades.get("open_positions", {}),
            closed_trades=trades.get("closed_trades", []),
            realized_pnl=Decimal(str(portfolio.get("realized_pnl", 0))),
        )
        return inst

    def save(self) -> None:
        trades_data = {
            "open_positions": self.positions,
            "closed_trades": self.closed_trades,
            "trades": list(self.positions.values()) + self.closed_trades,
        }
        portfolio_data = {
            "balance_usd": float(self.balance),
            "starting_balance": STARTING_BALANCE,
            "realized_pnl": float(self.realized_pnl),
        }
        _save(TRADES_FILE, trades_data)
        _save(PORTFOLIO_FILE, portfolio_data)

    @property
    def open_count(self) -> int:
        return len(self.positions)

    def open_position(self, coin: str, direction: str, price: Decimal, reason: str) -> None:
        if coin in self.positions:
            return
        if self.open_count >= MAX_POSITIONS:
            logger.info("Max positions reached, skipping %s", coin)
            return
        size = Decimal(POSITION_SIZE_USD)
        if self.balance < size:
            logger.info("Insufficient balance ($%.2f), skipping %s", self.balance, coin)
            return

        qty = size / price
        self.positions[coin] = {
            "symbol": coin,
            "direction": direction,
            "size_usd": float(size),
            "entry_price": float(price),
            "quantity": float(qty),
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason,
        }
        self.balance -= size
        self.save()
        logger.info("OPEN %s %s @ $%s | qty=%.4f | %s", direction, coin, price, qty, reason)

    def close_position(self, coin: str, price: Decimal, reason: str) -> Optional[Decimal]:
        pos = self.positions.pop(coin, None)
        if not pos:
            return None

        direction = pos["direction"]
        entry = Decimal(str(pos["entry_price"]))
        qty = Decimal(str(pos["quantity"]))
        size = Decimal(str(pos["size_usd"]))

        pnl = (price - entry) * qty if direction == "LONG" else (entry - price) * qty
        pnl_pct = (pnl / size) * 100
        return_usd = size + pnl

        self.closed_trades.append({
            **pos,
            "exit_price": float(price),
            "closed_at": datetime.now(timezone.utc).isoformat(),
            "pnl_usd": float(pnl),
            "pnl_pct": float(pnl_pct),
            "reason": reason,
        })
        self.balance += return_usd
        self.realized_pnl += pnl
        self.save()
        logger.info("CLOSE %s %s @ $%s | P&L: $%.2f (%.1f%%) | %s", direction, coin, price, pnl, pnl_pct, reason)
        return pnl_pct

    def unrealized_pnl(self, coin: str, current_price: Decimal) -> Optional[Decimal]:
        pos = self.positions.get(coin)
        if not pos:
            return None
        entry = Decimal(str(pos["entry_price"]))
        qty = Decimal(str(pos["quantity"]))
        size = Decimal(str(pos["size_usd"]))
        direction = pos["direction"]
        pnl = (current_price - entry) * qty if direction == "LONG" else (entry - current_price) * qty
        return (pnl / size) * 100


# ── LunarCrush gate ───────────────────────────────────────────────────────────

class LunarCrushGate:
    """Checks LC galaxy score and sentiment before allowing a trade."""

    def __init__(self, min_galaxy: float = 55, min_sentiment: float = 70):
        self._min_galaxy = min_galaxy
        self._min_sentiment = min_sentiment
        self._cache: Dict[str, tuple] = {}  # coin -> (galaxy, sentiment, ts)

    def is_approved(self, coin: str) -> bool:
        url = f"https://lunarcrush.com/api4/public/coins/{coin.lower()}/v1"
        try:
            req = urllib.request.Request(url, headers={
                "Authorization": f"Bearer {LC_KEY}",
                "User-Agent": "paper-trader/1.0",
            })
            data = json.loads(urllib.request.urlopen(req, timeout=8).read())
            d = data.get("data", {})
            galaxy = float(d.get("galaxy_score") or 0)
            sentiment = float(d.get("sentiment") or 50)
            logger.info("LC %s: galaxy=%.1f sentiment=%.0f%%", coin, galaxy, sentiment)
            return galaxy >= self._min_galaxy and sentiment >= self._min_sentiment
        except Exception as e:
            logger.warning("LC check failed for %s: %s - allowing trade", coin, e)
            return True  # Fail open so LC outage doesn't block all trades


# ── Basana strategy (TradingSignalSource) ─────────────────────────────────────

class RSIMACDStrategy(bs.TradingSignalSource):
    """Emits LONG/SHORT/NEUTRAL signals based on RSI crossover + MACD confirmation."""

    def __init__(self, dispatcher: bs.EventDispatcher, coin: str, lc_gate: LunarCrushGate):
        super().__init__(dispatcher)
        self._coin = coin
        self._lc_gate = lc_gate
        self._rsi = RSI(RSI_PERIOD)
        self._macd = MACD(MACD_FAST, MACD_SLOW, MACD_SIGNAL)
        self._pair = bs.Pair(coin, "USD")

    async def on_bar_event(self, bar_event: bs.BarEvent) -> None:
        close = float(bar_event.bar.close)
        self._rsi.add(close)
        self._macd.add(close)

        # Need enough history
        if len(self._rsi) < 2 or self._rsi[-2] is None or self._rsi[-1] is None:
            return
        if len(self._macd) < 2 or self._macd[-1] is None:
            return

        rsi_prev = self._rsi[-2]
        rsi_now = self._rsi[-1]
        macd_hist = self._macd[-1].histogram or 0

        # RSI crosses into oversold + MACD histogram positive (momentum turning up)
        if rsi_prev >= RSI_OVERSOLD and rsi_now < RSI_OVERSOLD and macd_hist > 0:
            if self._lc_gate.is_approved(self._coin):
                logger.info("%s LONG signal: RSI %.1f->%.1f MACD_hist=%.5g", self._coin, rsi_prev, rsi_now, macd_hist)
                self.push(bs.TradingSignal(bar_event.when, bs.Position.LONG, self._pair))

        # RSI crosses into overbought - exit long / go neutral
        elif rsi_prev <= RSI_OVERBOUGHT and rsi_now > RSI_OVERBOUGHT:
            logger.info("%s NEUTRAL signal: RSI %.1f->%.1f (overbought)", self._coin, rsi_prev, rsi_now)
            self.push(bs.TradingSignal(bar_event.when, bs.Position.NEUTRAL, self._pair))


# ── Paper position manager ────────────────────────────────────────────────────

class PaperPositionManager:
    """Receives trading signals and manages paper positions.

    Uses live Hyperliquid mid-prices for execution.
    Enforces stop-loss and take-profit on every bar.
    """

    def __init__(self, hl_exchange: HLExchange, portfolio: PaperPortfolio):
        self._exchange = hl_exchange
        self._portfolio = portfolio

    async def on_trading_signal(self, signal: bs.TradingSignal) -> None:
        pairs = list(signal.get_pairs())
        for pair, target_position in pairs:
            coin = pair.base_symbol
            try:
                price = await self._exchange.get_mid_price(coin)
            except Exception:
                # Coin not on Hyperliquid perps - use LC price
                lc_data = json.loads(urllib.request.urlopen(
                    urllib.request.Request(
                        f"https://lunarcrush.com/api4/public/coins/{coin.lower()}/v1",
                        headers={"Authorization": f"Bearer {LC_KEY}", "User-Agent": "paper-trader/1.0"}
                    ), timeout=8
                ).read())
                price = Decimal(str(lc_data["data"]["price"]))

            if target_position == bs.Position.LONG and coin not in self._portfolio.positions:
                self._portfolio.open_position(
                    coin, "LONG", price,
                    reason=f"RSI+MACD signal at {signal.when.isoformat()}",
                )

            elif target_position == bs.Position.NEUTRAL and coin in self._portfolio.positions:
                self._portfolio.close_position(coin, price, reason="RSI overbought exit")

    async def on_bar_event(self, bar_event: bs.BarEvent) -> None:
        """Check stop-loss and take-profit on every bar for all open positions."""
        coin = bar_event.bar.pair.base_symbol
        if coin not in self._portfolio.positions:
            return

        price = Decimal(str(bar_event.bar.close))
        pnl_pct = self._portfolio.unrealized_pnl(coin, price)
        if pnl_pct is None:
            return

        logger.debug("%s unrealized P&L: %.1f%%", coin, pnl_pct)

        if pnl_pct <= STOP_LOSS_PCT:
            logger.warning("%s stop-loss triggered at %.1f%%", coin, pnl_pct)
            self._portfolio.close_position(coin, price, reason=f"Stop-loss {pnl_pct:.1f}%")

        elif pnl_pct >= TAKE_PROFIT_PCT:
            logger.info("%s take-profit triggered at +%.1f%%", coin, pnl_pct)
            self._portfolio.close_position(coin, price, reason=f"Take-profit +{pnl_pct:.1f}%")


# ── Main ─────────────────────────────────────────────────────────────────────

async def main(coins: List[str], interval: str) -> None:
    portfolio = PaperPortfolio.load()
    logger.info(
        "Portfolio loaded: balance=$%.2f | open=%d | realized_pnl=$%.2f",
        portfolio.balance, portfolio.open_count, portfolio.realized_pnl,
    )

    dispatcher = bs.realtime_dispatcher()
    hl = HLExchange(dispatcher=dispatcher)
    lc_gate = LunarCrushGate()
    position_mgr = PaperPositionManager(hl, portfolio)

    for coin in coins:
        strategy = RSIMACDStrategy(dispatcher, coin, lc_gate)
        strategy.subscribe_to_trading_signals(position_mgr.on_trading_signal)
        hl.subscribe_to_bar_events(coin, interval, strategy.on_bar_event)
        hl.subscribe_to_bar_events(coin, interval, position_mgr.on_bar_event)

    logger.info("Paper trader live on %d coins (%s bars). Ctrl+C to stop.", len(coins), interval)

    try:
        await dispatcher.run()
    except KeyboardInterrupt:
        pass

    # Final summary
    logger.info("=== Final portfolio ===")
    logger.info("Balance: $%.2f | Realized P&L: $%.2f", portfolio.balance, portfolio.realized_pnl)
    for coin, pos in portfolio.positions.items():
        price = await hl.get_mid_price(coin)
        pnl_pct = portfolio.unrealized_pnl(coin, price)
        logger.info("  %s %s: unrealized %.1f%%", pos["direction"], coin, pnl_pct or 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Basana paper trader on Hyperliquid")
    parser.add_argument("--coins", nargs="+", default=DEFAULT_COINS, help="Coins to trade")
    parser.add_argument("--interval", default=DEFAULT_INTERVAL, help="Bar interval (1m, 5m, 1h, 4h...)")
    args = parser.parse_args()

    asyncio.run(main(coins=args.coins, interval=args.interval))
