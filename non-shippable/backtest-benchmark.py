from decimal import Decimal
from typing import Optional
import datetime

import asyncio

import numpy as np

from basana.backtesting import lending
from basana.backtesting.exchange import Exchange
import basana as bs


class BarEventSource(bs.EventSource):
    def __init__(
            self, pair: bs.Pair, pair_info: bs.PairInfo, initial_price: Decimal, begin: datetime.datetime,
            end: datetime.datetime, duration: datetime.timedelta
    ):
        super().__init__()
        self._pair = pair
        self._pair_info = pair_info
        self._current_price = float(initial_price)
        self._begin = begin
        self._end = end
        self._duration = duration
        self._volume = Decimal(1000)  # Fixed volume for simplicity

    def pop(self) -> Optional[bs.Event]:
        if self._begin < self._end:
            bar = self._generate_next_bar()
            event = bs.BarEvent(when=bar.begin + bar.duration, bar=bar)
            self._begin += self._duration
            return event
        return None

    def _generate_next_price(self) -> float:
        # Simulate price change with a random walk
        change_percent = np.random.normal(loc=0, scale=0.001)  # Average 0% change, 1% standard deviation
        next_price = self._current_price * (1 + change_percent)
        return next_price

    def _generate_next_bar(self) -> bs.Bar:
        # Simulate price change with a random walk
        open_price = self._current_price
        close_price = self._generate_next_price()
        high_price = max(
            open_price * (1 + np.abs(np.random.normal(loc=0, scale=0.001))),
            close_price
        )
        low_price = min(
            open_price * (1 - np.abs(np.random.normal(loc=0, scale=0.001))),
            close_price
        )

        self._current_price = close_price
        open_price, high_price, low_price, close_price = (
            bs.truncate_decimal(Decimal(value), self._pair_info.quote_precision)
            for value in (open_price, high_price, low_price, close_price)
        )

        return bs.Bar(
            begin=self._begin, pair=self._pair,
            open=open_price, high=high_price, low=low_price, close=close_price, volume=self._volume,
            duration=self._duration
        )


async def on_bar_event_for_stragety(bar_event: bs.BarEvent):
    # print(f"Received bar event for {bar_event.bar.pair} at {bar_event.when} with close price {bar_event.bar.close}")
    pass


async def on_bar_event_for_pos_mgr(bar_event: bs.BarEvent):
    # print(f"Received bar event for {bar_event.bar.pair} at {bar_event.when} with close price {bar_event.bar.close}")
    pass


async def main():
    pair = bs.Pair("BTC", "USDT")
    pair_info = bs.PairInfo(base_precision=6, quote_precision=2)
    initial_price = Decimal("76000")

    event_dispatcher = bs.backtesting_dispatcher()
    # We'll be opening short positions so we need to set a lending strategy when initializing the exchange.
    lending_strategy = lending.MarginLoans(
        pair.quote_symbol,
        margin_requirement=Decimal("0.5"),
        default_conditions=lending.MarginLoanConditions(
            interest_symbol=pair.quote_symbol, interest_percentage=Decimal("7"),
            interest_period=datetime.timedelta(days=365), min_interest=Decimal("0.01")
        )
    )
    exchange = Exchange(
        event_dispatcher,
        initial_balances={
            pair.quote_symbol: Decimal(10000)
        },
        lending_strategy=lending_strategy,
    )
    exchange.set_symbol_precision(pair.base_symbol, pair_info.base_precision)
    exchange.set_symbol_precision(pair.quote_symbol, pair_info.quote_precision)

    # Bars.
    bar_source = BarEventSource(
        pair=pair, pair_info=pair_info, initial_price=initial_price, 
        begin=datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc),
        end=datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc),
        duration=datetime.timedelta(minutes=1)
    )
    exchange.add_bar_source(bar_source)

    exchange.subscribe_to_bar_events(pair, on_bar_event_for_stragety)
    exchange.subscribe_to_bar_events(pair, on_bar_event_for_pos_mgr)

    await event_dispatcher.run()


if __name__ == "__main__":
    asyncio.run(main())
