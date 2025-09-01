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
import datetime
import io
import logging

from dateutil import tz
import pytest

from .helpers import abs_data_path
from basana.backtesting import errors, exchange, fees, helpers as bt_helpers, orders
from basana.core import bar, dt, event
from basana.core.enums import OrderOperation
from basana.core.pair import Pair, PairInfo
from basana.external.yahoo import bars


def test_account_balances(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(
            backtesting_dispatcher,
            {
                "usd": Decimal("1000"),
                "ars": Decimal("-2000.05"),
                "eth": Decimal("0")
            }
        )
        assert (await e.get_balance("usd")).available == Decimal("1000")
        assert (await e.get_balance("usd")).borrowed == Decimal("0")
        assert (await e.get_balance("usd")).total == Decimal("1000")
        assert (await e.get_balance("ars")).available == Decimal("0")
        assert (await e.get_balance("ars")).borrowed == Decimal("2000.05")
        assert (await e.get_balance("ars")).total == Decimal("-2000.05")
        assert (await e.get_balance("eth")).available == Decimal("0")
        assert (await e.get_balance("ltc")).available == Decimal("0")

    asyncio.run(impl())


def test_exchange_object_container():
    idx = bt_helpers.ExchangeObjectContainer[orders.Order]()

    for i in range(1, 3):
        idx.add(
            orders.MarketOrder(
                str(i), OrderOperation.BUY, Pair("BTC", "USD"), Decimal("1")
            )
        )
    assert "1" in [o.id for o in idx.get_open()]
    idx.get("1").cancel()
    assert "1" not in [o.id for o in idx.get_open()]
    assert "2" in [o.id for o in idx.get_open()]
    assert len(idx._open_items) == 2
    for _ in range(50 - 2):
        assert "2" in [o.id for o in idx.get_open()]
    assert len(idx._open_items) == 1


def test_bar_events_from_csv_and_backtesting_log_mode(backtesting_dispatcher, caplog):
    caplog.set_level(logging.INFO)

    # Create a custom logger with a specific format.
    logger = logging.getLogger(__name__)
    buff = io.StringIO()
    ch = logging.StreamHandler(stream=buff)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("[%(asctime)s %(levelname)s %(name)s] %(message)s"))
    logger.addHandler(ch)

    bar_events = []

    async def on_bar(bar_event):
        bar_events.append(bar_event)
        logger.info("%s %s", bar_event.bar.pair, bar_event.bar.close)

    async def impl():
        e = exchange.Exchange(
            backtesting_dispatcher,
            {
                "IBM": Decimal("0"),
                "USD": Decimal("1000"),
            }
        )
        p = Pair("IBM", "USD")
        e.add_bar_source(bars.CSVBarSource(p, abs_data_path("orcl-2001-yahoo.csv"), sort=True))
        e.subscribe_to_bar_events(p, on_bar)

        await backtesting_dispatcher.run()
        assert backtesting_dispatcher.now().date() == datetime.date(2002, 1, 1)
        # This will soon be deprecated.
        assert backtesting_dispatcher.current_event_dt.date() == datetime.date(2002, 1, 1)

        assert bar_events[0].when == datetime.datetime(2001, 1, 3, tzinfo=tz.tzlocal())
        assert bar_events[0].bar.open == Decimal("29.56")
        assert bar_events[0].bar.high == Decimal("29.75")
        assert bar_events[0].bar.low == Decimal("25.62")
        assert bar_events[0].bar.close == Decimal("26.37")
        assert bar_events[0].bar.volume == Decimal("46285300")

    asyncio.run(impl())

    # Get the log output.
    buff.flush()
    buff.seek(0)
    output = buff.read()

    assert "2001-01-03 00:00:00,000 INFO" in output
    assert "2002-01-01 00:00:00,000 INFO" in output


def test_pair_info(backtesting_dispatcher):
    async def impl():
        e = exchange.Exchange(
            backtesting_dispatcher,
            {
                "USD": Decimal("1e6"),
            },
            fee_strategy=fees.Percentage(percentage=Decimal("0.25"))
        )

        pair_info = await e.get_pair_info(Pair("ORCL", "USD"))
        assert pair_info.base_precision == 0
        assert pair_info.quote_precision == 2

        e.set_pair_info(Pair("ORCL", "EUR"), PairInfo(0, 3))
        pair_info = await e.get_pair_info(Pair("ORCL", "EUR"))
        assert pair_info.base_precision == 0
        assert pair_info.quote_precision == 3

    asyncio.run(impl())


def test_bid_ask(backtesting_dispatcher):
    e = exchange.Exchange(backtesting_dispatcher, {})

    async def impl():
        p = Pair("ORCL", "USD")
        bs = event.FifoQueueEventSource(events=[
            bar.BarEvent(
                dt.local_datetime(2000, 1, 3, 23, 59, 59),
                bar.Bar(
                    dt.local_datetime(2000, 1, 3), p,
                    Decimal("124.62"), Decimal("125.19"), Decimal("111.62"), Decimal("118.12"), Decimal("98122000")
                )
            )
        ])
        e.add_bar_source(bs)

        with pytest.raises(errors.Error, match="No price for"):
            await e.get_bid_ask(p)

        await backtesting_dispatcher.run()

        bid, ask = await e.get_bid_ask(p)
        assert bid == Decimal("117.83")
        assert ask == Decimal("118.41")

    asyncio.run(impl())
