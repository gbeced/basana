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
import datetime

from basana.core import bar, pair


def test_trades_to_bar_empty():
    trade_to_bar = bar.RealTimeTradesToBar(pair.Pair("BTC", "USD"), 5)

    begin = datetime.datetime(2000, 1, 1).replace(tzinfo=datetime.timezone.utc)
    end = datetime.datetime(2000, 1, 2).replace(tzinfo=datetime.timezone.utc)
    trade_to_bar._flush(begin, end)
    assert trade_to_bar.pop() is None


def test_trades_to_bar_manual_flush():
    bar_duration = 60
    trade_to_bar = bar.RealTimeTradesToBar(pair.Pair("BTC", "USD"), bar_duration)

    begin = datetime.datetime(2000, 1, 1).replace(tzinfo=datetime.timezone.utc)
    end = begin + datetime.timedelta(seconds=bar_duration, microseconds=-1)

    # This one should be ignored since it'll be outside the first window.
    trade_to_bar.push_trade(begin - datetime.timedelta(seconds=1), Decimal("1000"), Decimal("1000"))
    # These ones are part of the first window but they will be skipped since that is the default (skip_first_bar=True).
    trade_to_bar.push_trade(begin + datetime.timedelta(seconds=2), Decimal("104"), Decimal("0.3"))
    trade_to_bar.push_trade(begin + datetime.timedelta(seconds=3), Decimal("101"), Decimal("0.4"))
    # These ones should be included in the next window.
    trade_to_bar.push_trade(begin + datetime.timedelta(seconds=61), Decimal("200"), Decimal("1.1"))
    trade_to_bar.push_trade(begin + datetime.timedelta(seconds=62), Decimal("201"), Decimal("1.2"))
    trade_to_bar.push_trade(begin + datetime.timedelta(seconds=63), Decimal("202"), Decimal("1.3"))
    trade_to_bar.push_trade(begin + datetime.timedelta(seconds=64), Decimal("203"), Decimal("1.4"))
    # Leave a 1 day gap from the previous bar.
    trade_to_bar.push_trade(begin + datetime.timedelta(seconds=181), Decimal("300"), Decimal("2.1"))
    trade_to_bar.push_trade(begin + datetime.timedelta(seconds=182), Decimal("301"), Decimal("2.2"))
    trade_to_bar.push_trade(begin + datetime.timedelta(seconds=183), Decimal("302"), Decimal("2.3"))
    trade_to_bar.push_trade(begin + datetime.timedelta(seconds=185), Decimal("303"), Decimal("2.4"))
    # This one should be ignored since its not in order
    trade_to_bar.push_trade(begin + datetime.timedelta(seconds=184), Decimal("3000"), Decimal("3000"))

    # Flush the first day.
    trade_to_bar._flush(begin, end)
    bar_event = trade_to_bar.pop()
    assert bar_event is None

    # Thie first trades should have been disgarded.
    assert len(trade_to_bar._trades) == 8

    # Flush the next day.
    begin += datetime.timedelta(seconds=bar_duration)
    end += datetime.timedelta(seconds=bar_duration)
    trade_to_bar._flush(begin, end)
    bar_event = trade_to_bar.pop()
    assert bar_event.when == end
    assert bar_event.bar.open == Decimal("200")
    assert bar_event.bar.high == Decimal("203")
    assert bar_event.bar.low == Decimal("200")
    assert bar_event.bar.close == Decimal("203")
    assert bar_event.bar.volume == Decimal("5")

    # Flush the next day (gap day).
    begin += datetime.timedelta(seconds=bar_duration)
    end += datetime.timedelta(seconds=bar_duration)
    trade_to_bar._flush(begin, end)
    bar_event = trade_to_bar.pop()
    assert bar_event is None

    # Flush the next day.
    begin += datetime.timedelta(seconds=bar_duration)
    end += datetime.timedelta(seconds=bar_duration)
    trade_to_bar._flush(begin, end)
    bar_event = trade_to_bar.pop()
    assert bar_event.when == end
    assert bar_event.bar.open == Decimal("300")
    assert bar_event.bar.high == Decimal("303")
    assert bar_event.bar.low == Decimal("300")
    assert bar_event.bar.close == Decimal("303")
    assert bar_event.bar.volume == Decimal("9")

    assert len(trade_to_bar._trades) == 0
