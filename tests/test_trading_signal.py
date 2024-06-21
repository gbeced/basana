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

import asyncio

import pytest

from basana.core import dispatcher, dt, enums, errors
from basana.core.event_sources import trading_signal
from basana.core.pair import Pair


class TradingSignalSource(trading_signal.TradingSignalSource):
    def __init__(self, dispatcher: dispatcher.EventDispatcher):
        super().__init__(dispatcher)


def test_trading_signal_op(backtesting_dispatcher):
    trading_signals = []

    async def on_trading_signal(trading_signal: trading_signal.TradingSignal):
        trading_signals.append(trading_signal)

    async def impl():
        source = TradingSignalSource(backtesting_dispatcher)
        source.push(trading_signal.TradingSignal(dt.local_now(), enums.OrderOperation.BUY, Pair("BTC", "USDT")))
        source.subscribe_to_trading_signals(on_trading_signal)
        await backtesting_dispatcher.run()

    asyncio.run(impl())
    assert len(trading_signals) == 1
    for signal in trading_signals:
        assert signal.operation == enums.OrderOperation.BUY
        assert signal.position == enums.Position.LONG


def test_trading_signal_pos(backtesting_dispatcher):
    trading_signals = []

    async def on_trading_signal(trading_signal: trading_signal.TradingSignal):
        trading_signals.append(trading_signal)

    async def impl():
        source = TradingSignalSource(backtesting_dispatcher)
        source.push(trading_signal.TradingSignal(dt.local_now(), enums.Position.SHORT, Pair("BTC", "USDT")))
        source.subscribe_to_trading_signals(on_trading_signal)
        await backtesting_dispatcher.run()

    asyncio.run(impl())
    assert len(trading_signals) == 1
    for signal in trading_signals:
        assert signal.operation == enums.OrderOperation.SELL
        assert signal.position == enums.Position.SHORT


def test_neutral_position_cant_be_mapped_to_operation(backtesting_dispatcher):
    trading_signals = []

    async def on_trading_signal(trading_signal: trading_signal.TradingSignal):
        trading_signals.append(trading_signal)

    async def impl():
        source = TradingSignalSource(backtesting_dispatcher)
        source.push(trading_signal.TradingSignal(dt.local_now(), enums.Position.NEUTRAL, Pair("BTC", "USDT")))
        source.subscribe_to_trading_signals(on_trading_signal)
        await backtesting_dispatcher.run()

    asyncio.run(impl())
    assert len(trading_signals) == 1
    for signal in trading_signals:
        assert signal.position == enums.Position.NEUTRAL
        with pytest.raises(errors.Error):
            assert signal.operation
