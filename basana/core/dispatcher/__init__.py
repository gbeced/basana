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

# ruff: noqa

from .base import (
    EventDispatcher,
    EventHandler,
    LoopStartedHandler,
    SchedulerJob,
)

from .backtesting import (
    BacktestingDispatcher
)

from .realtime import (
    IdleHandler,
    RealtimeDispatcher,
)


def realtime_dispatcher(max_concurrent: int = 50) -> RealtimeDispatcher:
    """
    Creates an event dispatcher suitable for live trading.

    :param max_concurrent: The maximum number of events to process concurrently.
    """
    return RealtimeDispatcher(max_concurrent=max_concurrent)


def backtesting_dispatcher(max_concurrent: int = 1) -> BacktestingDispatcher:
    """
    Creates an event dispatcher suitable for backtesting.

    :param max_concurrent: The maximum number of events to process concurrently.
    """
    return BacktestingDispatcher(max_concurrent=max_concurrent)
