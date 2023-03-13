# Basana
#
# Copyright 2022-2023 Gabriel Martin Becedillas Ruiz
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
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""


from typing import cast, Any, Awaitable, Callable, List, Optional
import datetime

from basana.core import dispatcher, enums, event, pair


class TradingSignal(event.Event):
    def __init__(self, when: datetime.datetime, operation: enums.OrderOperation, pair: pair.Pair):
        super().__init__(when)
        self.operation = operation
        self.pair = pair


class TradingSignalSource(event.FifoQueueEventSource):
    def __init__(
            self, dispatcher: dispatcher.EventDispatcher, producer: Optional[event.Producer] = None,
            events: List[event.Event] = []
    ):
        super().__init__(producer=producer, events=events)
        self._dispatcher = dispatcher

    def subscribe_to_trading_signals(self, event_handler: Callable[[TradingSignal], Awaitable[Any]]):
        self._dispatcher.subscribe(self, cast(dispatcher.EventHandler, event_handler))
