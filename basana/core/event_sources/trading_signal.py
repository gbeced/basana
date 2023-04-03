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

from typing import cast, Any, Awaitable, Callable, List, Optional
import datetime

from basana.core import dispatcher, enums, event, pair


class TradingSignal(event.Event):
    """A trading signal is an event that instructs to buy or sell a given pair.

    :param when: The datetime when the trading signal occurred. It must have timezone information set.
    :param operation: The operation.
    :param pair: The pair to trade.
    """

    def __init__(self, when: datetime.datetime, operation: enums.OrderOperation, pair: pair.Pair):
        super().__init__(when)
        #: The operation.
        self.operation = operation
        #: The pair to trade.
        self.pair = pair


class TradingSignalSource(event.FifoQueueEventSource):
    """Base class for event sources that generate :class:`basana.TradingSignal` events.

    :param dispatcher: The event dispatcher.
    :param producer: An optional producer associated with this event source.
    :param events: An optional list of initial events.
    """

    def __init__(
            self, dispatcher: dispatcher.EventDispatcher, producer: Optional[event.Producer] = None,
            events: List[event.Event] = []
    ):
        super().__init__(producer=producer, events=events)
        self._dispatcher = dispatcher

    def subscribe_to_trading_signals(self, event_handler: Callable[[TradingSignal], Awaitable[Any]]):
        """Registers an async callable that will be called when a new trading signal is available.

        :param event_handler: An async callable that receives an trading signal.
        """

        self._dispatcher.subscribe(self, cast(dispatcher.EventHandler, event_handler))
