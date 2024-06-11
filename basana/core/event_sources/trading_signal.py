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

from typing import cast, Any, Awaitable, Callable, Dict, Iterable, List, Optional, Union
import datetime

from basana.core import dispatcher, enums, errors, event, helpers, pair


class BaseTradingSignal(event.Event):
    def __init__(self, when: datetime.datetime):
        super().__init__(when)
        self._positions: Dict[pair.Pair, enums.Position] = {}
        self._extras: Dict[pair.Pair, Dict[str, Any]] = {}

    def add_pair(self, pair: pair.Pair, position: enums.Position, extras: Dict[str, Any] = {}):
        self._positions[pair] = position
        self._extras.setdefault(pair, {}).update(extras)

    def get_pairs(self) -> Iterable[pair.Pair]:
        return self._positions.keys()

    def get_pair_position(self, pair: pair.Pair) -> enums.Position:
        return self._positions[pair]

    # def get_pair_extras(self, pair: pair.Pair) -> Dict[str, Any]:
    #     return self._extras.get(pair, {})


class TradingSignal(BaseTradingSignal):
    """
    A trading signal is an event that instructs to take a long, short, or neutral position on a given trading pair.

    :param when: The datetime when the trading signal occurred. It must have timezone information set.
    :param op_or_pos: A enums.Position or an enums.OrderOperation (for backwards compatibility purposes).
    :param pair: The pair to trade.
    """

    def __init__(
            self, when: datetime.datetime, op_or_pos: Union[enums.OrderOperation, enums.Position], pair: pair.Pair
    ):
        super().__init__(when)

        if isinstance(op_or_pos, enums.OrderOperation):
            helpers.deprecation_warning(
                "Support for bs.OrderOperation in trading signals will be removed soon."
                " Switch to bs.Position"
            )
            op_or_pos = {
                enums.OrderOperation.BUY: enums.Position.LONG,
                enums.OrderOperation.SELL: enums.Position.SHORT,
            }[op_or_pos]
        self.add_pair(pair, op_or_pos)

    @property
    def pair(self) -> pair.Pair:
        return next(iter(self.get_pairs()))

    @property
    def position(self) -> enums.Position:
        return self.get_pair_position(self.pair)

    @property
    def operation(self) -> enums.OrderOperation:
        """
        The operation.

        .. note::

          * This property is deprecated and position should be used instead.
        """
        position = self.position
        op = {
            enums.Position.LONG: enums.OrderOperation.BUY,
            enums.Position.SHORT: enums.OrderOperation.SELL,
        }.get(position)
        if op is None:
            raise errors.Error("{} can't be mapped to an operation".format(position))
        return op


class TradingSignalSource(event.FifoQueueEventSource):
    """
    Base class for event sources that generate :class:`basana.BaseTradingSignal` events.

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

    def subscribe_to_trading_signals(self, event_handler: Callable[[BaseTradingSignal], Awaitable[Any]]):
        """Registers an async callable that will be called when a new trading signal is available.

        :param event_handler: An async callable that receives an trading signal.
        """

        self._dispatcher.subscribe(self, cast(dispatcher.EventHandler, event_handler))
