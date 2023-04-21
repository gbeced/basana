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

from typing import List, Optional
import abc
import datetime

from . import dt


class Producer:
    """Base class for producers.

    A producer is the active part of an :class:`basana.EventSource` or a group of them.
    Take a look at :meth:`EventDispatcher.run` for details on how producers are used.

    .. note::

        This is a base class and should not be used directly.
    """

    async def initialize(self):
        """Override to perform initialization."""
        pass

    async def main(self):
        """Override to run the loop that produces events."""
        pass

    async def finalize(self):
        """Override to perform cleanup."""
        pass


class Event:
    """Base class for events.

    An event is something that occurs at a specific point in time. There are many different types of events:

    * An update to an order book.
    * A new trade.
    * An order update.
    * A new bar (candlestick/ohlc).
    * Others

    :param when: The datetime when the event occurred. It must have timezone information set.

    .. note::

        This is a base class and should not be used directly.
    """

    def __init__(self, when: datetime.datetime):
        assert not dt.is_naive(when), f"{when} should have timezone information set"

        #: The datetime when the event occurred.
        self.when: datetime.datetime = when


class EventSource(metaclass=abc.ABCMeta):
    """Base class for event sources.

    This class declares the interface that is required by the :class:`basana.EventDispatcher` to gather events for
    processing.

    :param producer: An optional producer associated with this event source.
    """

    def __init__(self, producer: Optional[Producer] = None):
        self.producer = producer

    @abc.abstractmethod
    def pop(self) -> Optional[Event]:
        """Override to return the next event, or None if there are no events available.

        This method is used by the :class:`basana.EventDispatcher` during the event dispatch loop so **it should return
        as soon as possible**.
        """
        raise NotImplementedError()


class FifoQueueEventSource(EventSource):
    """A FIFO queue event source.

    :param producer: An optional producer associated with this event source.
    :param events: An optional list of initial events.
    """
    def __init__(self, producer: Optional[Producer] = None, events: List[Event] = []):
        super().__init__(producer)
        self._queue: List[Event] = []
        self._queue.extend(events)

    def push(self, event: Event):
        """Adds an event to the end of the queue."""
        self._queue.append(event)

    def pop(self) -> Optional[Event]:
        """Removes and returns the next event in the queue."""
        ret = None
        if self._queue:
            ret = self._queue.pop(0)
        return ret
