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
    async def initialize(self):
        pass

    async def main(self):
        pass

    async def finalize(self):
        pass


class Event:
    def __init__(self, when: datetime.datetime):
        assert not dt.is_naive(when), f"{when} should have timezone information set"
        self.when = when


class EventSource(metaclass=abc.ABCMeta):
    def __init__(self, producer: Optional[Producer] = None):
        self.producer = producer

    @abc.abstractmethod
    def pop(self) -> Optional[Event]:  # pragma: no cover
        raise NotImplementedError()


class FifoQueueEventSource(EventSource):
    def __init__(self, producer: Optional[Producer] = None, events: List[Event] = []):
        super().__init__(producer)
        self._queue: List[Event] = []
        self._queue.extend(events)

    def push(self, event: Event):
        self._queue.append(event)

    def pop(self) -> Optional[Event]:
        ret = None
        if self._queue:
            ret = self._queue.pop(0)
        return ret
