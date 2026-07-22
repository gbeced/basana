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

from typing import Any
import abc
import asyncio
import logging

from . import helpers
from basana.core import event, logs
from basana.core.pair import Pair


logger = logging.getLogger(__name__)


class WatchEventSource(event.FifoQueueEventSource, event.Producer):
    def __init__(self, cli: Any, pair: Pair, watch_capability: str, unwatch_capability: str):
        if not cli.has.get(watch_capability):
            raise NotImplementedError(f"The exchange does not support {watch_capability}")

        super().__init__(producer=self)
        self._cli = cli
        self._pair = pair
        self._symbol = helpers.pair_to_symbol(pair)
        self._watch_capability = watch_capability
        self._unwatch_capability = unwatch_capability
        self.err_sleep = 0.1

    async def initialize(self):
        await self._cli.load_markets()

    async def main(self):
        while True:
            sleep = 0
            try:
                await self.watch()
            except Exception as e:
                logger.error(logs.StructuredMessage(
                    f"Error during {self._watch_capability}", pair=self._pair, error=e
                ))
                sleep = self.err_sleep
            # Yield to the event loop to allow other tasks to run.
            await asyncio.sleep(sleep)

    async def finalize(self):
        if not self._cli.has.get(self._unwatch_capability):
            return

        try:
            await self.unwatch()
        except Exception as e:
            logger.error(logs.StructuredMessage(
                f"Error during {self._unwatch_capability}", pair=self._pair, error=e
            ))

    @abc.abstractmethod
    async def watch(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def unwatch(self):
        raise NotImplementedError()
