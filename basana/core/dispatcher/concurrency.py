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

from typing import Callable, Coroutine, List, Optional, Union
import abc
import asyncio

from .. import helpers


CoroutineFunc = Callable[[], Coroutine]


class ConcurrencyStrategy(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def gather(self, coro_funcs: List[CoroutineFunc]):
        raise NotImplementedError()

    @abc.abstractmethod
    async def push_pool(self, coro_func: CoroutineFunc):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def pool_idle(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def cancel_pool(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def wait_pool(self, timeout: Optional[Union[int, float]] = None) -> bool:
        raise NotImplementedError()


class NoConcurrency(ConcurrencyStrategy):
    def __init__(self):
        self._canceled = False
        self._idle = True

    async def gather(self, coro_funcs: List[CoroutineFunc]):
        for coro_func in coro_funcs:
            if self._canceled:
                break
            await coro_func()

    async def push_pool(self, coro_func: CoroutineFunc):
        try:
            self._idle = False
            if not self._canceled:
                await coro_func()
        finally:
            self._idle = True

    @property
    def pool_idle(self) -> bool:
        return self._idle

    def cancel_pool(self):
        self._canceled = True

    async def wait_pool(self, timeout: Optional[Union[int, float]] = None) -> bool:
        return self._idle


class BoundedConcurrency(ConcurrencyStrategy):
    def __init__(self, max_concurrency: int):
        self._max_concurrency = max_concurrency
        self._task_pool = helpers.TaskPool(self._max_concurrency, max_queue_size=self._max_concurrency * 10)

    async def gather(self, coro_funcs: List[CoroutineFunc]):
        return await asyncio.gather(*[coro_func() for coro_func in coro_funcs])

    async def push_pool(self, coro_func: CoroutineFunc):
        await self._task_pool.push(coro_func)

    @property
    def pool_idle(self) -> bool:
        return self._task_pool.idle

    def cancel_pool(self):
        return self._task_pool.cancel()

    async def wait_pool(self, timeout: Optional[Union[int, float]] = None) -> bool:
        return await self._task_pool.wait(timeout)
