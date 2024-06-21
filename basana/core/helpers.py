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
from typing import Any, Coroutine, List, Optional, Set, Union
import asyncio
import contextlib
import decimal
import logging
import warnings

import aiohttp

from basana.core import logs


class TaskGroup:
    def __init__(self):
        self._tasks = []
        self._exiting = False

    async def __aenter__(self) -> "TaskGroup":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        self._exiting = True

        try:
            if not exc_type:
                await asyncio.gather(*self._tasks)
        finally:
            pending = self._cancel()
            if pending:
                # Don't raise exceptions since we're waiting for tasks to finish.
                await asyncio.gather(*pending, return_exceptions=True)

    def _cancel(self) -> List[asyncio.Task]:
        pending = [task for task in self._tasks if not task.done()]
        for task in pending:
            if not task.done():
                task.cancel()
        return pending

    def create_task(self, coro) -> asyncio.Task:
        assert not self._exiting
        ret = asyncio.create_task(coro)
        self._tasks.append(ret)
        return ret

    def cancel(self):
        self._cancel()


class TaskPool:
    """
    A class for managing a pool of asyncio tasks.

    :param size: The maximum size of the task pool.
    """
    def __init__(self, size: int):
        assert size > 0, "Invalid size"
        self._max_size = size
        self._tasks: Set[asyncio.Task] = set()
        self._done: List[asyncio.Task] = []

    @property
    def idle(self) -> int:
        """
        True if there are no active tasks in the pool, False otherwise.
        """
        return len(self._tasks) == 0

    async def push(self, coroutine: Coroutine[Any, Any, Any]):
        """
        Adds a coroutine to the task pool. If the pool is full it will block until there is room for the new task.

        :param coroutine: The coroutine to be added to the task pool.
        """
        # Wait for some task to complete if there is no more room.
        while len(self._tasks) >= self._max_size:
            await self._wait_impl(timeout=None, return_when=asyncio.FIRST_COMPLETED)
        self._tasks.add(asyncio.create_task(coroutine))

    def pop_done(self) -> List[asyncio.Task]:
        """
        Returns the tasks that are done running since the last call to pop_done.
        """
        ret = self._done
        self._done = []
        return ret

    def cancel(self):
        """
        Requests all tasks in the pool to be canceled.
        """
        pending = [task for task in self._tasks if not task.done()]
        for task in pending:
            task.cancel()

    async def wait(self, timeout: Optional[Union[int, float]] = None) -> bool:
        """
        Waits for all tasks in the pool to complete. Returns True if all tasks are done, False otherwise.

        :param timeout: The maximum number of seconds to wait for tasks to complete. If None, wait indefinitely.
        """
        return await self._wait_impl(timeout=timeout, return_when=asyncio.ALL_COMPLETED)

    async def _wait_impl(self, timeout: Optional[Union[int, float]], return_when: str) -> bool:
        done: List[asyncio.Task] = []
        if self._tasks:
            done, _ = await asyncio.wait(self._tasks, timeout=timeout, return_when=return_when)
        for task in done:
            self._tasks.remove(task)
            self._done.append(task)
        return len(done) > 0


@contextlib.contextmanager
def no_raise(logger: logging.Logger, msg: str, **kwargs):
    try:
        yield
    except Exception as e:
        log_args = {"exception": e}
        log_args.update(kwargs)
        logger.exception(logs.StructuredMessage(msg, **log_args))


@contextlib.asynccontextmanager
async def use_or_create_session(session: Optional[aiohttp.ClientSession] = None):
    if session:
        yield session
    else:
        async with aiohttp.ClientSession() as new_session:
            yield new_session


def round_decimal(value: Decimal, precision: int, rounding=None) -> Decimal:
    """Rounds a decimal value.

    :param value: The value to round.
    :param precision: The number of digits after the decimal point.
    :param rounding: An optional rounding option from the :mod:`decimal` module.
    :returns: The rounded value.
    """
    return value.quantize(Decimal(f"1e-{precision}"), rounding=rounding)


def truncate_decimal(value: Decimal, precision: int) -> Decimal:
    """Truncates a decimal value.

    :param value: The value to truncate.
    :param precision: The number of digits after the decimal point.
    :returns: The truncated value.
    """
    return round_decimal(value, precision, rounding=decimal.ROUND_DOWN)


def deprecation_warning(message: str):
    warnings.warn(message, DeprecationWarning, stacklevel=2)


def classpath(obj: object):
    cls = obj.__class__
    module = cls.__module__
    parts = [str(module), cls.__qualname__] if module else [cls.__qualname__]
    return ".".join(parts)
