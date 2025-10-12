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
from typing import Any, Coroutine, Dict, List, Optional, Union
import asyncio
import contextlib
import decimal
import logging
import uuid
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

    :param size: The maximum number of tasks to be running at the same time.
    :param max_queue_size: The maximum number of coroutines to be waiting in the queue for execution.
    """
    def __init__(self, max_tasks: int, max_queue_size: Optional[int] = None):
        assert max_tasks > 0, "Invalid max_tasks"
        assert max_queue_size is None or max_queue_size > 0, "Invalid max_queue_size"

        self._max_tasks = max_tasks
        self._queue = LazyProxy(
            lambda: asyncio.Queue(maxsize=max_tasks if max_queue_size is None else max_queue_size)
        )
        self._tasks: Dict[str, asyncio.Task] = {}
        self._queue_timeout = 1
        self._active = 0

    @property
    def idle(self) -> bool:
        """
        True if there are no active tasks in the pool, False otherwise.
        """
        return self._active == 0 and self._queue.empty()

    async def push(self, coroutine: Coroutine[Any, Any, Any]):
        """
        Adds a coroutine to the queue. It may block if the queue is full.

        :param coroutine: The coroutine to be added to the task pool.
        """

        await self._queue.put(coroutine)

        # Create a new task if necessary.
        if len(self._tasks) < self._max_tasks and self._queue.qsize():
            task_name = uuid.uuid4().hex
            task = asyncio.create_task(self._task_main(task_name))
            # We check before registering the task because if eager tasks are enabled (Python >= 3.12) the task may
            # have already ran by the time we got here.
            if not task.done() and task_name not in self._tasks:
                self._tasks[task_name] = task

    def cancel(self):
        """
        Requests all tasks in the pool to be canceled and clears the queue.
        """
        for task in self._tasks.values():
            task.cancel()

        # Empty the queue.
        while self._queue.qsize():
            self._queue.get_nowait()
            self._queue.task_done()

    async def wait(self, timeout: Optional[Union[int, float]] = None) -> bool:
        """
        Waits for all tasks in the pool to complete.

        :param timeout: The maximum number of seconds to wait for tasks to complete. If None, wait indefinitely.
        :returns: Returns True if all the coroutines in the queue have been processed, False otherwise.
        """

        ret = False
        try:
            await asyncio.wait_for(self._queue.join(), timeout=timeout)
            ret = True
        except asyncio.TimeoutError:
            pass
        return ret

    async def _task_main(self, task_name: str):
        current_task = asyncio.current_task()
        # Register ourselves in the task registry if not already there. This happens with eager tasks (Python >= 3.12).
        if current_task not in self._tasks:
            assert current_task is not None
            self._tasks[task_name] = current_task

        try:
            while True:
                coro = await asyncio.wait_for(self._queue.get(), timeout=self._queue_timeout)
                try:
                    self._active += 1
                    await coro
                except Exception:
                    # We don't want single task failures to take down the whole pool.
                    pass
                finally:
                    self._active -= 1
                    self._queue.task_done()
        except asyncio.TimeoutError:
            pass
        finally:
            # Remove ourselves from the task registry once we're done.
            self._tasks.pop(task_name)


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


class LazyProxy:
    def __init__(self, factory):
        self._factory = factory
        self._obj = None

    @property
    def initialized(self):
        return self._obj is not None

    @property
    def obj(self):
        if self._obj is None:
            self._obj = self._factory()
        return self._obj

    def __getattr__(self, name):
        return getattr(self.obj, name)
