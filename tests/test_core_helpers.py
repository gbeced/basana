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
import asyncio
import sys

import pytest

from basana.core import helpers


async def set_event(event, wait: float = 0.0):
    if wait:
        await asyncio.sleep(wait)
    event.set()


async def wait_event(event):
    await event.wait()


async def fail(msg, wait: float = 0.0):
    if wait:
        await asyncio.sleep(wait)
    raise Exception(msg)


async def cancel_task_group(task_group, wait: float = 0.0):
    if wait:
        await asyncio.sleep(wait)
    task_group.cancel()


@pytest.mark.parametrize("amount, precision, expected", [
    ("1", 0, "1"),
    ("1.1", 0, "1"),
    ("-1.1", 0, "-1"),
    ("-1.1999", 1, "-1.1"),
    ("0.2999", 2, "0.29"),
])
def test_truncate(amount, precision, expected):
    assert helpers.truncate_decimal(Decimal(amount), precision) == Decimal(expected)


def test_task_group_awaited_before_scope_exit():
    async def impl():
        events = [asyncio.Event() for _ in range(3)]

        async with helpers.TaskGroup() as tg:
            for event in events:
                tg.create_task(set_event(event, 0.1))

            await asyncio.gather(*[event.wait() for event in events])
            for ev in events:
                assert ev.is_set()

    asyncio.run(asyncio.wait_for(impl(), 1))


def test_task_group_awaited_on_scope_exit():
    async def impl():
        events = [asyncio.Event() for _ in range(3)]

        async with helpers.TaskGroup() as tg:
            for event in events:
                tg.create_task(set_event(event, 0.1))

        for ev in events:
            assert ev.is_set()

    asyncio.run(asyncio.wait_for(impl(), 1))


def test_task_group_finishes_with_errors():
    async def impl():
        should_be_set = asyncio.Event()
        should_not_be_set = asyncio.Event()

        with pytest.raises(Exception, match="some error"):
            async with helpers.TaskGroup() as tg:
                tg.create_task(set_event(should_be_set))
                tg.create_task(set_event(should_not_be_set, 2))
                tg.create_task(fail("some error", 0.2))

        assert should_be_set.is_set()
        assert not should_not_be_set.is_set()

    asyncio.run(asyncio.wait_for(impl(), 1))


def test_cancel_task_group():
    async def impl():
        should_be_set = asyncio.Event()
        should_not_be_set = asyncio.Event()

        with pytest.raises(asyncio.exceptions.CancelledError):
            async with helpers.TaskGroup() as tg:
                tg.create_task(set_event(should_be_set))
                tg.create_task(set_event(should_not_be_set, 2))
                tg.create_task(cancel_task_group(tg, 0.2))

        assert should_be_set.is_set()
        assert not should_not_be_set.is_set()

    asyncio.run(asyncio.wait_for(impl(), 1))


@pytest.mark.parametrize("pool_size, task_count", [
    (1, 1),
    (1, 2),
    (10, 200),
])
def test_task_pool(pool_size, task_count):
    task_calls = 0

    async def task():
        nonlocal task_calls
        task_calls += 1

    async def test_main():
        pool = helpers.TaskPool(pool_size)
        for _ in range(task_count):
            await pool.push(task)
        await pool.wait()

        assert pool.idle
        assert task_calls == task_count

    asyncio.run(asyncio.wait_for(test_main(), 1))


def test_task_pool_cancel():
    some_handler_called = helpers.LazyProxy(asyncio.Event)

    async def task():
        some_handler_called.set()
        await asyncio.sleep(60)

    async def test_main():
        pool_size = 2
        task_count = 3
        pool = helpers.TaskPool(pool_size, max_queue_size=pool_size)
        for _ in range(task_count):
            await pool.push(task)
        await some_handler_called.wait()

        assert not pool.idle

        pool.cancel()
        await pool.wait()

        assert pool.idle

    asyncio.run(asyncio.wait_for(test_main(), 5))


def test_task_pool_wait():

    async def task():
        await asyncio.sleep(0.5)

    async def test_main():
        pool_size = 10
        task_count = 5
        pool = helpers.TaskPool(pool_size)
        for _ in range(task_count):
            await pool.push(task)
        done = await pool.wait(0.05)

        assert not done
        assert not pool.idle

        done = await pool.wait()

        assert done
        assert pool.idle

        pool.cancel()

    asyncio.run(asyncio.wait_for(test_main(), 5))


def test_task_pool_with_failing_tasks():
    async def test_main():
        pool_size = 1
        task_count = 10
        pool = helpers.TaskPool(pool_size, max_queue_size=task_count)
        for _ in range(task_count):
            await pool.push(lambda: fail("some error", 0.01))
        done = await pool.wait(1)

        assert done
        assert pool.idle

    asyncio.run(asyncio.wait_for(test_main(), 2))


def test_task_pool_task_auto_quit():

    async def task():
        pass

    async def test_main():
        pool = helpers.TaskPool(5)
        pool._queue_timeout = 0.01
        await pool.push(task)
        assert len(pool._tasks) == 1

        done = await pool.wait()
        assert done
        assert pool.idle
        assert len(pool._tasks) == 1

        await asyncio.sleep(pool._queue_timeout * 2)
        assert len(pool._tasks) == 0

    asyncio.run(asyncio.wait_for(test_main(), 1))


if sys.version_info >= (3, 12):
    @pytest.mark.parametrize("task_factory", [
        None,
        asyncio.eager_task_factory,
    ])
    def test_task_pool_with_eager_tasks(task_factory):
        tasks_found = 0
        pool = helpers.TaskPool(10)

        async def task():
            nonlocal tasks_found
            tasks_found = len(pool._tasks)

        async def test_main():
            if task_factory:
                asyncio.get_running_loop().set_task_factory(task_factory)

            await pool.push(task)
            done = await pool.wait(0.01)

            assert done
            assert tasks_found == 1
            assert pool.idle

        asyncio.run(asyncio.wait_for(test_main(), 1))


@pytest.mark.parametrize("obj, expected_classpath", [
    ("hi", "builtins.str"),
    (3, "builtins.int"),
])
def test_classpath(obj, expected_classpath):
    assert helpers.classpath(obj) == expected_classpath


def test_lazy_proxy():
    class ExpensiveObject:
        def __init__(self):
            self.value = 42

    proxy = helpers.LazyProxy(ExpensiveObject)
    assert proxy.initialized is False
    assert proxy.value == 42
    assert proxy.obj.value == 42
    assert proxy.initialized is True
