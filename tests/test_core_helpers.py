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

import pytest

from basana.core import helpers


async def set_event(event, wait=0):
    if wait:
        await asyncio.sleep(wait)
    event.set()


async def wait_event(event):
    await event.wait()


async def fail(msg, wait=0):
    if wait:
        await asyncio.sleep(wait)
    raise Exception(msg)


async def cancel_task_group(task_group, wait=0):
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


def test_task_group_awaited_before_exit():
    async def impl():
        events = [asyncio.Event() for _ in range(3)]

        async with helpers.TaskGroup() as tg:
            for event in events:
                tg.create_task(set_event(event))

            await asyncio.gather(*[event.wait() for event in events])
            for ev in events:
                assert ev.is_set()

    asyncio.run(asyncio.wait_for(impl(), 3))


def test_task_group_awaited_on_exit():
    async def impl():
        events = [asyncio.Event() for _ in range(3)]

        async with helpers.TaskGroup() as tg:
            for event in events:
                tg.create_task(set_event(event))

        for ev in events:
            assert ev.is_set()

    asyncio.run(impl())


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

    asyncio.run(impl())


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

    asyncio.run(impl())


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
            await pool.push(task())
        await pool.wait()

        assert pool.idle
        tasks = pool.pop_done()
        assert len(tasks) == task_count
        assert task_calls == task_count
        assert all([task.done() for task in tasks])

    asyncio.run(test_main())


@pytest.mark.parametrize("obj, expected_classpath", [
    ("hi", "builtins.str"),
    (3, "builtins.int"),
])
def test_classpath(obj, expected_classpath):
    assert helpers.classpath(obj) == expected_classpath
