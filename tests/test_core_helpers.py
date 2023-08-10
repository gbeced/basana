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

import asyncio

import pytest

from basana.core import helpers


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
        idle = await pool.wait_all()
        assert idle
        assert task_calls == task_count

    asyncio.run(test_main())
