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

import asyncio
import time

import pytest

from basana.core.token_bucket import TokenBucketLimiter


@pytest.mark.parametrize("tokens_per_period, period_duration, initial_tokens, expected_wait", [
    (1, 1, 0, 1),
    (1, 1, 1, 0),
    (10, 1, 0, 0.1),
    (10, 7, 0, 0.7),
    (1, 2, 0, 2),
    (0.5, 1, 0, 2),
])
def test_token_consume(tokens_per_period, period_duration, initial_tokens, expected_wait):
    limiter = TokenBucketLimiter(tokens_per_period, period_duration, initial_tokens=initial_tokens)
    assert limiter.tokens == initial_tokens
    assert limiter.tokens_per_period == tokens_per_period
    assert limiter.period_duration == period_duration
    assert round(limiter.consume(), 2) == expected_wait


def test_token_wait():
    limiter = TokenBucketLimiter(2, 1)
    begin = time.time()
    asyncio.run(limiter.wait())
    assert round(time.time() - begin, 1) == 0.5


def test_dont_accumulate():
    limiter = TokenBucketLimiter(100, 1, 100)
    time.sleep(0.1)
    assert limiter.consume() == 0
