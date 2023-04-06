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
import time


class TokenBucketLimiter:
    """This class implements a token bucket algorithm, useful for throttling requests.

    :param tokens_per_period: The maximum amount of tokens per perdiod.
    :param period_duration: The period duration in seconds.
    :param initial_tokens: The initial amount of tokens.
    """

    def __init__(self, tokens_per_period: float, period_duration: int, initial_tokens=0):
        assert tokens_per_period > 0
        assert period_duration > 0
        assert initial_tokens >= 0

        self._tokens_per_period = tokens_per_period
        self._period_duration = period_duration
        self._tokens = initial_tokens
        self._last = time.time()

    @property
    def tokens(self) -> int:
        return max(int(self._tokens), 0)

    @property
    def tokens_per_period(self) -> float:
        return self._tokens_per_period

    @property
    def period_duration(self) -> int:
        return self._period_duration

    def consume(self) -> float:
        """Consumes one token and returns the time to wait before using it."""

        # Refill pool of tokens.
        now = time.time()
        lapse = now - self._last
        self._last = now
        self._tokens += lapse / self._period_duration * self._tokens_per_period
        if self._tokens > self._tokens_per_period:
            self._tokens = self._tokens_per_period

        # Consume one token.
        self._tokens -= 1

        if self._tokens >= 0:
            return 0.0
        else:
            return -self._tokens / self._tokens_per_period * self._period_duration

    async def wait(self):
        await asyncio.sleep(self.consume())
