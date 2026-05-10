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

from typing import Any, Awaitable, Callable, Optional, Tuple
import dataclasses
import datetime
import heapq

from .. import dt


SchedulerJob = Callable[[], Awaitable[Any]]


@dataclasses.dataclass(order=True)
class ScheduledJob:
    when: datetime.datetime
    job: SchedulerJob = dataclasses.field(compare=False)  # Comparing function objects fails on Win32


class SchedulerQueue:
    """
    A priority queue for scheduler jobs.
    Jobs are stored in ascending order by their scheduled execution time.
    This allows for efficient peek and pop operations of the next scheduled job.
    """

    def __init__(self):
        self._queue = []

    def __bool__(self):
        return bool(self._queue)

    def push(self, when: datetime.datetime, job: SchedulerJob):
        assert not dt.is_naive(when), f"{when} should have timezone information set"
        heapq.heappush(self._queue, ScheduledJob(when=when, job=job))

    def peek_next_event_dt(self) -> Optional[datetime.datetime]:
        ret = None
        if self._queue:
            ret = self._queue[0].when
        return ret

    def peek_last_event_dt(self) -> Optional[datetime.datetime]:
        ret = None
        if self._queue:
            ret = heapq.nlargest(1, self._queue)[0].when
        return ret

    def pop(self) -> Tuple[datetime.datetime, SchedulerJob]:
        assert self._queue
        scheduled_job = heapq.heappop(self._queue)
        return scheduled_job.when, scheduled_job.job
