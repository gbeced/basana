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

import datetime

from basana.core import dispatcher


async def scheduler_job():
    pass


def test_scheduler_queue():
    queue = dispatcher.SchedulerQueue()

    assert queue.peek_next_event_dt() is None
    assert queue.peek_last_event_dt() is None

    now = datetime.datetime(2000, 1, 1, tzinfo=datetime.UTC)
    queue.push(now, scheduler_job)
    expected_next = expected_last = now
    assert queue.peek_next_event_dt() == expected_next
    assert queue.peek_last_event_dt() == expected_last

    # Should go next.
    expected_next = now - datetime.timedelta(seconds=5)
    queue.push(expected_next, scheduler_job)
    assert queue.peek_next_event_dt() == expected_next
    assert queue.peek_last_event_dt() == expected_last

    # Should go in between.
    queue.push(now - datetime.timedelta(seconds=3), scheduler_job)
    assert queue.peek_next_event_dt() == expected_next
    assert queue.peek_last_event_dt() == expected_last
