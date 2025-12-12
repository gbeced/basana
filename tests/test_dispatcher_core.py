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

from basana.core import dispatcher, dt, event


async def scheduler_job():
    pass


def test_scheduler_queue():
    queue = dispatcher.SchedulerQueue()

    assert queue.peek_next_event_dt() is None
    assert queue.peek_last_event_dt() is None

    now = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
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


class Event(event.Event):
    def __init__(self, when: datetime.datetime, value: int):
        super().__init__(when)
        self.value = value


def test_multiplexer_priority():
    mux = dispatcher.EventMultiplexer()
    when = dt.utc_now()
    event_1 = Event(when, 1)
    event_2 = Event(when, 2)
    event_3 = Event(when, 3)
    src_1 = event.FifoQueueEventSource(events=[event_1])
    src_2 = event.FifoQueueEventSource(events=[event_2])
    src_3 = event.FifoQueueEventSource(events=[event_3])
    src_2.priority = src_3.priority + 1
    src_1.priority = src_2.priority + 1

    mux.add(src_2)
    mux.add(src_1)
    mux.add(src_3)

    events = []
    while next_dt := mux.peek_next_event_dt():
        _, evnt = mux.pop(next_dt)
        events.append(evnt)

    assert events == [event_1, event_2, event_3]