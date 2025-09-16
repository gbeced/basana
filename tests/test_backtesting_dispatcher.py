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
import datetime

import pytest

from . import helpers
from basana.core import dt, errors, event


def test_duplicate_subscription_is_ignored(backtesting_dispatcher):
    events = []

    async def save_events(event):
        events.append(event)

    async def test_main():
        src = event.FifoQueueEventSource(events=[event.Event(dt.utc_now())])
        backtesting_dispatcher.subscribe(src, save_events)
        backtesting_dispatcher.subscribe(src, save_events)

        await backtesting_dispatcher.run()

        assert len(events) == 1

    asyncio.run(test_main())


def test_subscription_order_per_source(backtesting_dispatcher):
    priorities = []

    async def test_main():
        src = event.FifoQueueEventSource(events=[event.Event(dt.utc_now())])

        handler_count = 100
        for i in range(handler_count):
            async def handler(e, i=i):
                priorities.append(i)
            backtesting_dispatcher.subscribe(src, handler)

        await backtesting_dispatcher.run()

        assert len(priorities) == handler_count
        assert priorities == list(range(handler_count))

    asyncio.run(test_main())


def test_sniffers(backtesting_dispatcher):
    handlers = []

    async def event_handler(event):
        handlers.append(2)

    async def all_event_handler(event):
        handlers.append(3)

    async def all_event_front_runner(event):
        handlers.append(1)

    async def test_main():
        src = event.FifoQueueEventSource(events=[event.Event(dt.utc_now())])

        backtesting_dispatcher.subscribe_all(all_event_handler)
        backtesting_dispatcher.subscribe(src, event_handler)
        backtesting_dispatcher.subscribe_all(all_event_front_runner, front_run=True)

        await backtesting_dispatcher.run()
        assert len(handlers) == 3
        assert helpers.is_sorted(handlers)

    asyncio.run(test_main())


@pytest.mark.parametrize("schedule_dates", [
    [
        datetime.datetime(2000, 1, 1, 0, 0, 1),
    ],
    [
        datetime.datetime(1999, 1, 1, 0, 0, 0),
        datetime.datetime(2000, 1, 2, 0, 0, 0),
        datetime.datetime(2001, 1, 2, 0, 0, 0),
        datetime.datetime(2002, 1, 1, 0, 0, 0),
        dt.local_now(),
        dt.utc_now(),
    ],
])
def test_backtesting_scheduler(schedule_dates, backtesting_dispatcher):
    datetimes = []

    def scheduled_job_factory(when):
        async def scheduled_job():
            datetimes.append(when)
        return scheduled_job

    async def proces_event(event):
        datetimes.append(event.when)

    async def failing_scheduled_job():
        raise Exception("oh no, oh no, oh no no no no")

    async def test_main():
        src = event.FifoQueueEventSource()
        event_datetimes = [
            datetime.datetime(2000, 1, 1),
            datetime.datetime(2001, 1, 1),
            datetime.datetime(2002, 1, 1),
        ]
        for when in event_datetimes:
            src.push(event.Event(when.replace(tzinfo=datetime.timezone.utc)))
        backtesting_dispatcher.subscribe(src, proces_event)

        for schedule_date in schedule_dates:
            if dt.is_naive(schedule_date):
                schedule_date = schedule_date.replace(tzinfo=datetime.timezone.utc)
            backtesting_dispatcher.schedule(schedule_date, scheduled_job_factory(schedule_date))
            backtesting_dispatcher.schedule(schedule_date, failing_scheduled_job)

        await backtesting_dispatcher.run()

        assert helpers.is_sorted(datetimes)
        assert len(datetimes) == len(schedule_dates) + len(event_datetimes)

    asyncio.run(test_main())


def test_handler_exceptions_dont_stop_the_dispatcher(backtesting_dispatcher):
    handler_calls = 0
    scheduler_handler_calls = 0

    async def event_handler(event):
        nonlocal handler_calls
        handler_calls += 1
        raise Exception("Event handler error")

    async def scheduler_handler():
        nonlocal scheduler_handler_calls
        scheduler_handler_calls += 1
        raise Exception("Scheduler handler error")

    async def test_main():
        src = event.FifoQueueEventSource(events=[
            event.Event(dt.utc_now()),
            event.Event(dt.utc_now()),
            event.Event(dt.utc_now()),
        ])

        backtesting_dispatcher.subscribe_all(event_handler, front_run=True)
        backtesting_dispatcher.subscribe(src, event_handler)
        backtesting_dispatcher.subscribe_all(event_handler)
        backtesting_dispatcher.schedule(dt.utc_now(), scheduler_handler)

        await backtesting_dispatcher.run()
        assert handler_calls == 3 * 3
        assert scheduler_handler_calls == 1

    asyncio.run(test_main())


def test_handler_exceptions_stop_the_dispatcher(backtesting_dispatcher):
    backtesting_dispatcher.stop_on_handler_exceptions = True
    handler_calls = 0

    async def event_handler(event):
        nonlocal handler_calls
        handler_calls += 1
        raise Exception("Event handler error")

    async def event_handler_2(event):
        nonlocal handler_calls
        handler_calls += 1
        raise Exception("Event handler error")

    async def test_main():
        src = event.FifoQueueEventSource(events=[
            event.Event(dt.utc_now()),
            event.Event(dt.utc_now()),
            event.Event(dt.utc_now()),
        ])

        backtesting_dispatcher.subscribe(src, event_handler)
        backtesting_dispatcher.subscribe(src, event_handler_2)

        await backtesting_dispatcher.run()
        assert handler_calls == 1

    asyncio.run(test_main())


def test_scheduler_handler_exceptions_stop_the_dispatcher(backtesting_dispatcher):
    backtesting_dispatcher.stop_on_handler_exceptions = True
    handler_calls = 0

    async def event_handler(event):
        pass

    async def scheduler_handler():
        nonlocal handler_calls
        handler_calls += 1
        raise Exception("Scheduler handler error")

    async def scheduler_handler_2():
        nonlocal handler_calls
        handler_calls += 1
        raise Exception("Scheduler handler error")

    async def test_main():
        src = event.FifoQueueEventSource(events=[
            event.Event(dt.utc_now()),
            event.Event(dt.utc_now()),
            event.Event(dt.utc_now()),
        ])

        backtesting_dispatcher.subscribe(src, event_handler)
        backtesting_dispatcher.schedule(dt.utc_now(), scheduler_handler)
        backtesting_dispatcher.schedule(dt.utc_now(), scheduler_handler_2)
        await backtesting_dispatcher.run()

        assert handler_calls == 1

    asyncio.run(test_main())


def test_now_fails_if_no_events_were_processed(backtesting_dispatcher):
    with pytest.raises(errors.Error, match="Can't calculate current datetime since no events were processed"):
        backtesting_dispatcher.now()


def test_recursive_schedule_bug(backtesting_dispatcher):
    jobs_processed = 0

    async def scheduled_job():
        nonlocal jobs_processed
        jobs_processed += 1
        next_dt = backtesting_dispatcher.now() + datetime.timedelta(hours=1)
        backtesting_dispatcher.schedule(next_dt, scheduled_job)

    async def proces_event(event):
        next_dt = event.when + datetime.timedelta(hours=1)
        backtesting_dispatcher.schedule(next_dt, scheduled_job)

    async def test_main():
        src = event.FifoQueueEventSource(events=[
            event.Event(datetime.datetime(2024, 1, 1).replace(tzinfo=datetime.timezone.utc)),
        ])
        backtesting_dispatcher.subscribe(src, proces_event)
        await backtesting_dispatcher.run()
        assert jobs_processed == 1

    asyncio.run(test_main())
