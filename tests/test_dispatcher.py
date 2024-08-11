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


class Error(Exception):
    pass


class Producer(event.Producer):
    def __init__(self):
        self.initialized = False
        self.finalized = False
        self.ran = False
        self.stopped = False

    async def initialize(self):
        await super().initialize()
        assert not self.initialized
        self.initialized = True

    async def main(self):
        await super().main()
        assert not self.ran
        assert not self.stopped
        self.ran = True
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            self.stopped = True
            raise

    async def finalize(self):
        await super().finalize()
        assert not self.finalized
        self.finalized = True


class FailingProducer(event.Producer):
    def __init__(self, fail_initialize, fail_main, fail_finalize):
        self.fail_initialize = fail_initialize
        self.fail_main = fail_main
        self.fail_finalize = fail_finalize

    async def initialize(self):
        if self.fail_initialize:
            raise Error("Error during initialize")

    async def main(self):
        if self.fail_main:
            raise Error("Error during main")

    async def finalize(self):
        if self.fail_finalize:
            raise Error("Error during finalize")


def test_producers_and_events(realtime_dispatcher):
    shared_producer = Producer()
    event_sources = [
        event.FifoQueueEventSource(), event.FifoQueueEventSource(),
        event.FifoQueueEventSource(producer=Producer()), event.FifoQueueEventSource(producer=Producer()),
        event.FifoQueueEventSource(producer=shared_producer), event.FifoQueueEventSource(producer=shared_producer),
    ]
    events = []

    async def stop_dispatcher():
        while len(events) < len(event_sources):
            await asyncio.sleep(0.1)
        realtime_dispatcher.stop()

    async def save_events(event):
        events.append(event)

    async def test_main():
        for event_source in event_sources:
            event_source.push(event.Event(dt.utc_now()))
            realtime_dispatcher.subscribe(event_source, save_events)

        await asyncio.gather(realtime_dispatcher.run(), stop_dispatcher())

        assert len(events) == len(event_sources)
        for event_source in event_sources:
            if event_source.producer:
                assert event_source.producer.initialized
                assert event_source.producer.ran
                assert event_source.producer.stopped
                assert event_source.producer.finalized

    asyncio.run(asyncio.wait_for(test_main(), 2))


@pytest.mark.parametrize("failing_producer, other_initialized, other_ran, other_stopped, other_finalized", [
    (FailingProducer(True, False, False), True, False, False, True),
    (FailingProducer(False, True, False), True, True, True, True),
    (FailingProducer(False, True, True), True, True, True, True),
])
def test_exceptions_in_producers(
    failing_producer, other_initialized, other_ran, other_stopped, other_finalized, realtime_dispatcher
):
    shared_producer = Producer()
    event_sources = [
        event.FifoQueueEventSource(producer=failing_producer),
        event.FifoQueueEventSource(),
        event.FifoQueueEventSource(producer=Producer()), event.FifoQueueEventSource(producer=Producer()),
        event.FifoQueueEventSource(producer=shared_producer), event.FifoQueueEventSource(producer=shared_producer),
    ]
    events = []

    async def save_events(event):
        events.append(event)

    async def test_main():
        for event_source in event_sources:
            event_source.push(event.Event(dt.utc_now()))
            realtime_dispatcher.subscribe(event_source, save_events)

        with pytest.raises(Error):
            await realtime_dispatcher.run()

        # If the other sources run, there might be events, otherwise there shouldn't be events.
        assert other_ran or len(events) == 0
        for event_source in event_sources:
            if event_source.producer and event_source.producer != failing_producer:
                assert event_source.producer.initialized == other_initialized
                assert event_source.producer.ran == other_ran
                assert event_source.producer.stopped == other_stopped
                assert event_source.producer.finalized == other_finalized

    asyncio.run(asyncio.wait_for(test_main(), 2))


def test_out_of_order_events_are_skipped(realtime_dispatcher):
    events = []

    async def stop_dispatcher():
        while len(events) < 2:
            await asyncio.sleep(0.1)
        realtime_dispatcher.stop()

    async def save_events(event):
        events.append(event)

    async def test_main():
        src = event.FifoQueueEventSource(events=[
            event.Event(dt.utc_now()),
            event.Event(dt.utc_now() - datetime.timedelta(hours=1)),
            event.Event(dt.utc_now() + datetime.timedelta(milliseconds=250)),
        ])
        realtime_dispatcher.subscribe(src, save_events)

        await asyncio.gather(realtime_dispatcher.run(), stop_dispatcher())

        assert len(events) == 2

    asyncio.run(asyncio.wait_for(test_main(), 2))


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


@pytest.mark.parametrize("delta_seconds", [
    0.5,
    1,
    -0.5,
])
def test_realtime_scheduler(delta_seconds, realtime_dispatcher):
    async def scheduled_job():
        realtime_dispatcher.stop()

    async def test_main():
        realtime_dispatcher.schedule(
            realtime_dispatcher.now() + datetime.timedelta(seconds=delta_seconds), scheduled_job
        )
        await realtime_dispatcher.run()

    asyncio.run(asyncio.wait_for(test_main(), 5))


def test_stop_dispatcher_when_idle(realtime_dispatcher):
    handler_calls = 0

    async def on_event(event):
        nonlocal handler_calls
        handler_calls += 1

    async def on_idle():
        realtime_dispatcher.stop()

    src = event.FifoQueueEventSource(events=[
        event.Event(datetime.datetime(2000, 1, 1).replace(tzinfo=datetime.timezone.utc)),
        event.Event(datetime.datetime(2000, 1, 2).replace(tzinfo=datetime.timezone.utc)),
    ])
    realtime_dispatcher.subscribe(src, on_event)
    realtime_dispatcher.subscribe_idle(on_idle)
    asyncio.run(realtime_dispatcher.run())

    assert handler_calls == 2


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
    with pytest.raises(errors.Error, match="No events processed yet"):
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
