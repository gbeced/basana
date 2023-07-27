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
import datetime

import pytest

from . import helpers
from basana.core import dispatcher, dt, event


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


def test_producers_and_events():
    d = dispatcher.EventDispatcher(strict_order=False, stop_when_idle=False)
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
        d.stop()

    async def save_events(event):
        events.append(event)

    async def test_main():
        for event_source in event_sources:
            event_source.push(event.Event(dt.utc_now()))
            d.subscribe(event_source, save_events)

        await asyncio.gather(d.run(), stop_dispatcher())

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
def test_exceptions_in_producers(failing_producer, other_initialized, other_ran, other_stopped, other_finalized):
    d = dispatcher.EventDispatcher(strict_order=False, stop_when_idle=False)
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
            d.subscribe(event_source, save_events)

        with pytest.raises(Error):
            await d.run()

        # If the other sources run, there might be events, otherwise there shouldn't be events.
        assert other_ran or len(events) == 0
        for event_source in event_sources:
            if event_source.producer and event_source.producer != failing_producer:
                assert event_source.producer.initialized == other_initialized
                assert event_source.producer.ran == other_ran
                assert event_source.producer.stopped == other_stopped
                assert event_source.producer.finalized == other_finalized

    asyncio.run(asyncio.wait_for(test_main(), 2))


def test_out_of_order_events_are_skipped():
    d = dispatcher.realtime_dispatcher()
    events = []

    async def stop_dispatcher():
        while len(events) < 2:
            await asyncio.sleep(0.1)
        d.stop()

    async def save_events(event):
        events.append(event)

    async def test_main():
        src = event.FifoQueueEventSource(events=[
            event.Event(dt.utc_now()),
            event.Event(dt.utc_now() - datetime.timedelta(hours=1)),
            event.Event(dt.utc_now() + datetime.timedelta(hours=1)),
        ])
        d.subscribe(src, save_events)

        await asyncio.gather(d.run(), stop_dispatcher())

        assert len(events) == 2

    asyncio.run(asyncio.wait_for(test_main(), 2))


def test_duplicate_subscription_is_ignored():
    d = dispatcher.backtesting_dispatcher()
    events = []

    async def save_events(event):
        events.append(event)

    async def test_main():
        src = event.FifoQueueEventSource(events=[event.Event(dt.utc_now())])
        d.subscribe(src, save_events)
        d.subscribe(src, save_events)

        await d.run()

        assert len(events) == 1

    asyncio.run(test_main())


def test_subscription_order_per_source():
    d = dispatcher.backtesting_dispatcher()
    priorities = []

    async def test_main():
        src = event.FifoQueueEventSource(events=[event.Event(dt.utc_now())])

        handler_count = 100
        for i in range(handler_count):
            async def handler(e, i=i):
                priorities.append(i)
            d.subscribe(src, handler)

        await d.run()

        assert len(priorities) == handler_count
        assert priorities == list(range(handler_count))

    asyncio.run(test_main())


def test_sniffers():
    d = dispatcher.backtesting_dispatcher()
    events = []
    sniffed_events = []

    async def save_event(event):
        events.append(event)

    async def save_sniffed_event(event):
        sniffed_events.append(event)

    async def test_main():
        src_count = 10

        d.subscribe_all(save_sniffed_event)
        for _ in range(src_count):
            src = event.FifoQueueEventSource(events=[event.Event(dt.utc_now())])
            d.subscribe(src, save_event)

        await d.run()

        assert len(events) == src_count
        assert len(sniffed_events) == src_count

    asyncio.run(test_main())


@pytest.mark.parametrize("schedule_dates", [
    [
        datetime.datetime(2000, 1, 1, 0, 0, 1),
    ],
    [
        datetime.datetime(1999, 1, 1, 0, 0, 0),
        datetime.datetime(2000, 1, 2, 0, 0, 0),
        datetime.datetime(2001, 1, 2, 0, 0, 0),
        datetime.datetime(2002, 1, 2, 0, 0, 0),
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
            schedule_date = schedule_date.replace(tzinfo=datetime.timezone.utc)
            backtesting_dispatcher.schedule(schedule_date, scheduled_job_factory(schedule_date))

        await backtesting_dispatcher.run()

        assert helpers.is_sorted(datetimes)
        assert len(datetimes) == len(schedule_dates) + 3

    asyncio.run(test_main())


@pytest.mark.parametrize("delta_seconds, timeout", [
    (0.5, 1),
    (0.8, 1),
])
def test_realtime_scheduler(delta_seconds, timeout, realtime_dispatcher):
    async def scheduled_job():
        realtime_dispatcher.stop()

    async def test_main():
        realtime_dispatcher.schedule(dt.utc_now() + datetime.timedelta(seconds=delta_seconds), scheduled_job)
        await asyncio.wait_for(realtime_dispatcher.run(), timeout=timeout)

    asyncio.run(test_main())
