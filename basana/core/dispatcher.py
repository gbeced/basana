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

from typing import Awaitable, Any, Callable, Dict, List, Set, Optional
import asyncio
import datetime
import logging
import signal

from . import event, helpers, logs


logger = logging.getLogger(__name__)
EventHandler = Callable[[event.Event], Awaitable[Any]]
IdleHandler = Callable[[], Awaitable[Any]]


class EventDispatcher:
    def __init__(self, strict_order: bool = True, stop_when_idle: bool = True):
        self._event_handlers: Dict[event.EventSource, Set[EventHandler]] = {}
        self._prefetched_events: Dict[event.EventSource, event.Event] = {}
        self._prev_events: Dict[event.EventSource, datetime.datetime] = {}
        self._idle_handlers: Set[IdleHandler] = set()
        self._producers: Set[event.Producer] = set()
        self._open_task_group: Optional[helpers.TaskGroup] = None
        self._strict_order = strict_order
        self._stop_when_idle = stop_when_idle
        self._running = False
        self._stopped = False
        self._current_event_dt = None

    @property
    def current_event_dt(self) -> Optional[datetime.datetime]:
        return self._current_event_dt

    def stop(self):
        self._stopped = True
        if self._open_task_group:
            self._open_task_group.cancel()

    def subscribe_idle(self, idle_handler: IdleHandler):
        # Called when there are no events to dispatch.
        self._idle_handlers.add(idle_handler)

    def subscribe(self, source: event.EventSource, event_handler: EventHandler):
        assert not self._running
        self._event_handlers.setdefault(source, set()).add(event_handler)
        if source.producer:
            self._producers.add(source.producer)

    async def run(self, stop_signals: List[int] = [signal.SIGINT, signal.SIGTERM]):
        assert not self._running, "Running or already ran"
        assert self._open_task_group is None

        for stop_signal in stop_signals:
            asyncio.get_event_loop().add_signal_handler(stop_signal, self.stop)

        self._running = True
        try:
            # Initialize producers.
            async with helpers.TaskGroup() as tg:
                self._open_task_group = tg  # So it can be canceled.
                for producer in self._producers:
                    tg.create_task(producer.initialize())
            # Run producers and dispatch loop.
            async with helpers.TaskGroup() as tg:
                self._open_task_group = tg  # So it can be canceled.
                for producer in self._producers:
                    tg.create_task(producer.main())
                tg.create_task(self._dispatch_loop())
        except asyncio.CancelledError:
            pass
        finally:
            # No more cancelation at this point.
            self._open_task_group = None
            # Finalize producers.
            coros = [await_no_raise(producer.finalize()) for producer in self._producers]
            await asyncio.gather(*coros)

    def on_error(self, error: Any):
        logger.error(error)

    def _prefetch(self):
        sources_to_pop = [
            source for source in self._event_handlers.keys() if self._prefetched_events.get(source) is None
        ]
        for source in sources_to_pop:
            if event := source.pop():
                # Check that events from the same source are returned in order.
                prev_event = self._prev_events.get(source)
                if prev_event is not None and event.when < prev_event.when:
                    self.on_error(logs.StructuredMessage(
                        "Events returned out of order", source=source, previous=prev_event.when, current=event.when
                    ))
                    continue

                self._prev_events[source] = event
                self._prefetched_events[source] = event

    def _get_next_dt_from_prefetched(self):
        next_dt = None
        prefetched_events = [e for e in self._prefetched_events.values() if e]
        if prefetched_events:
            next_dt = min(map(lambda e: e.when, prefetched_events))
        return next_dt

    async def _dispatch_next(self, ge_or_assert):
        # Pre-fetch events from all sources.
        self._prefetch()

        # Calculate the datetime for the next event using the prefetched events.
        next_dt = self._get_next_dt_from_prefetched()
        assert ge_or_assert is None or next_dt is None or next_dt >= ge_or_assert, \
            f"{next_dt} can't be dispatched after {ge_or_assert}"

        # Dispatch events matching the desired datetime.
        event_handlers = []
        for source, e in self._prefetched_events.items():
            if e is not None and e.when == next_dt:
                # Collect event handlers for the event source.
                event_handlers += [event_handler(e) for event_handler in self._event_handlers.get(source, [])]
                # Consume the event.
                self._prefetched_events[source] = None

        self._current_event_dt = next_dt
        await asyncio.gather(*event_handlers)
        self._current_event_dt = None

        return next_dt

    async def _dispatch_loop(self):
        last_dt = None

        while not self._stopped:
            dispatched_dt = await self._dispatch_next(last_dt if self._strict_order else None)
            if dispatched_dt is None:
                if self._stop_when_idle:
                    self.stop()
                elif self._idle_handlers:
                    await asyncio.gather(*[event_handler() for event_handler in self._idle_handlers])
                else:
                    # Otherwise we'll monopolize the event loop.
                    await asyncio.sleep(0.01)
            else:
                assert not self._strict_order or last_dt is None or dispatched_dt >= last_dt, \
                    f"{dispatched_dt} dispatched after {last_dt}"
                last_dt = dispatched_dt


class BacktestingDispatcher(EventDispatcher):
    def __init__(self):
        super().__init__(strict_order=True, stop_when_idle=True)

    async def run(self, stop_signals: List[int] = [signal.SIGINT, signal.SIGTERM]):
        with logs.backtesting_log_mode(self):
            await super().run(stop_signals=stop_signals)


async def await_no_raise(coro: Awaitable[Any], message: str = "Unhandled exception"):
    with helpers.no_raise(logger, message):
        await coro


def realtime_dispatcher():
    return EventDispatcher(strict_order=False, stop_when_idle=False)


def backtesting_dispatcher():
    return BacktestingDispatcher()
