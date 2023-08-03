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

from typing import cast, Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple
import asyncio
import contextlib
import datetime
import heapq
import logging
import platform
import signal

from . import dt, event, helpers, logs


logger = logging.getLogger(__name__)
EventHandler = Callable[[event.Event], Awaitable[Any]]
IdleHandler = Callable[[], Awaitable[Any]]
SchedulerJob = Callable[[], Awaitable[Any]]


class JobScheduler:
    def __init__(self):
        self._queue = []

    def schedule(self, when: datetime.datetime, job: SchedulerJob):
        assert not dt.is_naive(when), f"{when} should have timezone information set"
        heapq.heappush(self._queue, (when, job))

    def peek(self) -> Optional[datetime.datetime]:
        ret = None
        if self._queue:
            ret = self._queue[0][0]
        return ret

    async def run(self, now: datetime.datetime):
        """
        Execute jobs scheduled before a certain date and time.
        """
        coros: List[Awaitable[Any]] = []
        while self._queue and self._queue[0][0] <= now:
            _, job = heapq.heappop(self._queue)
            coros.append(job())
        await asyncio.gather(*coros)


class EventDispatcher:
    """Responsible for connecting event sources to event handlers and dispatching events in the right order.

    :param strict_order: True to abort if an event arrives out of order.
    :param stop_when_idle: True to stop when there are no more events to dispatch.

    .. note::

        The following helper functions are provided to build event dispatchers suitable for backtesting or for live
        trading:

        * :func:`basana.backtesting_dispatcher`
        * :func:`basana.realtime_dispatcher`
    """

    def __init__(self, strict_order: bool = True, stop_when_idle: bool = True):
        self._event_handlers: Dict[event.EventSource, List[EventHandler]] = {}
        self._prefetched_events: Dict[event.EventSource, Optional[event.Event]] = {}
        self._prev_events: Dict[event.EventSource, datetime.datetime] = {}
        self._idle_handlers: List[IdleHandler] = []
        self._sniffers_pre: List[EventHandler] = []
        self._sniffers_post: List[EventHandler] = []
        self._producers: Set[event.Producer] = set()
        self._open_task_group: Optional[helpers.TaskGroup] = None
        self._strict_order = strict_order
        self._stop_when_idle = stop_when_idle
        self._running = False
        self._stopped = False
        self._current_event_dt: Optional[datetime.datetime] = None
        self._scheduler = JobScheduler()
        self.idle_sleep = 0.01

    @property
    def current_event_dt(self) -> Optional[datetime.datetime]:
        """The datetime of the event that is currently being processed."""
        return self._current_event_dt

    @property
    def stopped(self) -> bool:
        """Returns True if stop was called, False otherwise."""
        return self._stopped

    def stop(self):
        """Requests the event dispatcher to stop the event processing loop."""
        self._stopped = True
        if self._open_task_group:
            self._open_task_group.cancel()

    def subscribe_idle(self, idle_handler: IdleHandler):
        """Registers an async callable that will be called when there are no events to dispatch.

        :param idle_handler: An async callable that receives no arguments.
        """

        assert not self._running, "Subscribing once we're running is not currently supported."

        if idle_handler not in self._idle_handlers:
            self._idle_handlers.append(idle_handler)

    def subscribe(self, source: event.EventSource, event_handler: EventHandler):
        """Registers an async callable that will be called when an event source has new events.

        :param source: An event source.
        :param event_handler: An async callable that receives an event.
        """

        assert not self._running, "Subscribing once we're running is not currently supported."

        handlers = self._event_handlers.setdefault(source, [])
        if event_handler not in handlers:
            handlers.append(event_handler)
        if source.producer:
            self._producers.add(source.producer)

    def subscribe_all(self, event_handler: EventHandler, front_run: bool = False):
        """Registers an async callable that will be called for all events.

        :param event_handler: An async callable that receives an event.
        :param front_run: True to front run all other handlers, False otherwise.
        """

        assert not self._running, "Subscribing once we're running is not currently supported."

        sniffers = self._sniffers_pre if front_run else self._sniffers_post
        if event_handler not in sniffers:
            sniffers.append(event_handler)

    def schedule(self, when: datetime.datetime, job: SchedulerJob):
        """TODO

        :param when: TODO.
        :param job: TODO.
        """
        self._scheduler.schedule(when, job)

    async def run(self, stop_signals: List[int] = [signal.SIGINT, signal.SIGTERM]):
        """Executes the event dispatch loop.

        :param stop_signals: The signals that will be handled to request :func:`run()` to :func:`stop()`.

        This method will execute the following steps in order:

        #. Call :meth:`basana.Producer.initialize` on all producers.
        #. Call :meth:`basana.Producer.main` on all producers and execute event dispatch loop until stopped.
        #. Call :meth:`basana.Producer.finalize` on all producers.
        """

        assert not self._running, "Can't run twice."
        assert self._open_task_group is None

        # This block has coverage on all platforms except on Windows.
        if platform.system() != "Windows":  # pragma: no cover
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

    @contextlib.contextmanager
    def _current_event_cm(self, current_event_dt: datetime.datetime):
        prev_dt = self.current_event_dt
        try:
            self._current_event_dt = current_event_dt
            yield
        finally:
            self._current_event_dt = prev_dt

    async def _before_idle_stop(self):  # pragma: no cover
        pass

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

    async def _dispatch_next(self, ge_or_assert: Optional[datetime.datetime]):
        # Pre-fetch events from all sources.
        self._prefetch()

        # Calculate the datetime for the next event using the prefetched events.
        next_dt = self._get_next_dt_from_prefetched()
        assert ge_or_assert is None or next_dt is None or next_dt >= ge_or_assert, \
            f"{next_dt} can't be dispatched after {ge_or_assert}"

        # Collect and dispatch events matching the desired datetime.
        if next_dt:
            def event_matches(t: Tuple[event.EventSource, Optional[event.Event]]) -> bool:
                event = t[1]
                return True if event and event.when == next_dt else False

            sniffers_pre = []
            event_handlers = []
            sniffers_post = []
            for source, e in filter(event_matches, self._prefetched_events.items()):
                e = cast(event.Event, e)  # No longer Optional after being filtered.
                # Sniffers that should run before other event handlers.
                sniffers_pre += [event_handler(e) for event_handler in self._sniffers_pre]
                # Collect event handlers for this particular event source.
                event_handlers += [event_handler(e) for event_handler in self._event_handlers.get(source, [])]
                # Sniffers that should run after other event handlers.
                sniffers_post += [event_handler(e) for event_handler in self._sniffers_post]
                # Consume the event.
                self._prefetched_events[source] = None

            with self._current_event_cm(next_dt):
                if sniffers_pre:
                    await asyncio.gather(*sniffers_pre)
                await asyncio.gather(*event_handlers)
                if sniffers_post:
                    await asyncio.gather(*sniffers_post)

        return next_dt

    async def _dispatch_loop(self):
        last_dt = None

        while not self._stopped:
            dispatched_dt = await self._dispatch_next(last_dt if self._strict_order else None)
            if dispatched_dt is None:
                if self._stop_when_idle:
                    await self._before_idle_stop()
                    self.stop()
                elif self._idle_handlers:
                    # TODO: Should we put some throttling here ?
                    # TokenBucket to limit idle calls to no more than 100 / s ?
                    await asyncio.gather(*[event_handler() for event_handler in self._idle_handlers])
                else:
                    # Otherwise we'll monopolize the event loop.
                    await asyncio.sleep(self.idle_sleep)
            else:
                last_dt = dispatched_dt

    async def _run_scheduler(self, now: datetime.datetime):
        with self._current_event_cm(now):
            await self._scheduler.run(now)


class BacktestingDispatcher(EventDispatcher):
    def __init__(self):
        super().__init__(strict_order=True, stop_when_idle=True)
        self.subscribe_all(self._before_event, front_run=True)

    async def run(self, stop_signals: List[int] = [signal.SIGINT, signal.SIGTERM]):
        with logs.backtesting_log_mode(self):
            await super().run(stop_signals=stop_signals)

    async def _before_event(self, event: event.Event):
        # Execute jobs that were scheduled to run before this event.
        next_scheduled_dt = self._scheduler.peek()
        while next_scheduled_dt and next_scheduled_dt <= event.when:
            await self._run_scheduler(next_scheduled_dt)
            next_scheduled_dt = self._scheduler.peek()

    async def _before_idle_stop(self):
        # Drain the job queue.
        next_scheduled_dt = self._scheduler.peek()
        while next_scheduled_dt:
            await self._run_scheduler(next_scheduled_dt)
            next_scheduled_dt = self._scheduler.peek()


class RealtimeDispatcher(EventDispatcher):
    def __init__(self):
        super().__init__(strict_order=False, stop_when_idle=False)
        self.scheduler_tick = 0.01

    async def run(self, stop_signals: List[int] = [signal.SIGINT, signal.SIGTERM]):
        await asyncio.gather(
            super().run(stop_signals=stop_signals),
            self._scheduler_service_loop()
        )

    async def _scheduler_service_loop(self):
        while not self.stopped:
            await asyncio.sleep(self.scheduler_tick)
            await self._run_scheduler(dt.utc_now())


async def await_no_raise(coro: Awaitable[Any], message: str = "Unhandled exception"):
    with helpers.no_raise(logger, message):
        await coro


def realtime_dispatcher() -> EventDispatcher:
    """Creates an event dispatcher suitable for live trading."""
    return RealtimeDispatcher()


def backtesting_dispatcher() -> EventDispatcher:
    """Creates an event dispatcher suitable for backtesting."""
    return BacktestingDispatcher()
