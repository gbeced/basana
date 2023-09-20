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

from typing import cast, Any, Awaitable, Callable, Dict, Generator, List, Optional, Set, Tuple
import abc
import asyncio
import dataclasses
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


@dataclasses.dataclass
class EventDispatch:
    event: event.Event
    handlers: List[EventHandler]


@dataclasses.dataclass(order=True)
class ScheduledJob:
    when: datetime.datetime
    job: SchedulerJob = dataclasses.field(compare=False)  # Comparing function objects fails on Win32


class SchedulerQueue:
    def __init__(self):
        self._queue = []

    def push(self, when: datetime.datetime, job: SchedulerJob):
        assert not dt.is_naive(when), f"{when} should have timezone information set"
        heapq.heappush(self._queue, ScheduledJob(when=when, job=job))

    def peek_next_event_dt(self) -> Optional[datetime.datetime]:
        ret = None
        if self._queue:
            ret = self._queue[0].when
        return ret

    def pop(self) -> Tuple[datetime.datetime, SchedulerJob]:
        assert self._queue
        scheduled_job = heapq.heappop(self._queue)
        return scheduled_job.when, scheduled_job.job


class EventMultiplexer:
    def __init__(self) -> None:
        self._prefetched_events: Dict[event.EventSource, Optional[event.Event]] = {}

    def add(self, source: event.EventSource):
        self._prefetched_events.setdefault(source)

    def peek_next_event_dt(self) -> Optional[datetime.datetime]:
        self._prefetch()

        next_dt = None
        prefetched_events = [evnt for evnt in self._prefetched_events.values() if evnt]
        if prefetched_events:
            next_dt = min(map(lambda evnt: evnt.when, prefetched_events))
        return next_dt

    def pop(self, max_dt: datetime.datetime) -> Tuple[Optional[event.EventSource], Optional[event.Event]]:
        ret_source: Optional[event.EventSource] = None
        ret_event: Optional[event.Event] = None

        # Find the next event to return, this is, the oldest one that is <= max_dt.
        for source, evnt in self._prefetched_events.items():
            # Prefetch the event for sorting purposes.
            if evnt is None:
                evnt = source.pop()
                self._prefetched_events[source] = evnt
            # If the event matches the filter, check if the next one to return.
            if evnt and evnt.when <= max_dt and (ret_event is None or evnt.when < ret_event.when):
                ret_source = source
                ret_event = evnt

        # Consume the event.
        if ret_source:
            self._prefetched_events[ret_source] = None

        return (ret_source, ret_event)

    def pop_while(self, max_dt: datetime.datetime) -> Generator[Tuple[event.EventSource, event.Event], None, None]:
        while None not in (src_and_event := self.pop(max_dt)):
            yield (cast(event.EventSource, src_and_event[0]), cast(event.Event, src_and_event[1]))

    def _prefetch(self):
        sources_to_pop = [
            source for source, event in self._prefetched_events.items() if event is None
        ]
        for source in sources_to_pop:
            if event := source.pop():
                self._prefetched_events[source] = event


class EventDispatcher(metaclass=abc.ABCMeta):
    """Responsible for connecting event sources to event handlers and dispatching events in the right order.

    :param max_concurrent: The maximum number of events to process concurrently.

    .. note::

        The following helper functions are provided to build event dispatchers suitable for backtesting or for live
        trading:

        * :func:`basana.backtesting_dispatcher`
        * :func:`basana.realtime_dispatcher`
    """

    def __init__(self, max_concurrent: int):
        self._event_handlers: Dict[event.EventSource, List[EventHandler]] = {}
        self._sniffers_pre: List[EventHandler] = []
        self._sniffers_post: List[EventHandler] = []
        self._producers: Set[event.Producer] = set()
        self._active_tasks: Optional[helpers.TaskGroup] = None
        self._running = False
        self._stopped = False
        self._scheduler_queue = SchedulerQueue()
        self._event_mux = EventMultiplexer()
        self._task_pool = helpers.TaskPool(max_concurrent)

    @property
    def current_event_dt(self) -> Optional[datetime.datetime]:
        helpers.deprecation("Use now() instead")
        return self.now()

    @abc.abstractmethod
    def now(self) -> datetime.datetime:
        raise NotImplementedError()

    @property
    def stopped(self) -> bool:
        """Returns True if stop was called, False otherwise."""
        return self._stopped

    def stop(self):
        """Requests the event dispatcher to stop the event processing loop."""
        logger.debug("Stop requested")
        self._stopped = True
        if self._active_tasks:
            self._active_tasks.cancel()
        self._task_pool.cancel()

    def subscribe(self, source: event.EventSource, event_handler: EventHandler):
        """Registers an async callable that will be called when an event source has new events.

        :param source: An event source.
        :param event_handler: An async callable that receives an event.
        """

        assert not self._running, "Subscribing once we're running is not currently supported."

        self._event_mux.add(source)
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
        """Schedules a function to be executed at a given time.

        :param when: The datetime when the function should be execution.
        :param job: The function to execute.
        """
        self._scheduler_queue.push(when, job)

    async def run(self, stop_signals: List[int] = [signal.SIGINT, signal.SIGTERM]):
        """Executes the event dispatch loop.

        :param stop_signals: The signals that will be handled to request :func:`run()` to :func:`stop()`.

        This method will execute the following steps in order:

        #. Call :meth:`basana.Producer.initialize` on all producers.
        #. Call :meth:`basana.Producer.main` on all producers and execute event dispatch loop until stopped.
        #. Call :meth:`basana.Producer.finalize` on all producers.
        """

        assert not self._running, "Can't run twice."
        assert self._active_tasks is None

        # This block has coverage on all platforms except on Windows.
        if platform.system() != "Windows":  # pragma: no cover
            for stop_signal in stop_signals:
                asyncio.get_event_loop().add_signal_handler(stop_signal, self.stop)

        self._running = True
        try:
            # Initialize producers.
            async with helpers.TaskGroup() as tg:
                self._active_tasks = tg  # So it can be canceled.
                for producer in self._producers:
                    tg.create_task(producer.initialize())
            # Run producers and dispatch loop.
            async with helpers.TaskGroup() as tg:
                self._active_tasks = tg  # So it can be canceled.
                for producer in self._producers:
                    tg.create_task(producer.main())
                tg.create_task(self._dispatch_loop())
        except asyncio.CancelledError:
            pass
        finally:
            # Cancel any pending task in the event pool.
            self._task_pool.cancel()
            await self._task_pool.wait_all()
            # No more cancelation at this point.
            self._active_tasks = None
            # Finalize producers.
            await gather_no_raise(*[producer.finalize() for producer in self._producers])

    def on_error(self, error: Any):
        logger.error(error)

    @abc.abstractmethod
    async def _dispatch_loop(self):
        raise NotImplementedError()

    async def _dispatch_event(self, event_dispatch: EventDispatch):
        logger.debug(logs.StructuredMessage(
            "Dispatching event", when=event_dispatch.event.when, type=type(event_dispatch.event)
        ))
        if self._sniffers_pre:
            await gather_no_raise(
                *[event_handler(event_dispatch.event) for event_handler in self._sniffers_pre]
            )
        if event_dispatch.handlers:
            await gather_no_raise(
                *[event_handler(event_dispatch.event) for event_handler in event_dispatch.handlers]
            )
        if self._sniffers_post:
            await gather_no_raise(
                *[event_handler(event_dispatch.event) for event_handler in self._sniffers_post]
            )

    async def _execute_scheduled(self, dt: datetime.datetime, job: SchedulerJob):
        logger.debug(logs.StructuredMessage("Executing scheduled job", scheduled=dt))
        await await_no_raise(job(), message="Unhandled exception executing scheduled job")


class BacktestingDispatcher(EventDispatcher):
    """Event dispatcher for backtesting.

    :param max_concurrent: The maximum number of events to process concurrently.
    """

    def __init__(self, max_concurrent: int):
        super().__init__(max_concurrent=max_concurrent)
        self._last_dt: Optional[datetime.datetime] = None

    async def run(self, stop_signals: List[int] = [signal.SIGINT, signal.SIGTERM]):
        with logs.backtesting_log_mode(self):
            await super().run(stop_signals=stop_signals)

    def now(self) -> datetime.datetime:
        ret = self._last_dt
        if ret is None:
            ret = dt.utc_now()
        return ret

    async def _dispatch_loop(self):
        while not self.stopped:
            next_dt = self._event_mux.peek_next_event_dt()
            if next_dt:
                # Check that events are processed in ascending order.
                assert self._last_dt is None or next_dt >= self._last_dt, \
                    f"{next_dt} can't be dispatched after {self._last_dt}"

                await self._dispatch_scheduled(next_dt)
                await self._dispatch_events(next_dt)
            else:
                await self._dispatch_scheduled(dt.utc_now())  # Dispatch all past events.
                self.stop()

    async def _dispatch_scheduled(self, dt: datetime.datetime):
        # Execute jobs that were scheduled to run before dt.
        next_scheduled_dt = self._scheduler_queue.peek_next_event_dt()
        while next_scheduled_dt and next_scheduled_dt <= dt:
            # If self._last_dt is already set in the future, don't move it backwards in time.
            next_scheduled_dt, job = self._scheduler_queue.pop()
            if self._last_dt is None or next_scheduled_dt > self._last_dt:
                self._last_dt = next_scheduled_dt

            await self._task_pool.push(self._execute_scheduled(next_scheduled_dt, job))
            # Waiting here and not outside of the loop to prevent executing distant scheduled jobs at the same time.
            await self._task_pool.wait_all()

            next_scheduled_dt = self._scheduler_queue.peek_next_event_dt()

    async def _dispatch_events(self, dt: datetime.datetime):
        # Pop events, push them into the task pool, and wait those to finish executing.
        self._last_dt = dt
        for source, evnt in self._event_mux.pop_while(dt):
            await self._task_pool.push(
                self._dispatch_event(EventDispatch(event=evnt, handlers=self._event_handlers.get(source, [])))
            )
        await self._task_pool.wait_all()


class RealtimeDispatcher(EventDispatcher):
    """Event dispatcher for live trading.

    :param max_concurrent: The maximum number of events to process concurrently.
    """

    def __init__(self, max_concurrent: int):
        super().__init__(max_concurrent=max_concurrent)
        self._prev_event_dt: Dict[event.EventSource, datetime.datetime] = {}
        self.idle_sleep = 0.01
        self._wait_all_timeout: Optional[float] = 0.01
        self._idle_handlers: List[IdleHandler] = []

    def now(self) -> datetime.datetime:
        return dt.utc_now()

    def subscribe_idle(self, idle_handler: IdleHandler):
        """Registers an async callable that will be called when there are no events to dispatch.

        :param idle_handler: An async callable that receives no arguments.
        """

        assert not self._running, "Subscribing once we're running is not currently supported."

        if idle_handler not in self._idle_handlers:
            self._idle_handlers.append(idle_handler)

    async def _dispatch_loop(self):
        while not self.stopped:
            now = dt.utc_now()
            # Feed the task pool with scheduled jobs and events that are ready for processing.
            await asyncio.gather(
                self._push_scheduled(now),
                self._push_events(now),
            )
            # Give some time for tasks to execute, and keep on pushing tasks.
            idle = await self._task_pool.wait_all(timeout=self._wait_all_timeout)
            if idle:
                await self._on_idle()

    async def _on_idle(self):
        if self._idle_handlers:
            await gather_no_raise(*[
                self._task_pool.push(idle_handler()) for idle_handler in self._idle_handlers
            ])
        else:
            # Otherwise we'll monopolize the event loop.
            await asyncio.sleep(self.idle_sleep)

    async def _push_scheduled(self, dt: datetime.datetime):
        while (next_scheduled_dt := self._scheduler_queue.peek_next_event_dt()) and next_scheduled_dt <= dt:
            next_scheduled_dt, job = self._scheduler_queue.pop()
            # Push scheduled job into the task pool for processing.
            await self._task_pool.push(self._execute_scheduled(next_scheduled_dt, job))

    async def _push_events(self, dt: datetime.datetime):
        # Pop events and feed the pool.
        for source, evnt in self._event_mux.pop_while(dt):
            # Check that events from the same source are returned in order.
            prev_event_dt = self._prev_event_dt.get(source)
            if prev_event_dt is not None and evnt.when < prev_event_dt:
                self.on_error(logs.StructuredMessage(
                    "Events returned out of order", source=type(source), previous=prev_event_dt, current=evnt.when
                ))
                # TODO: Not ignoring out-of-order events should be an option.
                continue
            self._prev_event_dt[source] = evnt.when

            # Push event into the task pool for processing.
            await self._task_pool.push(
                self._dispatch_event(EventDispatch(
                    event=evnt,
                    handlers=self._event_handlers.get(source, [])
                ))
            )


async def gather_no_raise(*awaitables):
    await asyncio.gather(*[await_no_raise(awaitable) for awaitable in awaitables])


async def await_no_raise(coro: Awaitable[Any], message: str = "Unhandled exception"):
    with helpers.no_raise(logger, message):
        await coro


def realtime_dispatcher(max_concurrent: int = 50) -> EventDispatcher:
    """Creates an event dispatcher suitable for live trading.

    :param max_concurrent: The maximum number of events to process concurrently.
    """
    return RealtimeDispatcher(max_concurrent=max_concurrent)


def backtesting_dispatcher(max_concurrent: int = 50) -> EventDispatcher:
    """Creates an event dispatcher suitable for backtesting.

    :param max_concurrent: The maximum number of events to process concurrently.
    """
    return BacktestingDispatcher(max_concurrent=max_concurrent)
