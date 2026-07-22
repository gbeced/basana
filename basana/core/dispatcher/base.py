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

from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict, Generator, List, Optional, Set, Tuple
import abc
import asyncio
import contextlib
import datetime
import functools
import heapq
import logging
import platform
import signal

from .. import event as core_event, helpers, logs
from .concurrency import BoundedConcurrency, NoConcurrency
from .scheduler import SchedulerJob, SchedulerQueue


logger = logging.getLogger(__name__)
EventHandler = Callable[[core_event.Event], Awaitable[Any]]
LoopStartedHandler = Callable[[], Awaitable[Any]]


# Indices for prefetched event tuples stored in the EventMultiplexer heap.
# Using tuples instead of a dataclass avoids per-event __init__ and __lt__ overhead.
_PE_WHEN = 0
_PE_PRIORITY = 1
_PE_SOURCE = 3
_PE_EVENT = 4


class EventMultiplexer:
    """
    A multiplexer that manages multiple event sources and provides methods to retrieve events in chronological order.
    """
    def __init__(self) -> None:
        self._prefetched_events: Dict[core_event.EventSource, Optional[core_event.Event]] = {}
        self._event_heap: List[tuple] = []
        self._sources_to_prefetch: Set[core_event.EventSource] = set()

    def add(self, source: core_event.EventSource):
        if source not in self._prefetched_events:
            self._prefetched_events[source] = None
            self._sources_to_prefetch.add(source)

    def peek_next_event_dt(self) -> Optional[datetime.datetime]:
        self._prefetch()
        return self._event_heap[0][_PE_WHEN] if self._event_heap else None

    def pop(self, max_dt: datetime.datetime) -> Tuple[Optional[core_event.EventSource], Optional[core_event.Event]]:
        self._prefetch()
        if not self._event_heap or self._event_heap[0][_PE_WHEN] > max_dt:
            return (None, None)

        entry = heapq.heappop(self._event_heap)
        source = entry[_PE_SOURCE]
        self._prefetched_events[source] = None
        self._sources_to_prefetch.add(source)
        return (source, entry[_PE_EVENT])

    def pop_while(
            self, max_dt: datetime.datetime
    ) -> Generator[Tuple[core_event.EventSource, core_event.Event], None, None]:
        while True:
            source, event = self.pop(max_dt)
            if source is None:
                return
            yield (source, event)  # type: ignore[misc]

    def _prefetch(self):
        if not self._sources_to_prefetch:
            return
        done = []
        for source in self._sources_to_prefetch:
            if event := source.pop():
                self._prefetched_events[source] = event
                heapq.heappush(self._event_heap, (event.when, -source.priority, id(source), source, event))
                done.append(source)
        self._sources_to_prefetch.difference_update(done)


class EventDispatcher(metaclass=abc.ABCMeta):
    """
    Responsible for connecting event sources to event handlers and dispatching events in the right order.

    :param max_concurrent: The maximum number of events to process concurrently.

    .. note::

        The following helper functions are provided to build event dispatchers suitable for backtesting or for live
        trading:

        * :func:`basana.backtesting_dispatcher`
        * :func:`basana.realtime_dispatcher`
    """

    def __init__(self, max_concurrent: int):
        assert max_concurrent > 0

        self._event_handlers: Dict[core_event.EventSource, List[EventHandler]] = defaultdict(list)
        self._sniffers_pre: List[EventHandler] = []
        self._sniffers_post: List[EventHandler] = []
        self._loop_started_handlers: List[LoopStartedHandler] = []
        self._producers: Set[core_event.Producer] = set()
        # Task group for core tasks like producers and dispatch loop.
        self._core_tasks: Optional[helpers.TaskGroup] = None
        self._running = False
        self._stopped = False
        self._scheduler_queue = SchedulerQueue()
        self._event_mux = EventMultiplexer()
        # Set to True for the dispatcher to stop if a handler raises an exception.
        self.stop_on_handler_exceptions = False
        self._debug_enabled = False
        # Handles concurrency related to event and scheduler handlers.
        self._concurrency = NoConcurrency() if max_concurrent == 1 \
            else BoundedConcurrency(max_concurrency=max_concurrent)

    @property
    def current_event_dt(self) -> Optional[datetime.datetime]:
        helpers.deprecation_warning("Use now() instead")
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
        if self._core_tasks:
            self._core_tasks.cancel()
        self._concurrency.cancel_pool()

    def subscribe(self, source: core_event.EventSource, event_handler: EventHandler):
        """Registers an async callable that will be called when an event source has new events.

        :param source: An event source.
        :param event_handler: An async callable that receives an event.
        """

        assert not self._running, "Subscribing once we're running is not currently supported."

        self._event_mux.add(source)
        handlers = self._event_handlers[source]
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

    def subscribe_event_loop_started(self, handler: LoopStartedHandler):
        """Registers an async callable that will be called once when the dispatch loop starts.

        :param handler: An async callable that receives no arguments.
        """

        assert not self._running, "Subscribing once we're running is not currently supported."

        if handler not in self._loop_started_handlers:
            self._loop_started_handlers.append(handler)

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
        assert self._core_tasks is None

        self._debug_enabled = logger.isEnabledFor(logging.DEBUG)

        # This block has coverage on all platforms except on Windows.
        if platform.system() != "Windows":  # pragma: no cover
            for stop_signal in stop_signals:
                asyncio.get_event_loop().add_signal_handler(stop_signal, self.stop)

        self._running = True
        try:
            # Initialize producers.
            async with self._core_task_group() as tg:
                for producer in self._producers:
                    tg.create_task(producer.initialize())
            # Run producers and dispatch loop.
            async with self._core_task_group() as tg:
                for producer in self._producers:
                    tg.create_task(producer.main())
                tg.create_task(self._dispatch_loop())
        except asyncio.CancelledError:
            if not self.stopped:
                raise
        finally:
            # Cancel any pending task in the event handlers pool.
            self._concurrency.cancel_pool()
            await self._concurrency.wait_pool()
            # No more cancelation at this point.
            self._core_tasks = None
            # Finalize producers.
            await gather_no_raise(*[producer.finalize() for producer in self._producers])

    def on_error(self, error: Any):
        logger.error(error)

    @abc.abstractmethod
    async def _dispatch_loop(self):
        raise NotImplementedError()

    @contextlib.asynccontextmanager
    async def _core_task_group(self):
        try:
            async with helpers.TaskGroup() as tg:
                self._core_tasks = tg  # So it can be canceled.
                yield tg
        finally:
            self._core_tasks = None

    async def _notify_loop_started(self):
        if self._loop_started_handlers:
            await gather_no_raise(*[handler() for handler in self._loop_started_handlers])

    async def _dispatch_event(self, event: core_event.Event, handlers: List[EventHandler]):
        if self._debug_enabled:
            logger.debug(logs.StructuredMessage(
                "Dispatching event", when=event.when, type=helpers.classpath(event)
            ))
        if self._sniffers_pre:
            await self._call_event_handlers(event, self._sniffers_pre)
        await self._call_event_handlers(event, handlers)
        if self._sniffers_post:
            await self._call_event_handlers(event, self._sniffers_post)

    async def _call_event_handlers(self, evnt: core_event.Event, handlers: List[EventHandler]):
        match len(handlers):
            case 1:
                await self._call_event_handler(evnt, handlers[0])
            case _:
                await self._concurrency.gather([
                    functools.partial(self._call_event_handler, evnt, handler) for handler in handlers
                ])

    async def _call_event_handler(self, event: core_event.Event, handler: EventHandler):
        try:
            return await handler(event)
        except Exception as e:
            logger.exception(logs.StructuredMessage(
                "Unhandled exception in event handler", error=e, event=dict(type=type(event), when=event.when),
                handler=handler
            ))
            if self.stop_on_handler_exceptions:
                self.stop()

    async def _execute_scheduled(self, dt: datetime.datetime, job: SchedulerJob):
        if self._debug_enabled:
            logger.debug(logs.StructuredMessage("Executing scheduled job", scheduled=dt))

        try:
            await job()
        except Exception as e:
            logger.exception(logs.StructuredMessage(
                "Unhandled exception executing scheduled job", error=e, dt=dt, scheduler_job=job
            ))
            if self.stop_on_handler_exceptions:
                self.stop()


async def gather_no_raise(*awaitables):
    await asyncio.gather(*[await_no_raise(awaitable) for awaitable in awaitables])


async def await_no_raise(coro: Awaitable[Any], message: str = "Unhandled exception"):
    with helpers.no_raise(logger, message):
        await coro
