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
        self._idle_handlers: List[IdleHandler] = []
        self._sniffers_pre: List[EventHandler] = []
        self._sniffers_post: List[EventHandler] = []
        self._producers: Set[event.Producer] = set()
        self._active_tasks: Optional[helpers.TaskGroup] = None
        self._running = False
        self._stopped = False
        self._scheduler = JobScheduler()
        self._event_mux = EventMultiplexer()
        self._event_task_pool = helpers.TaskPool(max_concurrent)

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
        self._event_task_pool.cancel()

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
            self._event_task_pool.cancel()
            await self._event_task_pool.wait_all()
            # No more cancelation at this point.
            self._active_tasks = None
            # Finalize producers.
            coros = [await_no_raise(producer.finalize()) for producer in self._producers]
            await asyncio.gather(*coros)

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
            await asyncio.gather(
                *[event_handler(event_dispatch.event) for event_handler in self._sniffers_pre],
                return_exceptions=True
            )
        if event_dispatch.handlers:
            await asyncio.gather(
                *[event_handler(event_dispatch.event) for event_handler in event_dispatch.handlers],
                return_exceptions=True
            )
        if self._sniffers_post:
            await asyncio.gather(
                *[event_handler(event_dispatch.event) for event_handler in self._sniffers_post],
                return_exceptions=True
            )

    async def _run_scheduler(self, now: datetime.datetime):
        await self._scheduler.run(now)


class BacktestingDispatcher(EventDispatcher):
    def __init__(self, max_concurrent: int):
        super().__init__(max_concurrent=max_concurrent)
        self.subscribe_all(self._before_event, front_run=True)
        self._last_event_dt: Optional[datetime.datetime] = None

    async def run(self, stop_signals: List[int] = [signal.SIGINT, signal.SIGTERM]):
        with logs.backtesting_log_mode(self):
            await super().run(stop_signals=stop_signals)

    def now(self) -> datetime.datetime:
        ret = self._last_event_dt
        if ret is None:
            ret = dt.utc_now()
        return ret

    async def _dispatch_loop(self):
        while not self._stopped:
            next_dt = self._event_mux.peek_next_event_dt()
            if next_dt:
                # Check that events are processed in ascending order.
                assert self._last_event_dt is None or next_dt >= self._last_event_dt, \
                    f"{next_dt} can't be dispatched after {self._last_event_dt}"

                # Push events to dispatch and wait those to finish executing.
                self._last_event_dt = next_dt
                for source, evnt in self._event_mux.pop_while(next_dt):
                    await self._event_task_pool.push(
                        self._dispatch_event(EventDispatch(event=evnt, handlers=self._event_handlers.get(source)))
                    )
                await self._event_task_pool.wait_all()
            else:
                await self._run_pending_scheduled()
                self.stop()

    async def _before_event(self, event: event.Event):
        # Execute jobs that were scheduled to run before this event.
        next_scheduled_dt = self._scheduler.peek()
        while next_scheduled_dt and next_scheduled_dt <= event.when:
            await self._run_scheduler(next_scheduled_dt)
            next_scheduled_dt = self._scheduler.peek()

    async def _run_pending_scheduled(self):
        # Drain the job queue.
        next_scheduled_dt = self._scheduler.peek()
        while next_scheduled_dt:
            await self._run_scheduler(next_scheduled_dt)
            next_scheduled_dt = self._scheduler.peek()


class RealtimeDispatcher(EventDispatcher):
    def __init__(self, max_concurrent: int):
        super().__init__(max_concurrent=max_concurrent)
        self._prev_events: Dict[event.EventSource, datetime.datetime] = {}
        self.scheduler_sleep = 0.1
        self.idle_sleep = 0.01
        self._wait_all_timeout: Optional[float] = 0.01

    async def run(self, stop_signals: List[int] = [signal.SIGINT, signal.SIGTERM]):
        await asyncio.gather(
            super().run(stop_signals=stop_signals),
            self._scheduler_service_loop()
        )

    def now(self) -> datetime.datetime:
        return dt.utc_now()

    async def _dispatch_loop(self):
        while not self._stopped:
            # Pop events and feed the pool.
            for source, evnt in self._event_mux.pop_while(dt.utc_now()):
                # Check that events from the same source are returned in order.
                prev_event = self._prev_events.get(source)
                if prev_event is not None and evnt.when < prev_event.when:
                    self.on_error(logs.StructuredMessage(
                        "Events returned out of order", source=type(source), previous=prev_event.when, current=evnt.when
                    ))
                    # TODO: Not ignoring out-of-order events should be an option.
                    continue
                self._prev_events[source] = evnt

                # Push event into the pool for processing.
                await self._event_task_pool.push(
                    self._dispatch_event(EventDispatch(event=evnt, handlers=self._event_handlers.get(source)))
                )

            idle = await self._event_task_pool.wait_all(timeout=self._wait_all_timeout)
            if idle:
                if self._idle_handlers:
                    # TODO: Should put some throttling here. Maybe tokenbucket to limit idle calls to no x / s ?
                    await asyncio.gather(
                        *[event_handler() for event_handler in self._idle_handlers], return_exceptions=True
                    )
                else:
                    # Otherwise we'll monopolize the event loop.
                    await asyncio.sleep(self.idle_sleep)

    async def _scheduler_service_loop(self):
        while not self.stopped:
            await asyncio.sleep(self.scheduler_sleep)
            await self._run_scheduler(dt.utc_now())


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
