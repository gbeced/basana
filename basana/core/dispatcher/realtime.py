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

from typing import Any, Awaitable, Callable, Dict, List
import asyncio
import datetime
import functools

from . import base
from .. import dt, event as core_event, logs


IdleHandler = Callable[[], Awaitable[Any]]


class RealtimeDispatcher(base.EventDispatcher):
    """Event dispatcher for live trading.

    :param max_concurrent: The maximum number of events to process concurrently.
    """

    def __init__(self, max_concurrent: int):
        super().__init__(max_concurrent=max_concurrent)
        self._prev_event_dt: Dict[core_event.EventSource, datetime.datetime] = {}
        self.idle_sleep: float = 0.001
        self._wait_all_timeout: float = 0   # TODO: Will be removed in a future version.
        self._idle_handlers: List[IdleHandler] = []

    def now(self) -> datetime.datetime:
        return dt.utc_now()

    def subscribe_idle(self, idle_handler: IdleHandler):
        """
        Registers an async callable that will be called when there are no events to dispatch.

        :param idle_handler: An async callable that receives no arguments.
        """

        assert not self._running, "Subscribing once we're running is not currently supported."

        if idle_handler not in self._idle_handlers:
            self._idle_handlers.append(idle_handler)

    async def _dispatch_loop(self):
        await self._notify_loop_started()
        while not self.stopped:
            now = dt.utc_now()
            # Feed the task pool with scheduled jobs and events that are ready for processing.
            await asyncio.gather(
                self._push_scheduled(now),
                self._push_events(now),
            )
            # Optionally give some time for handlers to execute before pushing new ones.
            # This is disabled by default and it will be deprecated.
            if self._wait_all_timeout:  # pragma: no cover
                await self._concurrency.wait_pool(timeout=self._wait_all_timeout)

            if self._concurrency.pool_idle:
                await self._on_idle()
            else:
                # Yield to the event loop to allow other tasks to run.
                await asyncio.sleep(0)

    async def _on_idle(self):
        if self._idle_handlers:
            await base.gather_no_raise(*[
                self._concurrency.push_pool(idle_handler) for idle_handler in self._idle_handlers
            ])

        # Avoid trashing the CPU if there's nothing to do.
        await asyncio.sleep(self.idle_sleep)

    async def _push_scheduled(self, dt: datetime.datetime):
        while (next_scheduled_dt := self._scheduler_queue.peek_next_event_dt()) and next_scheduled_dt <= dt:
            next_scheduled_dt, job = self._scheduler_queue.pop()
            # Push scheduled job into the task pool for processing.
            await self._concurrency.push_pool(functools.partial(self._execute_scheduled, next_scheduled_dt, job))

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
            await self._concurrency.push_pool(
                functools.partial(self._dispatch_event, evnt, self._event_handlers[source])
            )
