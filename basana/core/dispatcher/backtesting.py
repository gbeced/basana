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

from typing import List, Optional
import datetime
import functools
import signal

from .. import errors, logs
from . import base


class BacktestingDispatcher(base.EventDispatcher):
    """
    Event dispatcher for backtesting.

    :param max_concurrent: The maximum number of events to process concurrently.
    """

    def __init__(self, max_concurrent: int):
        super().__init__(max_concurrent=max_concurrent)
        self._last_dt: Optional[datetime.datetime] = None

    async def run(self, stop_signals: List[int] = [signal.SIGINT, signal.SIGTERM]):
        with logs.backtesting_log_mode(self):
            await super().run(stop_signals=stop_signals)

    @property
    def now_available(self) -> bool:
        return self._last_dt is not None

    def now(self) -> datetime.datetime:
        if self._last_dt is None:
            raise errors.Error("Can't calculate current datetime since no events were processed")
        return self._last_dt

    def _set_now(self, now: datetime.datetime):
        # For testing purposes.
        assert self._last_dt is None or now >= self._last_dt
        self._last_dt = now

    async def _dispatch_loop(self):
        self._last_dt = self._event_mux.peek_next_event_dt()
        await self._notify_loop_started()

        while not self.stopped:
            # Time is driven by event timestamps, so "now" advances only when events are processed.
            next_dt = self._event_mux.peek_next_event_dt()
            if next_dt:
                # Check that events are processed in ascending order.
                assert self._last_dt is None or next_dt >= self._last_dt, \
                    f"{next_dt} can't be dispatched after {self._last_dt}"

                if self._scheduler_queue:
                    await self._dispatch_scheduled(next_dt)
                await self._dispatch_events(next_dt)
            else:
                # No more events. Dispatch all pending scheduled jobs before stopping.
                if last_scheduled_dt := self._scheduler_queue.peek_last_event_dt():
                    await self._dispatch_scheduled(last_scheduled_dt)
                self.stop()

    async def _dispatch_scheduled(self, dt: datetime.datetime):
        # Execute jobs that were scheduled to run before dt.
        next_scheduled_dt = self._scheduler_queue.peek_next_event_dt()
        while next_scheduled_dt and next_scheduled_dt <= dt and not self.stopped:
            # If self._last_dt is already set in the future, don't move it backwards in time.
            next_scheduled_dt, job = self._scheduler_queue.pop()
            if self._last_dt is None or next_scheduled_dt > self._last_dt:
                self._last_dt = next_scheduled_dt

            # Scheduled jobs are intentionally processed one at a time in backtesting mode to prevent executing distant
            # jobs at the same time.
            await self._execute_scheduled(next_scheduled_dt, job)

            next_scheduled_dt = self._scheduler_queue.peek_next_event_dt()

    async def _dispatch_events(self, dt: datetime.datetime):
        self._last_dt = dt
        event_dispatches = [
            (event, self._event_handlers[source])
            for source, event in self._event_mux.pop_while(dt)
        ]
        match len(event_dispatches):
            case 1:
                await self._dispatch_event(*event_dispatches[0])
            case _:
                for event_dispatch in event_dispatches:
                    await self._concurrency.push_pool(functools.partial(self._dispatch_event, *event_dispatch))
                # In backtesting mode we don't move forward with future events.
                await self._concurrency.wait_pool()
