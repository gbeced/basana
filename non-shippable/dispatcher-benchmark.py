from typing import Optional
import asyncio

import basana as bs


class EventSource(bs.EventSource):
    def __init__(self, event_count: int):
        super().__init__()
        self._event_count = event_count

    def pop(self) -> Optional[bs.Event]:
        if self._event_count:
            self._event_count -= 1
            return bs.Event(bs.utc_now())
        return None


async def event_hander(event: bs.Event):
    # print(event)
    pass


async def run_dispatcher(event_counts: list):
    dispatcher = bs.backtesting_dispatcher()
    for count in event_counts:
        source = EventSource(count)
        dispatcher.subscribe(source, event_hander)
    await dispatcher.run()


async def main():
    # await run_dispatcher([1e6])
    await run_dispatcher([5e5, 5e5])


if __name__ == "__main__":
    asyncio.run(main())
