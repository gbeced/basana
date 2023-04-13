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

from decimal import Decimal
import logging

from . import helpers
from basana.core import bar, event, websockets as core_ws
from basana.core.pair import Pair


logger = logging.getLogger(__name__)


class Bar(bar.Bar):
    def __init__(self, pair: Pair, json: dict):
        super().__init__(
            helpers.timestamp_to_datetime(int(json["t"])), pair, Decimal(json["o"]), Decimal(json["h"]),
            Decimal(json["l"]), Decimal(json["c"]), Decimal(json["v"])
        )
        self.pair: Pair = pair
        self.json: dict = json


# Generate BarEvents events from websocket messages.
class WebSocketEventSource(core_ws.ChannelEventSource):
    def __init__(self, pair: Pair, producer: event.Producer):
        super().__init__(producer=producer)
        self._pair: Pair = pair

    async def push_from_message(self, message: dict):
        kline_event = message["data"]
        kline = kline_event["k"]
        # Wait for the last update to the kline.
        if kline["x"] is False:
            return
        self.push(bar.BarEvent(
            helpers.timestamp_to_datetime(int(kline_event["E"])),
            Bar(self._pair, kline)
        ))


def get_channel(pair: Pair, interval: str) -> str:
    return "{}@kline_{}".format(helpers.pair_to_order_book_symbol(pair).lower(), interval)
