# Basana
#
# Copyright 2026 Christian Pojoni
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

import datetime

from basana.core.pair import Pair


def timestamp_to_datetime(timestamp_ms: int) -> datetime.datetime:
    """Convert a Hyperliquid millisecond timestamp to a timezone-aware datetime."""
    return datetime.datetime.fromtimestamp(timestamp_ms / 1e3, tz=datetime.timezone.utc)


def pair_to_coin(pair: Pair) -> str:
    """Return the Hyperliquid coin name for a trading pair."""
    return pair.base_symbol.upper()


def coin_to_pair(coin: str) -> Pair:
    """Return a Basana Pair for a Hyperliquid coin name."""
    return Pair(coin.upper(), "USD")
