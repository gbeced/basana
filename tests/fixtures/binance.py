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

import aioresponses
import pytest

from basana.core import token_bucket
from basana.external.binance import exchange


@pytest.fixture()
def binance_http_api_mock():
    with aioresponses.aioresponses(passthrough_unmatched=True) as m:
        yield m


@pytest.fixture()
def binance_exchange(realtime_dispatcher):
    return exchange.Exchange(
        realtime_dispatcher, "api_key", "api_secret", tb=token_bucket.TokenBucketLimiter(10, 1, 0),
        config_overrides={"api": {"http": {"base_url": "http://binance.mock/"}}}
    )
