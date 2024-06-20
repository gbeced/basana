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

import pytest

from basana.core.config import get_config_value


CONFIG_VALUES = {
    "api": {
        "http": {
            "base_url": "https://www.bitstamp.net/",
        },
        "websockets": {
            "base_url": "wss://ws.bitstamp.net/",
        }
    }
}


@pytest.mark.parametrize("path, default, overrides, expected", [
    ("api.http", None, {}, {"base_url": "https://www.bitstamp.net/"}),
    ("api.http.base_url", "http://google.com/", {}, "https://www.bitstamp.net/"),
    ("api.websockets.timeout", 10, {}, 10),
    ("api.timeout", 10, {}, 10),
    ("api.timeout", 10, {"api": {"timeout": 50}}, 50),
])
def test_get_config_value(path, default, overrides, expected):
    assert get_config_value(CONFIG_VALUES, path, default=default, overrides=overrides) == expected
