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

DEFAULTS = {
    "api": {
        "http": {
            "base_url": "https://api.binance.com/",
            "timeout": 30,
        },
        "websockets": {
            "base_url": "wss://stream.binance.com/",
            "heartbeat": 30,
            "spot": {
                "user_data_stream": {
                    "heartbeat": 15 * 60,
                },
            },
            "cross_margin": {
                "user_data_stream": {
                    "heartbeat": 15 * 60,
                },
            },
            "isolated_margin": {
                "user_data_stream": {
                    "heartbeat": 15 * 60,
                },
            },
        }
    }
}
