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

from basana.core import pair


def test_str():
    assert str(pair.Pair("BTC", "USD")) == "BTC/USD"


def test_eq():
    assert pair.Pair("BTC", "USD") == pair.Pair("BTC", "USD")
    assert pair.Pair("BTC", "USD") != pair.Pair("ARS", "USD")
