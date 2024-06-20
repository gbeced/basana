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

from basana.core import enums


def test_order_operation():
    assert str(enums.OrderOperation.BUY) == "buy"
    assert str(enums.OrderOperation.SELL) == "sell"


def test_position():
    assert str(enums.Position.LONG) == "long"
    assert str(enums.Position.NEUTRAL) == "neutral"
    assert str(enums.Position.SHORT) == "short"
