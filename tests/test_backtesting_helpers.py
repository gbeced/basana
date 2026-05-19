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

from basana.backtesting.helpers import ExchangeObjectContainer


class MockItem:
    def __init__(self, is_open: bool):
        self.id = "mock-id"
        self.is_open = is_open


def test_exchange_object_container_add_closed_item():
    container = ExchangeObjectContainer()
    item = MockItem(is_open=False)
    container.add(item)
    # A closed item should not appear in _open_items.
    assert item not in container._open_items
