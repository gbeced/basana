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

from decimal import Decimal

from samples.binance.order_book_mirror import OrderBook
import pytest


@pytest.mark.parametrize("updates, expected_bids, expected_asks", [
    ([], [], []),
    (
        [
            (102000, 1, True),
            (102000, "0.005", True),
            (101900, 1, True),
            (103000, 1, True),
            (103000, 0, True),
            (103000, 7, True),

            (103900, 2, False),
            (104000, 1, False),
            (103500, 4, False),
        ],
        [
            (103000, 7),
            (102000, "0.005"),
            (101900, 1),
        ],
        [
            (103500, 4),
            (103900, 2),
            (104000, 1),
        ],
    ),
])
def test_order_book(updates, expected_bids, expected_asks):
    order_book = OrderBook()
    for price, amount, is_bid in updates:
        order_book.update(Decimal(price), Decimal(amount), is_bid)
    assert order_book.bids == [(Decimal(price), Decimal(amount)) for price, amount in expected_bids]
    assert order_book.asks == [(Decimal(price), Decimal(amount)) for price, amount in expected_asks]
