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

import pytest

from samples.backtesting.position_manager import OrderInfo, PositionInfo
import basana as bs


btc_usdt_pair = bs.Pair("BTC", "USDT")
btc_usdt_pair_info = bs.PairInfo(8, 2)


def test_long_partially_filled():
    target = Decimal(10)
    order = OrderInfo(
        id="1234", operation=bs.OrderOperation.BUY, is_open=True, amount_filled=Decimal(0), fill_price=None,
    )
    pos_info = PositionInfo(
        pair=btc_usdt_pair, pair_info=btc_usdt_pair_info, initial=Decimal(0), initial_avg_price=Decimal(0),
        target=target, order=order
    )

    assert pos_info.order_open is True
    assert pos_info.target_reached is False
    assert pos_info.initial_avg_price == Decimal(0)
    assert pos_info.avg_price == Decimal(0)

    order.amount_filled = Decimal(7)
    order.fill_price = Decimal(10000)

    assert pos_info.order_open is True
    assert pos_info.target_reached is False
    assert pos_info.avg_price == Decimal(10000)

    order.is_open = False

    assert pos_info.order_open is False
    assert pos_info.target_reached is False
    assert pos_info.avg_price == Decimal(10000)


def test_short_completely_filled():
    target = Decimal(-10)
    order = OrderInfo(
        id="1234", operation=bs.OrderOperation.SELL, is_open=True, amount_filled=Decimal(0), fill_price=None,
    )
    pos_info = PositionInfo(
        pair=btc_usdt_pair, pair_info=btc_usdt_pair_info, initial=Decimal(0), initial_avg_price=Decimal(0),
        target=target, order=order
    )

    assert pos_info.order_open is True
    assert pos_info.target_reached is False
    assert pos_info.avg_price == Decimal(0)

    order.amount_filled = abs(target)
    order.fill_price = Decimal(10000)
    order.is_open = False

    assert pos_info.order_open is False
    assert pos_info.target_reached is True
    assert pos_info.avg_price == Decimal(10000)


@pytest.mark.parametrize("target_position", [
    bs.Position.LONG,
    bs.Position.SHORT,
])
def test_long_jump(target_position):
    assert target_position in [bs.Position.LONG, bs.Position.SHORT]

    sign = 1 if target_position == bs.Position.LONG else -1
    target = Decimal(10) * sign
    operation = {
        bs.Position.LONG: bs.OrderOperation.BUY,
        bs.Position.SHORT: bs.OrderOperation.SELL,
    }[target_position]
    reverse_operation = {
        bs.OrderOperation.BUY: bs.OrderOperation.SELL,
        bs.OrderOperation.SELL: bs.OrderOperation.BUY,
    }[operation]

    # First order that jumps from one position to the opposite one.
    order = OrderInfo(
        id="1", operation=operation, is_open=True, amount_filled=Decimal(0), fill_price=None,
    )
    pos_info = PositionInfo(
        pair=btc_usdt_pair, pair_info=btc_usdt_pair_info, initial=-target, initial_avg_price=Decimal(900),
        target=target, order=order
    )

    assert pos_info.order_open is True
    assert pos_info.target_reached is False

    # The order gets partially filled and gets canceled.
    order.amount_filled = Decimal(13)
    order.fill_price = Decimal(1000)
    order.is_open = False

    assert pos_info.order_open is False
    assert pos_info.target_reached is False
    assert pos_info.avg_price == Decimal(1000)

    # The last order was canceled and a new one will start at 3/-3.
    pos_info.initial, pos_info.initial_avg_price = pos_info.current, pos_info.avg_price
    order = OrderInfo(
        id="2", operation=operation, is_open=True, amount_filled=Decimal(0), fill_price=None,
    )
    pos_info.order = order

    assert pos_info.order_open is True
    assert pos_info.target_reached is False

    # The order gets partially filled and gets canceled.
    order.amount_filled = Decimal(5)
    order.fill_price = Decimal(1100)
    order.is_open = False

    assert pos_info.order_open is False
    assert pos_info.target_reached is False
    assert pos_info.avg_price == (3 * Decimal(1000) + 5 * Decimal(1100)) / Decimal(8)

    # We're at 8/-8, and the position is resized to 7/-7 so we need to step back 1.
    pos_info.initial, pos_info.initial_avg_price = pos_info.current, pos_info.avg_price
    pos_info.target = Decimal(7) * sign
    order = OrderInfo(
        id="3", operation=reverse_operation, is_open=True, amount_filled=Decimal(0), fill_price=None,
    )
    pos_info.order = order

    # The resize order gets completely filled.
    order.amount_filled = Decimal(1)
    order.fill_price = Decimal(100000)
    order.is_open = False

    assert pos_info.order_open is False
    assert pos_info.target_reached is True
    assert pos_info.avg_price == (3 * Decimal(1000) + 5 * Decimal(1100)) / Decimal(8)


@pytest.mark.parametrize(
    (
        "initial, initial_avg_price, target, order_operation, order_filled_amount, order_filled_price, "
        "expected_avg_price"
    ),
    [
        # Long
        (0, 0, 1, bs.OrderOperation.BUY, 0, 0, 0),
        (0, 0, 1, bs.OrderOperation.BUY, 1, "100.10", "100.10"),
        (1, 100, 2, bs.OrderOperation.BUY, 1, 100, 100),
        (1, 100, 2, bs.OrderOperation.BUY, 1, 200, 150),
        (2, 1000, 1, bs.OrderOperation.SELL, 1, 5000, 1000),
        # Short
        (0, 0, -1, bs.OrderOperation.SELL, 0, 0, 0),
        (0, 0, -1, bs.OrderOperation.SELL, 1, "100.10", "100.10"),
        (-1, 100, -2, bs.OrderOperation.SELL, 1, 100, 100),
        (-1, 100, -2, bs.OrderOperation.SELL, 1, 200, 150),
        (-2, 1000, -1, bs.OrderOperation.BUY, 1, 5000, 1000),
        # Regression
        (1, 1234, 0, bs.OrderOperation.SELL, 1, 55, 0),
    ]
)
def test_avg_price(
        initial, initial_avg_price, target, order_operation, order_filled_amount, order_filled_price, expected_avg_price
):
    initial, initial_avg_price, target, order_filled_amount, order_filled_price, expected_avg_price = [
        Decimal(value) for value in [
            initial, initial_avg_price, target, order_filled_amount, order_filled_price, expected_avg_price
        ]
    ]
    is_open = abs(target - initial) != order_filled_amount
    fill_price = None if order_filled_amount == 0 else order_filled_price

    order = OrderInfo(
        id="1", operation=order_operation, is_open=is_open, amount_filled=order_filled_amount, fill_price=fill_price,
    )
    pos_info = PositionInfo(
        pair=btc_usdt_pair, pair_info=btc_usdt_pair_info, initial=initial, initial_avg_price=initial_avg_price,
        target=target, order=order
    )
    assert pos_info.avg_price == expected_avg_price


def test_pnl_pct():
    order = OrderInfo(
        id="1", operation=bs.OrderOperation.BUY, is_open=False, amount_filled=Decimal(1), fill_price=Decimal(1000),
    )
    pos_info = PositionInfo(
        pair=btc_usdt_pair, pair_info=btc_usdt_pair_info, initial=Decimal(0), initial_avg_price=Decimal(0),
        target=Decimal(1), order=order
    )

    assert pos_info.avg_price == Decimal(1000)
    assert pos_info.calculate_unrealized_pnl_pct(Decimal(1000), Decimal(1000)) == 0
    assert pos_info.calculate_unrealized_pnl_pct(Decimal(1500), Decimal(9999)) == Decimal("50")
    assert pos_info.calculate_unrealized_pnl_pct(Decimal(700), Decimal(9999)) == Decimal("-30")
