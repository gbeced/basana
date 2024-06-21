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

from basana.backtesting import exchange
from samples.backtesting.position_manager import PositionInfo
import basana as bs


def test_long_partially_filled():
    target = Decimal(10)
    order = exchange.OrderInfo(
        id="1234", is_open=True, operation=bs.OrderOperation.BUY, amount=target, amount_filled=Decimal(0),
        amount_remaining=target, quote_amount_filled=Decimal(0), fees={}
    )
    pos_info = PositionInfo(
        pair=bs.Pair("BTC", "USDT"), initial=Decimal(0), initial_avg_price=Decimal(0), target=target, order=order
    )

    assert pos_info.order_open is True
    assert pos_info.target_reached is False
    assert pos_info.initial_avg_price == Decimal(0)
    assert pos_info.avg_price == Decimal(0)

    order.amount_filled = Decimal(7)
    order.amount_remaining = Decimal(3)
    order.quote_amount_filled = Decimal(70000)

    assert pos_info.order_open is True
    assert pos_info.target_reached is False
    assert pos_info.avg_price == Decimal(10000)

    order.is_open = False

    assert pos_info.order_open is False
    assert pos_info.target_reached is False
    assert pos_info.avg_price == Decimal(10000)


def test_short_completely_filled():
    target = Decimal(-10)
    order = exchange.OrderInfo(
        id="1234", is_open=True, operation=bs.OrderOperation.SELL, amount=target, amount_filled=Decimal(0),
        amount_remaining=abs(target), quote_amount_filled=Decimal(0), fees={}
    )
    pos_info = PositionInfo(
        pair=bs.Pair("BTC", "USDT"), initial=Decimal(0), initial_avg_price=Decimal(0), target=target, order=order
    )

    assert pos_info.order_open is True
    assert pos_info.target_reached is False
    assert pos_info.avg_price == Decimal(0)

    order.amount_filled = abs(target)
    order.amount_remaining = Decimal(0)
    order.quote_amount_filled = Decimal(100000)
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
    order = exchange.OrderInfo(
        id="1", is_open=True, operation=operation, amount=abs(target * 2), amount_filled=Decimal(0),
        amount_remaining=abs(target * 2), quote_amount_filled=Decimal(0), fees={}
    )
    pos_info = PositionInfo(
        pair=bs.Pair("BTC", "USDT"), initial=-target, initial_avg_price=Decimal(900), target=target, order=order
    )

    assert pos_info.order_open is True
    assert pos_info.target_reached is False

    # The order gets partially filled and gets canceled.
    pos_info.order.amount_filled = Decimal(13)
    pos_info.order.amount_remaining = Decimal(7)
    pos_info.order.quote_amount_filled = Decimal(13000)
    pos_info.order.is_open = False

    assert pos_info.order_open is False
    assert pos_info.target_reached is False
    assert pos_info.avg_price == Decimal(1000)

    # The last order was canceled and a new one will start at 3/-3.
    pos_info.initial, pos_info.initial_avg_price = pos_info.current, pos_info.avg_price
    pos_info.order = exchange.OrderInfo(
        id="2", is_open=True, operation=operation, amount=Decimal(7), amount_filled=Decimal(0),
        amount_remaining=Decimal(7), quote_amount_filled=Decimal(0), fees={}
    )

    assert pos_info.order_open is True
    assert pos_info.target_reached is False

    # The order gets partially filled and gets canceled.
    pos_info.order.amount_filled = Decimal(5)
    pos_info.order.amount_remaining = Decimal(2)
    pos_info.order.quote_amount_filled = Decimal(5500)
    pos_info.order.is_open = False

    assert pos_info.order_open is False
    assert pos_info.target_reached is False
    assert pos_info.avg_price == (3 * Decimal(1000) + 5 * Decimal(1100)) / Decimal(8)

    # We're at 8/-8, and the position is resized to 7/-7 so we need to step back 1.
    pos_info.initial, pos_info.initial_avg_price = pos_info.current, pos_info.avg_price
    pos_info.target = Decimal(7) * sign
    pos_info.order = exchange.OrderInfo(
        id="3", is_open=True, operation=reverse_operation, amount=Decimal(1), amount_filled=Decimal(0),
        amount_remaining=Decimal(1), quote_amount_filled=Decimal(0), fees={}
    )

    # The resize order gets completely filled.
    pos_info.order.amount_filled = Decimal(1)
    pos_info.order.amount_remaining = Decimal(0)
    pos_info.order.quote_amount_filled = Decimal(100000)
    pos_info.order.is_open = False

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
    order_amount = abs(target - initial)
    is_open = order_amount != order_filled_amount

    order = exchange.OrderInfo(
        id="1", is_open=is_open, operation=order_operation, amount=order_amount,
        amount_filled=order_filled_amount, amount_remaining=order_amount - order_filled_amount,
        quote_amount_filled=order_filled_amount * order_filled_price, fees={}
    )
    pos_info = PositionInfo(
        pair=bs.Pair("BTC", "USDT"), initial=initial, initial_avg_price=initial_avg_price, target=target, order=order
    )
    assert pos_info.avg_price == expected_avg_price


def test_pnl_pct():
    order = exchange.OrderInfo(
        id="1", is_open=False, operation=bs.OrderOperation.BUY, amount=Decimal(1),
        amount_filled=Decimal(1), amount_remaining=Decimal(0),
        quote_amount_filled=(Decimal(1000)), fees={}
    )
    pos_info = PositionInfo(
        pair=bs.Pair("BTC", "USDT"), initial=Decimal(0), initial_avg_price=Decimal(0), target=Decimal(1), order=order
    )

    assert pos_info.avg_price == Decimal(1000)
    assert pos_info.calculate_unrealized_pnl_pct(Decimal(1000), Decimal(1000)) == 0
    assert pos_info.calculate_unrealized_pnl_pct(Decimal(1500), Decimal(9999)) == Decimal("50")
    assert pos_info.calculate_unrealized_pnl_pct(Decimal(700), Decimal(9999)) == Decimal("-30")
