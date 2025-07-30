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
import datetime

from dateutil import tz

from basana.backtesting import requests
from basana.core.enums import OrderOperation
from basana.core.pair import Pair


orcl_p = Pair("ORCL", "USD")
aapl_p = Pair("AAPL", "USD")

test_order_requests_data = [
    # Order plan is a dictionary with the following structure:
    # {
    #     datetime.date: [
    #         (
    #             order_factory(exchange),
    #             Optional[expected_fills],
    #             Optional[expected_order_events],
    #         ),
    #     ],
    # },
    # Stop order canceled due to insufficient funds.
    (
        {
            datetime.date(2000, 1, 4): [
                (
                    lambda e: e.create_stop_order(
                        OrderOperation.BUY, orcl_p, Decimal("1e6"), Decimal("0.01")
                    ),
                    [],
                    None
                ),
            ],
        },
        False
    ),
    # Market order canceled due to insufficient funds.
    (
        {
            datetime.date(2000, 1, 8): [
                (
                    lambda e: e.create_market_order(
                        OrderOperation.BUY, orcl_p, Decimal("9649")
                    ),
                    [],
                    None
                ),
            ],
        },
        False
    ),
    # Multiple orders in the test.
    (
        {
            datetime.date(2000, 1, 4): [
                # Buy market.
                (
                    lambda e: e.create_market_order(
                        OrderOperation.BUY, orcl_p, Decimal("2")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 1, 5, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("2"), "USD": Decimal("-231.00")},
                            fees={"USD": Decimal("-0.58")},
                        ),
                    ],
                    [
                        dict(
                            pair=orcl_p,
                            is_open=True,
                            operation=OrderOperation.BUY,
                            amount=Decimal("2"),
                            amount_filled=Decimal("0"),
                            amount_remaining=Decimal("2"),
                            quote_amount_filled=Decimal("0"),
                            fees={}
                        ),
                        dict(
                            is_open=False,
                            operation=OrderOperation.BUY,
                            amount=Decimal("2"),
                            amount_filled=Decimal("2"),
                            amount_remaining=Decimal("0"),
                            quote_amount_filled=Decimal("231.00"),
                            fees={"USD": Decimal("0.58")},
                        ),
                    ]
                ),
                # Limit order within bar.
                (
                    lambda e: e.create_limit_order(
                        OrderOperation.BUY, orcl_p, Decimal("4"), Decimal("110.01")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 1, 5, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("4"), "USD": Decimal("-440.04")},
                            fees={"USD": Decimal("-1.11")},
                        ),
                    ],
                    None
                ),
            ],
            datetime.date(2000, 1, 15): [
                # Sell market.
                (
                    lambda e: e.create_market_order(
                        OrderOperation.SELL, orcl_p, Decimal("1")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 1, 19, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("-1"), "USD": Decimal("107.87")},
                            fees={"USD": Decimal("-0.27")},
                        ),
                    ],
                    None
                ),
                # Limit order within bar.
                (
                    lambda e: e.create_limit_order(
                        OrderOperation.SELL, orcl_p, Decimal("1"), Decimal("108")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 1, 19, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("-1"), "USD": Decimal("108.00")},
                            fees={"USD": Decimal("-0.27")},
                        ),
                    ],
                    None
                ),
                # Sell stop.
                (
                    lambda e: e.create_stop_order(
                        OrderOperation.SELL, orcl_p, Decimal("1"), Decimal("108")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 1, 19, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("-1"), "USD": Decimal("107.87")},
                            fees={"USD": Decimal("-0.27")},
                        ),
                    ],
                    None
                ),
            ],
            datetime.date(2000, 1, 19): [
                # Stop price should be hit on 2000-01-20 and order should be filled on 2000-01-24.
                (
                    lambda e: e.create_stop_limit_order(
                        OrderOperation.BUY, orcl_p, Decimal("5"), Decimal("59.5"),
                        Decimal("58.03")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 1, 25, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("5"), "USD": Decimal("-290.15")},
                            fees={"USD": Decimal("-0.73")},
                        ),
                    ],
                    None
                ),
            ],
            datetime.date(2000, 3, 10): [
                # Stop price should be hit on 2000-03-10 and order should be filled on 2000-03-13 at open price.
                (
                    lambda e: e.create_stop_limit_order(
                        OrderOperation.BUY, orcl_p, Decimal("10"), Decimal("81.62"),
                        Decimal("80.24")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 3, 14, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("10"), "USD": Decimal("-785.00")},
                            fees={"USD": Decimal("-1.97")},
                        ),
                    ],
                    None
                ),
                # Stop price should be hit on 2000-03-10 and order should be filled on 2000-03-10.
                (
                    lambda e: e.create_stop_limit_order(
                        OrderOperation.BUY, orcl_p, Decimal("9"), Decimal("81.62"),
                        Decimal("81")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 3, 11, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("9"), "USD": Decimal("-729.00")},
                            fees={"USD": Decimal("-1.83")},
                        ),
                    ],
                    None
                ),
                # Stop price should be hit on 2000-03-13 and order should be filled on 2000-03-13.
                (
                    lambda e: e.create_stop_limit_order(
                        OrderOperation.SELL, orcl_p, Decimal("1"), Decimal("79"),
                        Decimal("78.75")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 3, 14, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("-1"), "USD": Decimal("78.75")},
                            fees={"USD": Decimal("-0.20")},
                        ),
                    ],
                    None
                ),
                # Stop price should be hit on 2000-03-13 and order should be filled on 2000-03-14.
                (
                    lambda e: e.create_stop_limit_order(
                        OrderOperation.SELL, orcl_p, Decimal("1"), Decimal("79"),
                        Decimal("83.65")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 3, 15, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("-1"), "USD": Decimal("83.65")},
                            fees={"USD": Decimal("-0.21")},
                        ),
                    ],
                    None
                ),
                # Stop price should be hit on 2000-03-13 and order should be filled on 2000-03-15 at open.
                (
                    lambda e: e.create_stop_limit_order(
                        OrderOperation.SELL, orcl_p, Decimal("1"), Decimal("79"),
                        Decimal("83.80")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 3, 16, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("-1"), "USD": Decimal("84.00")},
                            fees={"USD": Decimal("-0.21")},
                        ),
                    ],
                    None
                ),
            ],
        },
        False
    ),
    # Limit order is filled in multiple bars.
    (
        {
            datetime.date(2001, 1, 3): [
                (
                    lambda e: e.create_limit_order(
                        OrderOperation.BUY, aapl_p, Decimal("50"), Decimal("10")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2001, 1, 4, tzinfo=tz.tzlocal()),
                            balance_updates={"AAPL": Decimal("25"), "USD": Decimal("-137.50")},
                            fees={"USD": Decimal("-0.35")},
                        ),
                        dict(
                            when=datetime.datetime(2001, 1, 5, tzinfo=tz.tzlocal()),
                            balance_updates={"AAPL": Decimal("25"), "USD": Decimal("-137.50")},
                            fees={"USD": Decimal("-0.34")},
                        ),
                    ],
                    [
                        dict(
                            is_open=True,
                            operation=OrderOperation.BUY,
                            amount=Decimal("50"),
                            amount_filled=Decimal("0"),
                            amount_remaining=Decimal("50"),
                            quote_amount_filled=Decimal("0"),
                            fees={},
                            limit_price=Decimal("10"),
                        ),
                        dict(
                            is_open=True,
                            operation=OrderOperation.BUY,
                            amount=Decimal("50"),
                            amount_filled=Decimal("25"),
                            amount_remaining=Decimal("25"),
                            quote_amount_filled=Decimal("137.50"),
                            fees={"USD": Decimal("0.35")},
                            limit_price=Decimal("10"),
                        ),
                        dict(
                            is_open=False,
                            operation=OrderOperation.BUY,
                            amount=Decimal("50"),
                            amount_filled=Decimal("50"),
                            amount_remaining=Decimal("0"),
                            quote_amount_filled=Decimal("275.00"),
                            fees={"USD": Decimal("0.69")},
                            limit_price=Decimal("10"),
                        ),
                    ]
                ),
            ],
        },
        False
    ),
    # Regression test.
    (
        {
            datetime.date(2000, 1, 4): [
                (
                    lambda e: e.create_limit_order(
                        OrderOperation.BUY, orcl_p, Decimal("8600"), Decimal("115.50")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 1, 5, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("8600"), "USD": Decimal("-993300.00")},
                            fees={"USD": Decimal("-2483.25")},
                        ),
                    ],
                    None
                ),
            ],
        },
        False
    ),
    # Multiple orders with immediate processing enabled.
    (
        {
            datetime.date(2000, 1, 4): [
                # Market order gets filled immediately.
                (
                    lambda e: e.create_market_order(OrderOperation.BUY, orcl_p, Decimal("1")),
                    [
                        dict(
                            when=datetime.datetime(2000, 1, 4, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("1"), "USD": Decimal("-118.12")},
                            fees={"USD": Decimal("-0.3")},
                        ),
                    ],
                    [
                        dict(
                            is_open=False,
                            operation=OrderOperation.BUY,
                            amount=Decimal("1"),
                            amount_filled=Decimal("1"),
                            amount_remaining=Decimal("0"),
                            quote_amount_filled=Decimal("118.12"),
                            fees={"USD": Decimal("0.3")},
                            limit_price=None
                        ),
                    ]
                ),
                # Limit order gets filled immediately.
                (
                    lambda e: e.create_limit_order(
                        OrderOperation.BUY, orcl_p, Decimal("1"), Decimal("119")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 1, 4, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("1"), "USD": Decimal("-118.12")},
                            fees={"USD": Decimal("-0.3")},
                        ),
                    ],
                    [
                        dict(
                            is_open=False,
                            operation=OrderOperation.BUY,
                            amount=Decimal("1"),
                            amount_filled=Decimal("1"),
                            amount_remaining=Decimal("0"),
                            quote_amount_filled=Decimal("118.12"),
                            fees={"USD": Decimal("0.3")},
                            limit_price=Decimal("119")
                        ),
                    ]
                ),
                # Limit order gets filled on the next bar.
                (
                    lambda e: e.create_limit_order(
                        OrderOperation.BUY, orcl_p, Decimal("1"), Decimal("108")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 1, 5, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("1"), "USD": Decimal("-108")},
                            fees={"USD": Decimal("-0.27")},
                        ),
                    ],
                    [
                        dict(
                            is_open=True,
                            operation=OrderOperation.BUY,
                            amount=Decimal("1"),
                            amount_filled=Decimal("0"),
                            amount_remaining=Decimal("1"),
                            quote_amount_filled=Decimal("0"),
                            fees={},
                            limit_price=Decimal("108")
                        ),
                        dict(
                            is_open=False,
                            operation=OrderOperation.BUY,
                            amount=Decimal("1"),
                            amount_filled=Decimal("1"),
                            amount_remaining=Decimal("0"),
                            quote_amount_filled=Decimal("108"),
                            fees={"USD": Decimal("0.27")},
                            limit_price=Decimal("108")
                        ),
                    ]
                ),
            ],
            datetime.date(2000, 1, 25): [
                # Stop order gets filled immediately.
                (
                    lambda e: e.create_stop_order(
                        OrderOperation.SELL, orcl_p, Decimal("1"), Decimal("55")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 1, 25, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("-1"), "USD": Decimal("54.19")},
                            fees={"USD": Decimal("-0.14")},
                        ),
                    ],
                    [
                        dict(
                            is_open=False,
                            operation=OrderOperation.SELL,
                            amount=Decimal("1"),
                            amount_filled=Decimal("1"),
                            amount_remaining=Decimal("0"),
                            quote_amount_filled=Decimal("54.19"),
                            fees={"USD": Decimal("0.14")},
                            stop_price=Decimal("55")
                        )
                    ]
                ),
            ],
            datetime.date(2000, 1, 26): [
                # Stop limit order gets filled on the next bar.
                (
                    lambda e: e.create_stop_limit_order(
                        OrderOperation.SELL, orcl_p, Decimal("1"), Decimal("56"), Decimal("55.5")
                    ),
                    [
                        dict(
                            when=datetime.datetime(2000, 1, 27, tzinfo=tz.tzlocal()),
                            balance_updates={"ORCL": Decimal("-1"), "USD": Decimal("55.5")},
                            fees={"USD": Decimal("-0.14")},
                        ),
                    ],
                    [
                        dict(
                            is_open=True,
                            operation=OrderOperation.SELL,
                            amount=Decimal("1"),
                            amount_filled=Decimal("0"),
                            amount_remaining=Decimal("1"),
                            quote_amount_filled=Decimal("0"),
                            fees={},
                            stop_price=Decimal("56"),
                            limit_price=Decimal("55.5")
                        ),
                        dict(
                            is_open=False,
                            operation=OrderOperation.SELL,
                            amount=Decimal("1"),
                            amount_filled=Decimal("1"),
                            amount_remaining=Decimal("0"),
                            quote_amount_filled=Decimal("55.5"),
                            fees={"USD": Decimal("0.14")},
                            stop_price=Decimal("56"),
                            limit_price=Decimal("55.5")
                        )
                    ]
                ),
            ],
            datetime.date(2000, 12, 29): [
                # No bars for the given pair at the moment. Should get canceled.
                (
                    lambda e: e.create_market_order(OrderOperation.BUY, aapl_p, Decimal("1")),
                    [],
                    [
                        dict(
                            is_open=False,
                            operation=OrderOperation.BUY,
                            amount=Decimal("1"),
                            amount_filled=Decimal("0"),
                            amount_remaining=Decimal("1"),
                            quote_amount_filled=Decimal("0"),
                            fees={},
                            limit_price=None
                        ),
                    ]
                ),
            ],
        },
        True
    ),
]

test_invalid_requests_data = [
    # Market order: Invalid amount.
    requests.MarketOrder(OrderOperation.BUY, orcl_p, Decimal(0)),
    requests.MarketOrder(OrderOperation.BUY, orcl_p, Decimal(-1)),
    requests.MarketOrder(OrderOperation.BUY, orcl_p, Decimal("0.1")),
    requests.MarketOrder(OrderOperation.BUY, orcl_p, Decimal("1.1")),
    requests.MarketOrder(OrderOperation.BUY, Pair("BTC", "USD"), Decimal("1.000000001")),
    # Limit order: Invalid amount/price.
    requests.LimitOrder(OrderOperation.BUY, orcl_p, Decimal(0), Decimal("1")),
    requests.LimitOrder(OrderOperation.BUY, orcl_p, Decimal(1), Decimal("0")),
    requests.LimitOrder(OrderOperation.BUY, orcl_p, Decimal(1), Decimal("0.001")),
    requests.LimitOrder(OrderOperation.BUY, orcl_p, Decimal(1), Decimal("1.001")),
    requests.LimitOrder(OrderOperation.BUY, orcl_p, Decimal(1), Decimal("-0.1")),
    # Stop order: Invalid amount/price.
    requests.StopOrder(OrderOperation.BUY, orcl_p, Decimal(0), Decimal("1")),
    requests.StopOrder(OrderOperation.BUY, orcl_p, Decimal(1), Decimal("0")),
    requests.StopOrder(OrderOperation.BUY, orcl_p, Decimal(1), Decimal("0.001")),
    requests.StopOrder(OrderOperation.BUY, orcl_p, Decimal(1), Decimal("1.001")),
    requests.StopOrder(OrderOperation.BUY, orcl_p, Decimal(1), Decimal("-0.1")),
    # Stop limit order: Invalid amount/price.
    requests.StopLimitOrder(
        OrderOperation.BUY, orcl_p, Decimal(0), Decimal("1"), Decimal("1")
    ),
    requests.StopLimitOrder(
        OrderOperation.BUY, orcl_p, Decimal(1), Decimal("0"), Decimal("1")
    ),
    requests.StopLimitOrder(
        OrderOperation.BUY, orcl_p, Decimal(1), Decimal("0.001"), Decimal("1")
    ),
    requests.StopLimitOrder(
        OrderOperation.BUY, orcl_p, Decimal(1), Decimal("1.001"), Decimal("1")
    ),
    requests.StopLimitOrder(
        OrderOperation.BUY, orcl_p, Decimal(1), Decimal("-0.1"), Decimal("1")
    ),
    requests.StopLimitOrder(
        OrderOperation.BUY, orcl_p, Decimal(1), Decimal("1"), Decimal("0")
    ),
    requests.StopLimitOrder(
        OrderOperation.BUY, orcl_p, Decimal(1), Decimal("1"), Decimal("0.001")
    ),
    requests.StopLimitOrder(
        OrderOperation.BUY, orcl_p, Decimal(1), Decimal("1"), Decimal("1.001")
    ),
    requests.StopLimitOrder(
        OrderOperation.BUY, orcl_p, Decimal(1), Decimal("1"), Decimal("-0.1")
    ),
]
