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

from ccxt.base.decimal_to_precision import SIGNIFICANT_DIGITS, TICK_SIZE  # type: ignore[import-untyped]

from typing import Any, List, Optional, Union

from basana.core import bar, helpers as core_helpers
from basana.core.enums import OrderOperation, PrecisionMode
from basana.core.pair import Pair, PairInfo


Candle = List[Union[int, float, str]]


def to_decimal(value: Union[int, float, str, Decimal]) -> Decimal:
    return Decimal(str(value))


def optional_decimal(value: Optional[Union[int, float, str, Decimal]]) -> Optional[Decimal]:
    if value is None:
        return None
    return to_decimal(value)


def pair_to_symbol(pair: Pair) -> str:
    return "{}/{}".format(pair.base_symbol, pair.quote_symbol)


def symbol_to_pair(symbol: str) -> Pair:
    base, quote = symbol.split("/")
    return Pair(base, quote)


def timestamp_to_datetime(timestamp: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(timestamp / 1e3, tz=datetime.timezone.utc)


def ohlcv_to_bar(pair: Pair, candle: Candle, duration_secs: int) -> bar.Bar:
    begin = timestamp_to_datetime(int(candle[0]))
    return bar.Bar(
        begin, pair,
        to_decimal(candle[1]), to_decimal(candle[2]), to_decimal(candle[3]), to_decimal(candle[4]),
        to_decimal(candle[5]), datetime.timedelta(seconds=duration_secs)
    )


def order_operation_to_side(operation: OrderOperation) -> str:
    return {
        OrderOperation.BUY: "buy",
        OrderOperation.SELL: "sell",
    }[operation]


def side_to_order_operation(side: str) -> OrderOperation:
    return {
        "buy": OrderOperation.BUY,
        "sell": OrderOperation.SELL,
    }[side.lower()]


def order_status_is_open(status: str) -> bool:
    return status == "open"


def order_params(client_order_id: Optional[str] = None, **kwargs: Any) -> dict:
    params = dict(kwargs)
    if client_order_id is not None:
        params["clientOrderId"] = client_order_id
    return params


def balance_symbols(balance: dict) -> set:
    symbols = set()
    for field in ["free", "used", "total"]:
        symbols.update(balance.get(field, {}).keys())
    return {symbol.upper() for symbol in symbols}


def pair_info_from_market(market: dict, precision_mode: int) -> PairInfo:
    precision = market["precision"]
    if precision_mode == SIGNIFICANT_DIGITS:
        return PairInfo(
            base_precision=int(precision["amount"]),
            quote_precision=int(precision["price"]),
            precision_mode=PrecisionMode.SIGNIFICANT_DIGITS,
        )
    if precision_mode == TICK_SIZE:
        base_tick_size = to_decimal(precision["amount"])
        quote_tick_size = to_decimal(precision["price"])
        return PairInfo(
            base_precision=core_helpers.decimal_places_from_tick_size(base_tick_size),
            quote_precision=core_helpers.decimal_places_from_tick_size(quote_tick_size),
            precision_mode=PrecisionMode.TICK_SIZE,
            base_tick_size=base_tick_size,
            quote_tick_size=quote_tick_size,
        )
    return PairInfo(
        base_precision=int(precision["amount"]),
        quote_precision=int(precision["price"]),
    )
