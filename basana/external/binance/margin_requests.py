# Basana
#
# Copyright 2022-2023 Gabriel Martin Becedillas Ruiz
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
from typing import Any, Dict, Optional
import abc

from . import helpers
from basana.core.enums import OrderOperation
from basana.core.pair import Pair


class ExchangeOrder(metaclass=abc.ABCMeta):
    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Optional[Decimal],
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ):
        self._operation = operation
        self._pair = pair
        self._amount = amount
        self._client_order_id = client_order_id
        self._kwargs = kwargs

    @abc.abstractmethod
    async def create_order(self, margin_account_cli) -> dict:
        raise NotImplementedError()


class MarketOrder(ExchangeOrder):
    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Optional[Decimal] = None,
            quote_amount: Optional[Decimal] = None, client_order_id: Optional[str] = None,
            side_effect_type: str = "NO_SIDE_EFFECT", **kwargs: Dict[str, Any]
    ):
        assert (amount is not None) ^ (quote_amount is not None), "Either amount or quote_amount should be set"
        super().__init__(operation, pair, amount, client_order_id=client_order_id, **kwargs)
        self._quote_amount = quote_amount
        self._side_effect_type = side_effect_type

    async def create_order(self, margin_account_cli) -> dict:
        return await margin_account_cli.create_order(
            helpers.pair_to_order_book_symbol(self._pair), helpers.order_operation_to_side(self._operation), "MARKET",
            quantity=self._amount, quote_order_qty=self._quote_amount, new_client_order_id=self._client_order_id,
            side_effect_type=self._side_effect_type, **self._kwargs
        )


class LimitOrder(ExchangeOrder):
    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, limit_price: Decimal,
            side_effect_type: str = "NO_SIDE_EFFECT", time_in_force: str = "GTC", client_order_id: Optional[str] = None,
            **kwargs: Dict[str, Any]
    ):
        super().__init__(operation, pair, amount, client_order_id=client_order_id, **kwargs)
        self._limit_price = limit_price
        self._time_in_force = time_in_force
        self._side_effect_type = side_effect_type

    async def create_order(self, margin_account_cli) -> dict:
        return await margin_account_cli.create_order(
            helpers.pair_to_order_book_symbol(self._pair), helpers.order_operation_to_side(self._operation), "LIMIT",
            quantity=self._amount, price=self._limit_price, time_in_force=self._time_in_force,
            new_client_order_id=self._client_order_id, side_effect_type=self._side_effect_type, **self._kwargs
        )


class StopLimitOrder(ExchangeOrder):
    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal, limit_price: Decimal,
            side_effect_type: str = "NO_SIDE_EFFECT", time_in_force: str = "GTC",
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ):
        super().__init__(operation, pair, amount, client_order_id=client_order_id, **kwargs)
        self._stop_price = stop_price
        self._limit_price = limit_price
        self._time_in_force = time_in_force
        self._side_effect_type = side_effect_type

    async def create_order(self, margin_account_cli) -> dict:
        return await margin_account_cli.create_order(
            helpers.pair_to_order_book_symbol(self._pair), helpers.order_operation_to_side(self._operation),
            "STOP_LOSS_LIMIT", quantity=self._amount, stop_price=self._stop_price, price=self._limit_price,
            time_in_force=self._time_in_force, new_client_order_id=self._client_order_id,
            side_effect_type=self._side_effect_type, **self._kwargs
        )


class OCOOrder:
    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, limit_price: Decimal, stop_price: Decimal,
            stop_limit_price: Optional[Decimal] = None, side_effect_type: str = "NO_SIDE_EFFECT",
            stop_limit_time_in_force: str = "GTC", list_client_order_id: Optional[str] = None,
            limit_client_order_id: Optional[str] = None, stop_client_order_id: Optional[str] = None,
            **kwargs: Dict[str, Any]
    ):
        self._operation = operation
        self._pair = pair
        self._amount = amount
        self._limit_price = limit_price
        self._stop_price = stop_price
        self._stop_limit_price = stop_limit_price
        self._stop_limit_time_in_force = None if stop_limit_price is None else stop_limit_time_in_force
        self._list_client_order_id = list_client_order_id
        self._side_effect_type = side_effect_type
        self._limit_client_order_id = limit_client_order_id
        self._stop_client_order_id = stop_client_order_id
        self._kwargs = kwargs

    async def create_order(self, margin_account_cli) -> dict:
        return await margin_account_cli.create_oco(
            helpers.pair_to_order_book_symbol(self._pair), helpers.order_operation_to_side(self._operation),
            self._amount, self._limit_price, self._stop_price, stop_limit_price=self._stop_limit_price,
            stop_limit_time_in_force=self._stop_limit_time_in_force, list_client_order_id=self._list_client_order_id,
            side_effect_type=self._side_effect_type, limit_client_order_id=self._limit_client_order_id,
            stop_client_order_id=self._stop_client_order_id, **self._kwargs
        )
