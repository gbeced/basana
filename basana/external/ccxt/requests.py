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
from typing import Any, Dict, Optional
import abc

from . import helpers
from basana.core.enums import OrderOperation
from basana.core.pair import Pair


class ExchangeOrder(metaclass=abc.ABCMeta):
    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal,
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ):
        self._operation = operation
        self._pair = pair
        self._amount = amount
        self._client_order_id = client_order_id
        self._kwargs = kwargs

    @abc.abstractmethod
    async def create_order(self, cli) -> dict:
        raise NotImplementedError()


class MarketOrder(ExchangeOrder):
    async def create_order(self, cli) -> dict:
        params = helpers.order_params(self._client_order_id, **self._kwargs)
        return await cli.create_order(
            helpers.pair_to_symbol(self._pair), "market", helpers.order_operation_to_side(self._operation),
            str(self._amount), params=params
        )


class LimitOrder(ExchangeOrder):
    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, limit_price: Decimal,
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ):
        super().__init__(operation, pair, amount, client_order_id=client_order_id, **kwargs)
        self._limit_price = limit_price

    async def create_order(self, cli) -> dict:
        params = helpers.order_params(self._client_order_id, **self._kwargs)
        return await cli.create_order(
            helpers.pair_to_symbol(self._pair), "limit", helpers.order_operation_to_side(self._operation),
            str(self._amount), str(self._limit_price), params
        )


class StopOrder(ExchangeOrder):
    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal,
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ):
        super().__init__(operation, pair, amount, client_order_id=client_order_id, **kwargs)
        self._stop_price = stop_price

    async def create_order(self, cli) -> dict:
        params = helpers.order_params(self._client_order_id, **self._kwargs)
        return await cli.create_stop_order(
            helpers.pair_to_symbol(self._pair), "market", helpers.order_operation_to_side(self._operation),
            str(self._amount), triggerPrice=str(self._stop_price), params=params
        )


class StopLimitOrder(ExchangeOrder):
    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal,
            limit_price: Decimal, client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ):
        super().__init__(operation, pair, amount, client_order_id=client_order_id, **kwargs)
        self._stop_price = stop_price
        self._limit_price = limit_price

    async def create_order(self, cli) -> dict:
        params = helpers.order_params(self._client_order_id, **self._kwargs)
        return await cli.create_stop_limit_order(
            helpers.pair_to_symbol(self._pair), helpers.order_operation_to_side(self._operation),
            str(self._amount), str(self._limit_price), str(self._stop_price), params
        )