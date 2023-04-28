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

from . import client, helpers
from basana.core.enums import OrderOperation
from basana.core.pair import Pair


class ExchangeOrder(metaclass=abc.ABCMeta):
    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, client_order_id: Optional[str] = None,
            **kwargs: Dict[str, Any]
    ):
        self._operation = operation
        self._pair = pair
        self._amount = amount
        self._client_order_id = client_order_id
        self._kwargs = kwargs

    def _get_action(self) -> str:
        return {
            OrderOperation.BUY: "buy",
            OrderOperation.SELL: "sell",
        }[self.operation]

    @property
    def pair(self) -> Pair:
        return self._pair

    @property
    def amount(self) -> Decimal:
        return self._amount

    @property
    def operation(self) -> OrderOperation:
        return self._operation

    @abc.abstractmethod
    async def create_order(self, cli: client.APIClient) -> dict:
        raise NotImplementedError()


class MarketOrder(ExchangeOrder):
    """
    Market order request.

    A market order is an order to buy or sell a stock at the best available price.
    Generally, this type of order will be executed immediately. However, the price at which a market order will be
    executed is not guaranteed.
    """

    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, client_order_id: Optional[str] = None,
            **kwargs: Dict[str, Any]
    ):
        super().__init__(operation, pair, amount, client_order_id=client_order_id, **kwargs)

    async def create_order(self, cli: client.APIClient) -> dict:
        return await cli.create_market_order(
            self._get_action(), helpers.pair_to_currency_pair(self.pair), self.amount,
            client_order_id=self._client_order_id, **self._kwargs
        )


class LimitOrder(ExchangeOrder):
    """Limit order request.

    A limit order is an order to buy or sell a stock at a specific price or better.
    A buy limit order can only be executed at the limit price or lower, and a sell limit order can only be executed
    at the limit price or higher.
    """

    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, limit_price: Decimal,
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ):
        super().__init__(operation, pair, amount, client_order_id=client_order_id, **kwargs)
        self._limit_price = limit_price

    @property
    def limit_price(self) -> Decimal:
        return self._limit_price

    async def create_order(self, cli: client.APIClient) -> dict:
        return await cli.create_limit_order(
            self._get_action(), helpers.pair_to_currency_pair(self.pair), self.amount, self.limit_price,
            client_order_id=self._client_order_id, **self._kwargs
        )


class InstantOrder(ExchangeOrder):
    def __init__(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, amount_in_counter: bool = False,
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ):
        super().__init__(operation, pair, amount, client_order_id=client_order_id, **kwargs)
        self._amount_in_counter = amount_in_counter

    async def create_order(self, cli: client.APIClient) -> dict:
        return await cli.create_instant_order(
            self._get_action(), helpers.pair_to_currency_pair(self.pair), self.amount,
            amount_in_counter=self._amount_in_counter, client_order_id=self._client_order_id, **self._kwargs
        )
