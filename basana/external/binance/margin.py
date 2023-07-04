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
from typing import Any, Dict, List, Optional
import abc

from . import common, helpers, margin_requests
from .client import margin as margin_client
from basana.core.enums import OrderOperation
from basana.core.pair import Pair


class CanceledOCOOrder(common.CanceledOCOOrder):
    pass


class CanceledOrder(common.CanceledOrder):
    pass


class OpenOrder(common.OpenOrder):
    pass


class CreatedOCOOrder(common.CreatedOCOOrder):
    pass


class Fill(common.Fill):
    pass


class OCOOrderInfo(common.OCOOrderInfo):
    pass


class OrderInfo(common.OrderInfo):
    pass


class Balance(common.Balance):
    @property
    def borrowed(self) -> Decimal:
        """The borrowed balance."""
        return Decimal(self.json["borrowed"])


class Trade(common.Trade):
    @property
    def is_isolated(self) -> bool:
        return self.json["isIsolated"]


class CreatedOrder(common.CreatedOrder):
    @property
    def fills(self) -> List[Fill]:
        """The fills.

        Only available for FULL responses.
        """
        return [Fill(fill) for fill in self.json.get("fills", [])]


class Account(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def client(self) -> margin_client.MarginAccount:
        raise NotImplementedError()

    async def create_order(self, order_request: margin_requests.ExchangeOrder) -> CreatedOrder:
        created_order = await order_request.create_order(self.client)
        return CreatedOrder(created_order)

    async def create_market_order(
            self, operation: OrderOperation, pair: Pair, amount: Optional[Decimal] = None,
            quote_amount: Optional[Decimal] = None, client_order_id: Optional[str] = None,
            side_effect_type: str = "NO_SIDE_EFFECT", **kwargs: Dict[str, Any]
    ) -> CreatedOrder:
        """Creates a market order.

        Check https://binance-docs.github.io/apidocs/spot/en/#margin-account-new-order-trade for more information.
        If the order can't be created a :class:`basana.external.binance.exchange.Error` will be raised.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The amount to buy/sell in base units.
        :param quote_amount: The amount to buy/sell in quote units.
        :param client_order_id: A client order id.
        :param side_effect_type: One of NO_SIDE_EFFECT, MARGIN_BUY or AUTO_REPAY.
        :param kwargs: Additional keyword arguments that will be forwarded.

        .. note::

          * Either amount or quote_amount should be set, but not both.
        """

        return await self.create_order(margin_requests.MarketOrder(
            operation, pair, amount=amount, quote_amount=quote_amount, client_order_id=client_order_id,
            side_effect_type=side_effect_type, **kwargs
        ))

    async def create_limit_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, limit_price: Decimal,
            side_effect_type: str = "NO_SIDE_EFFECT", time_in_force: str = "GTC", client_order_id: Optional[str] = None,
            **kwargs: Dict[str, Any]
    ) -> CreatedOrder:
        """Creates a limit order.

        Check https://binance-docs.github.io/apidocs/spot/en/#margin-account-new-order-trade for more information.
        If the order can't be created a :class:`basana.external.binance.exchange.Error` will be raised.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The amount to buy/sell in base units.
        :param limit_price: The limit price.
        :param side_effect_type: One of NO_SIDE_EFFECT, MARGIN_BUY or AUTO_REPAY.
        :param time_in_force: The time in force.
        :param client_order_id: A client order id.
        :param kwargs: Additional keyword arguments that will be forwarded.
        """

        return await self.create_order(margin_requests.LimitOrder(
            operation, pair, amount, limit_price, side_effect_type=side_effect_type, time_in_force=time_in_force,
            client_order_id=client_order_id, **kwargs
        ))

    async def create_stop_limit_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal, limit_price: Decimal,
            side_effect_type: str = "NO_SIDE_EFFECT", time_in_force: str = "GTC",
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> CreatedOrder:
        """Creates a stop limit order.

        Check https://binance-docs.github.io/apidocs/spot/en/#margin-account-new-order-trade for more information.
        If the order can't be created a :class:`basana.external.binance.exchange.Error` will be raised.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The amount to buy/sell in base units.
        :param stop_price: The stop price.
        :param limit_price: The limit price.
        :param side_effect_type: One of NO_SIDE_EFFECT, MARGIN_BUY or AUTO_REPAY.
        :param time_in_force: The time in force.
        :param client_order_id: A client order id.
        :param kwargs: Additional keyword arguments that will be forwarded.
        """

        return await self.create_order(margin_requests.StopLimitOrder(
            operation, pair, amount, stop_price, limit_price, side_effect_type=side_effect_type,
            time_in_force=time_in_force, client_order_id=client_order_id, **kwargs
        ))

    async def get_order_info(
            self, pair: Pair, order_id: Optional[str] = None, client_order_id: Optional[str] = None,
            include_trades: bool = True
    ) -> OrderInfo:
        """Returns information about an order.

        :param pair: That trading pair.
        :param order_id: The order id.
        :param client_order_id: The client order id.

        .. note::

          * Either order_id or client_order_id should be set, but not both.
          * Including trades requires making an extra request to Binance.
        """
        order_book_symbol = helpers.pair_to_order_book_symbol(pair)
        order_info = await self.client.query_order(
            order_book_symbol, order_id=None if order_id is None else int(order_id),
            orig_client_order_id=client_order_id
        )
        trades = []
        if include_trades:
            trades = [
                Trade(trade) for trade in
                await self.client.get_trades(order_book_symbol, order_id=order_info["orderId"])
            ]
        return OrderInfo(order_info, trades)

    async def get_open_orders(self, pair: Optional[Pair] = None) -> List[OpenOrder]:
        """Returns open orders.

        :param pair: If set, only open orders matching this pair will be returned, otherwise all open orders will be
            returned.
        """
        order_book_symbol = None
        if pair:
            order_book_symbol = helpers.pair_to_order_book_symbol(pair)
        return [
            OpenOrder(open_order) for open_order in await self.client.get_open_orders(order_book_symbol)
        ]

    async def cancel_order(
            self, pair: Pair, order_id: Optional[str] = None, client_order_id: Optional[str] = None,
    ) -> CanceledOrder:
        """Cancels an order.

        If the order can't be canceled a :class:`basana.external.binance.exchange.Error` will be raised.

        :param pair: The trading pair.
        :param order_id: The order id.
        :param client_order_id: The client order id.

        .. note::

          * Either order_id or client_order_id should be set, but not both.
        """
        canceled_order = await self.client.cancel_order(
            helpers.pair_to_order_book_symbol(pair), order_id=None if order_id is None else int(order_id),
            orig_client_order_id=client_order_id
        )
        return CanceledOrder(canceled_order)

    async def create_oco_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, limit_price: Decimal, stop_price: Decimal,
            stop_limit_price: Optional[Decimal] = None, side_effect_type: str = "NO_SIDE_EFFECT",
            stop_limit_time_in_force: str = "GTC", list_client_order_id: Optional[str] = None,
            limit_client_order_id: Optional[str] = None, stop_client_order_id: Optional[str] = None,
            **kwargs: Dict[str, Any]
    ) -> CreatedOCOOrder:
        """Creates an OCO order.

        Check https://binance-docs.github.io/apidocs/spot/en/#margin-account-new-oco-trade for more information.
        If the order can't be created a :class:`basana.external.binance.exchange.Error` will be raised.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The amount to buy/sell in base units.
        :param limit_price: The limit price.
        :param stop_price: The stop price.
        :param stop_limit_price: The stop limit price.
        :param side_effect_type: One of NO_SIDE_EFFECT, MARGIN_BUY or AUTO_REPAY.
        :param stop_limit_time_in_force: The time in force for the stop limit order.
        :param list_client_order_id: A client id for the order list.
        :param limit_client_order_id: A client id for the limit order.
        :param stop_client_order_id: A client id for the stop order.
        :param kwargs: Additional keyword arguments that will be forwarded.
        """
        order_req = margin_requests.OCOOrder(
            operation, pair, amount, limit_price, stop_price, stop_limit_price=stop_limit_price,
            side_effect_type=side_effect_type, stop_limit_time_in_force=stop_limit_time_in_force,
            list_client_order_id=list_client_order_id, limit_client_order_id=limit_client_order_id,
            stop_client_order_id=stop_client_order_id, **kwargs
        )
        created_order = await order_req.create_order(self.client)
        return CreatedOCOOrder(created_order)

    async def get_oco_order_info(
            self, order_list_id: Optional[str] = None, client_order_list_id: Optional[str] = None,
    ) -> OCOOrderInfo:
        """Returns information about an OCO order.

        :param order_list_id: The order list id.
        :param client_order_list_id: A client id for the order list.

        .. note::

          * Either order_list_id or client_order_list_id should be set, but not both.
        """
        order_info = await self.client.query_oco_order(
            order_list_id=None if order_list_id is None else int(order_list_id),
            client_order_list_id=client_order_list_id
        )
        return OCOOrderInfo(order_info)

    async def cancel_oco_order(
            self, pair: Pair, order_list_id: Optional[str] = None, client_order_list_id: Optional[str] = None,
    ) -> CanceledOCOOrder:
        """Cancels an OCO order.

        If the order can't be canceled a :class:`basana.external.binance.exchange.Error` will be raised.

        :param pair: The trading pair.
        :param order_list_id: The order list id.
        :param client_order_list_id: A client id for the order list.

        .. note::

          * Either order_list_id or client_order_list_id should be set, but not both.
        """
        canceled_order = await self.client.cancel_oco_order(
            helpers.pair_to_order_book_symbol(pair),
            order_list_id=None if order_list_id is None else int(order_list_id),
            client_order_list_id=client_order_list_id
        )
        return CanceledOCOOrder(canceled_order)
