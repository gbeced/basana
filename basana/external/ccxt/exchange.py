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
from typing import cast, Any, Dict, Optional, Tuple
import datetime

from dateutil.parser import parse as dt_parse
import aiohttp
import ccxt.pro as ccxt  # type: ignore[import-untyped]

from . import bars, helpers, order_book, orders, requests, trades
from basana.core import bar, dispatcher
from basana.core.enums import OrderOperation
from basana.core.pair import Pair, PairInfo


TradeEvent = trades.TradeEvent
TradeEventHandler = trades.TradeEventHandler
PartialOrderBook = order_book.PartialOrderBook
PartialOrderBookEvent = order_book.PartialOrderBookEvent
PartialOrderBookEventHandler = order_book.PartialOrderBookEventHandler
OrderEvent = orders.OrderEvent
OrderEventHandler = orders.OrderEventHandler


class Balance:
    def __init__(self, balance: dict, symbol: str):
        self._balance = balance
        self._symbol = symbol.upper()

    @property
    def available(self) -> Decimal:
        """The available balance."""
        return helpers.to_decimal(self._balance.get("free", {}).get(self._symbol, 0))

    @property
    def locked(self) -> Decimal:
        """The locked balance."""
        return helpers.to_decimal(self._balance.get("used", {}).get(self._symbol, 0))

    @property
    def total(self) -> Decimal:
        """The total balance (available + locked)."""
        total = self._balance.get("total", {}).get(self._symbol)
        if total is None:
            return self.available + self.locked
        return helpers.to_decimal(total)


class OrderInfo:
    def __init__(self, json: dict):
        self.json = json

    @property
    def id(self) -> str:
        """The order id."""
        return str(self.json["id"])

    @property
    def client_order_id(self) -> Optional[str]:
        """The client order id."""
        return helpers.optional_client_order_id(self.json)

    @property
    def operation(self) -> OrderOperation:
        """The operation."""
        return helpers.side_to_order_operation(self.json["side"])

    @property
    def is_open(self) -> bool:
        """True if the order is open, False otherwise."""
        return helpers.order_status_is_open(self.json["status"])

    @property
    def amount(self) -> Decimal:
        """The amount."""
        return helpers.to_decimal(self.json["amount"])

    @property
    def amount_filled(self) -> Decimal:
        """The amount filled."""
        return helpers.to_decimal(self.json.get("filled", 0))

    @property
    def amount_remaining(self) -> Decimal:
        """The amount remaining to be filled."""
        return helpers.to_decimal(self.json.get("remaining", 0))

    @property
    def quote_amount_filled(self) -> Decimal:
        """The amount filled in quote units."""
        return helpers.to_decimal(self.json.get("cost", 0))

    @property
    def limit_price(self) -> Optional[Decimal]:
        """The limit price."""
        return helpers.optional_decimal(self.json.get("price"))

    @property
    def stop_price(self) -> Optional[Decimal]:
        """The stop price."""
        return helpers.optional_decimal(self.json.get("stopPrice"))

    @property
    def fill_price(self) -> Optional[Decimal]:
        """The average fill price.

        Uses the exchange-reported ``average`` price when available; otherwise it is computed as
        ``quote_amount_filled / amount_filled`` (a volume-weighted average), which may not be exact due to rounding.
        """
        average = helpers.optional_decimal(self.json.get("average"))
        if average is not None:
            return average
        if self.amount_filled == 0:
            return None
        return self.quote_amount_filled / self.amount_filled


class CanceledOrder:
    def __init__(self, json: dict):
        self.json = json

    @property
    def id(self) -> str:
        """The order id."""
        return str(self.json["id"])

    @property
    def operation(self) -> OrderOperation:
        """The operation."""
        return helpers.side_to_order_operation(self.json["side"])

    @property
    def amount(self) -> Decimal:
        """The amount."""
        return helpers.to_decimal(self.json["amount"])

    @property
    def limit_price(self) -> Optional[Decimal]:
        """The limit price."""
        return helpers.optional_decimal(self.json.get("price"))

    @property
    def client_order_id(self) -> Optional[str]:
        """The client order id."""
        return helpers.optional_client_order_id(self.json)


class CreatedOrder:
    def __init__(self, json: dict):
        self.json = json

    @property
    def id(self) -> str:
        """The order id."""
        return str(self.json["id"])

    @property
    def datetime(self) -> datetime.datetime:
        """The creation datetime."""
        dt = dt_parse(self.json["datetime"])
        if dt.tzinfo is None:
            return dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)

    @property
    def operation(self) -> OrderOperation:
        """The operation."""
        return helpers.side_to_order_operation(self.json["side"])

    @property
    def limit_price(self) -> Optional[Decimal]:
        """The limit price."""
        return helpers.optional_decimal(self.json.get("price"))

    @property
    def stop_price(self) -> Optional[Decimal]:
        """The stop price."""
        return helpers.optional_decimal(self.json.get("stopPrice"))

    @property
    def amount(self) -> Decimal:
        """The amount."""
        return helpers.to_decimal(self.json["amount"])

    @property
    def client_order_id(self) -> Optional[str]:
        """The client order id."""
        return helpers.optional_client_order_id(self.json)


class Exchange:
    """A client for crypto currency exchanges through `CCXT <https://github.com/ccxt/ccxt>`_.

    This class wraps a subset of the `unified CCXT API
    <https://docs.ccxt.com/#/README?id=unified-api>`_, exposing the most common trading operations
    with an interface that is consistent with the rest of the exchanges in Basana. Any other CCXT
    functionality can still be accessed through the underlying exchange instance, available via
    :attr:`ccxt`.

    :param dispatcher: The event dispatcher.
    :param exchange_id: The CCXT exchange id.
    :param api_key: An optional api key.
    :param api_secret: An optional api secret.
    :param session: An optional client session, in case you want to reuse connections.
    :type session: aiohttp.ClientSession
    :param config: An optional dictionary for overriding CCXT config settings.
    """

    def __init__(
            self, dispatcher: dispatcher.EventDispatcher, exchange_id: str,
            api_key: Optional[str] = None, api_secret: Optional[str] = None,
            session: Optional[aiohttp.ClientSession] = None, config: Optional[dict] = None
    ):
        self._dispatcher = dispatcher
        self._pair_info_cache: Dict[Pair, PairInfo] = {}
        self._bar_event_sources: Dict[Tuple[Pair, str], bars.WatchOHLCVEventSource] = {}
        self._trade_event_sources: Dict[Pair, trades.WatchTradesEventSource] = {}
        self._order_book_event_sources: Dict[Tuple[Pair, Optional[int]], order_book.WatchOrderBookEventSource] = {}
        self._order_event_sources: Dict[Pair, orders.WatchOrdersEventSource] = {}

        ccxt_config = dict(config or {})
        if api_key is not None:
            ccxt_config["apiKey"] = api_key
        if api_secret is not None:
            ccxt_config["secret"] = api_secret
        if session is not None:
            ccxt_config["session"] = session

        self._cli = getattr(ccxt, exchange_id)(ccxt_config)

    @property
    def ccxt(self):
        """The underlying CCXT exchange instance.

        Use this to call any CCXT method not wrapped by this class.
        """
        return self._cli

    async def get_balance(self, symbol: str) -> Balance:
        """
        Returns the balance for a specific currency/symbol/etc..

        :param symbol: The currency/symbol/etc..
        """
        balance = await self._cli.fetch_balance()
        return Balance(balance, symbol)

    async def get_balances(self) -> Dict[str, Balance]:
        """Returns all balances."""
        balance = await self._cli.fetch_balance()
        return {
            symbol: Balance(balance, symbol)
            for symbol in helpers.balance_symbols(balance)
        }

    async def create_order(self, order_request: requests.ExchangeOrder) -> CreatedOrder:
        created_order = await order_request.create_order(self._cli)
        return CreatedOrder(created_order)

    async def create_market_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, client_order_id: Optional[str] = None,
            **kwargs: Any
    ) -> CreatedOrder:
        """Creates a market order.

        If the order can't be created CCXT will raise an exception.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The amount to buy/sell in base units.
        :param client_order_id: A client order id.
        :param kwargs: Additional keyword arguments that will be forwarded.
        """
        return await self.create_order(requests.MarketOrder(
            operation, pair, amount, client_order_id=client_order_id, **kwargs
        ))

    async def create_limit_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, limit_price: Decimal,
            client_order_id: Optional[str] = None, **kwargs: Any
    ) -> CreatedOrder:
        """Creates a limit order.

        If the order can't be created CCXT will raise an exception.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The amount to buy/sell in base units.
        :param limit_price: The limit price.
        :param client_order_id: A client order id.
        :param kwargs: Additional keyword arguments that will be forwarded.
        """
        return await self.create_order(requests.LimitOrder(
            operation, pair, amount, limit_price, client_order_id=client_order_id, **kwargs
        ))

    async def create_stop_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal,
            client_order_id: Optional[str] = None, **kwargs: Any
    ) -> CreatedOrder:
        """Creates a stop order.

        If the order can't be created CCXT will raise an exception.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The amount to buy/sell in base units.
        :param stop_price: The stop price.
        :param client_order_id: A client order id.
        :param kwargs: Additional keyword arguments that will be forwarded.
        """
        return await self.create_order(requests.StopOrder(
            operation, pair, amount, stop_price, client_order_id=client_order_id, **kwargs
        ))

    async def create_stop_limit_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, stop_price: Decimal,
            limit_price: Decimal, client_order_id: Optional[str] = None, **kwargs: Any
    ) -> CreatedOrder:
        """Creates a stop limit order.

        If the order can't be created CCXT will raise an exception.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The amount to buy/sell in base units.
        :param stop_price: The stop price.
        :param limit_price: The limit price.
        :param client_order_id: A client order id.
        :param kwargs: Additional keyword arguments that will be forwarded.
        """
        return await self.create_order(requests.StopLimitOrder(
            operation, pair, amount, stop_price, limit_price, client_order_id=client_order_id, **kwargs
        ))

    async def get_order_info(
            self, pair: Pair, order_id: Optional[str] = None, client_order_id: Optional[str] = None,
            **kwargs: Any
    ) -> OrderInfo:
        """Returns information about an order.

        :param pair: The trading pair.
        :param order_id: The order id.
        :param client_order_id: The client order id.
        :param kwargs: Additional keyword arguments that will be forwarded.

        .. note::

          * Either order_id or client_order_id should be set, but not both.
        """
        if (order_id is None) == (client_order_id is None):
            raise ValueError("Either order_id or client_order_id should be set (but not both)")
        symbol = helpers.pair_to_symbol(pair)
        params = dict(kwargs)
        if client_order_id is not None:
            params["clientOrderId"] = client_order_id
            lookup_id: str = client_order_id
        else:
            lookup_id = cast(str, order_id)
        order = await self._cli.fetch_order(lookup_id, symbol, params)
        return OrderInfo(order)

    async def cancel_order(
            self, pair: Pair, order_id: Optional[str] = None, client_order_id: Optional[str] = None,
            **kwargs: Any
    ) -> CanceledOrder:
        """Cancels an order.

        If the order can't be canceled CCXT will raise an exception.

        :param pair: The trading pair.
        :param order_id: The order id.
        :param client_order_id: The client order id.
        :param kwargs: Additional keyword arguments that will be forwarded.

        .. note::

          * Either order_id or client_order_id should be set, but not both.
        """
        if (order_id is None) == (client_order_id is None):
            raise ValueError("Either order_id or client_order_id should be set (but not both)")
        symbol = helpers.pair_to_symbol(pair)
        params = dict(kwargs)
        if client_order_id is not None:
            params["clientOrderId"] = client_order_id
            lookup_id: str = client_order_id
        else:
            lookup_id = cast(str, order_id)
        canceled_order = await self._cli.cancel_order(lookup_id, symbol, params)
        return CanceledOrder(canceled_order)

    def subscribe_to_bar_events(self, pair: Pair, bar_duration: str, event_handler: bar.BarEventHandler, **kwargs: Any):
        """Registers an async callable that will be called when a new bar is available.

        Uses CCXT Pro's ``watchOHLCV`` websocket method. Only closed candles are reported.

        :param pair: The trading pair.
        :param bar_duration: The bar duration as a CCXT timeframe (e.g. ``1m``, ``5m``, ``1h``).
        :param event_handler: An async callable that receives a :class:`basana.BarEvent`.
        :param kwargs: Additional keyword arguments that will be forwarded to CCXT.

        .. note::

          * The target exchange must support ``watchOHLCV``. Check ``exchange.ccxt.has['watchOHLCV']`` after
            loading markets.
        """
        key = (pair, bar_duration)
        event_source = self._bar_event_sources.get(key)
        if event_source is None:
            event_source = bars.WatchOHLCVEventSource(self._cli, pair, bar_duration, params=dict(kwargs))
            self._bar_event_sources[key] = event_source
        self._dispatcher.subscribe(event_source, cast(dispatcher.EventHandler, event_handler))

    def subscribe_to_public_trade_events(self, pair: Pair, event_handler: TradeEventHandler, **kwargs: Any):
        """Registers an async callable that will be called for every new trade.

        Uses CCXT Pro's ``watchTrades`` websocket method.

        :param pair: The trading pair.
        :param event_handler: An async callable that receives a :class:`TradeEvent`.
        :param kwargs: Additional keyword arguments that will be forwarded to CCXT.

        .. note::

          * The target exchange must support ``watchTrades``. Check ``exchange.ccxt.has['watchTrades']`` after
            loading markets.
        """
        event_source = self._trade_event_sources.get(pair)
        if event_source is None:
            event_source = trades.WatchTradesEventSource(self._cli, pair, params=dict(kwargs))
            self._trade_event_sources[pair] = event_source
        self._dispatcher.subscribe(event_source, cast(dispatcher.EventHandler, event_handler))

    def subscribe_to_order_book_events(
            self, pair: Pair, event_handler: PartialOrderBookEventHandler,
            depth: Optional[int] = None, **kwargs: Any
    ):
        """Registers an async callable that will be called with the top bids/asks of the order book.

        Uses CCXT Pro's ``watchOrderBook`` websocket method.

        :param pair: The trading pair.
        :param event_handler: An async callable that receives a :class:`PartialOrderBookEvent`.
        :param depth: The order book depth. If not set, CCXT's default depth is used.
        :param kwargs: Additional keyword arguments that will be forwarded to CCXT.

        .. note::

          * The target exchange must support ``watchOrderBook``. Check ``exchange.ccxt.has['watchOrderBook']`` after
            loading markets.
        """
        key = (pair, depth)
        event_source = self._order_book_event_sources.get(key)
        if event_source is None:
            event_source = order_book.WatchOrderBookEventSource(
                self._cli, pair, depth, params=dict(kwargs)
            )
            self._order_book_event_sources[key] = event_source
        self._dispatcher.subscribe(event_source, cast(dispatcher.EventHandler, event_handler))

    def subscribe_to_private_order_events(
            self, pair: Pair, event_handler: OrderEventHandler, **kwargs: Any
    ):
        """Registers an async callable that will be called when your own orders are updated.

        Uses CCXT Pro's ``watchOrders`` websocket method.

        :param pair: The trading pair.
        :param event_handler: An async callable that receives an :class:`OrderEvent`.
        :param kwargs: Additional keyword arguments that will be forwarded to CCXT.

        .. note::

          * The target exchange must support ``watchOrders``. Check ``exchange.ccxt.has['watchOrders']`` after
            loading markets.
          * API credentials must be configured when creating the exchange.
        """
        event_source = self._order_event_sources.get(pair)
        if event_source is None:
            event_source = orders.WatchOrdersEventSource(self._cli, pair, params=dict(kwargs))
            self._order_event_sources[pair] = event_source
        self._dispatcher.subscribe(event_source, cast(dispatcher.EventHandler, event_handler))

    async def get_bid_ask(self, pair: Pair) -> Tuple[Decimal, Decimal]:
        """
        Returns the current best bid and ask prices.

        :param pair: The trading pair.
        """
        ticker = await self._cli.fetch_ticker(helpers.pair_to_symbol(pair))
        return helpers.to_decimal(ticker["bid"]), helpers.to_decimal(ticker["ask"])

    async def get_pair_info(self, pair: Pair) -> PairInfo:
        """
        Returns information about a trading pair.

        :param pair: The trading pair.
        """
        ret = self._pair_info_cache.get(pair)
        if ret is None:
            await self._cli.load_markets()
            market = self._cli.market(helpers.pair_to_symbol(pair))
            ret = helpers.pair_info_from_market(market, self._cli.precisionMode)
            self._pair_info_cache[pair] = ret
        return ret

    async def close(self):
        """Closes the underlying CCXT exchange."""
        await self._cli.close()