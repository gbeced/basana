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
from typing import cast, Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union
import datetime
import enum

from dateutil.parser import parse as dt_parse
import aiohttp

from . import client, helpers, requests, order_book, orders, trades, websockets as bitstamp_ws
from basana.core import bar, dispatcher, event, token_bucket, websockets as core_ws
from basana.core.enums import OrderOperation
from basana.core.pair import Pair, PairInfo


BarEventHandler = Callable[[bar.BarEvent], Awaitable[Any]]
Error = client.Error
OrderBookEvent = order_book.OrderBookEvent
OrderBookEventHandler = Callable[[order_book.OrderBookEvent], Awaitable[Any]]
OrderEvent = orders.OrderEvent
OrderEventHandler = Callable[[orders.OrderEvent], Awaitable[Any]]
TradeEvent = trades.TradeEvent
TradeEventHandler = Callable[[trades.TradeEvent], Awaitable[Any]]


@enum.unique
class TransactionType(enum.IntEnum):
    """Enumeration for transaction types."""

    #:
    DEPOSIT = 0

    #:
    WITHDRAWAL = 1

    #:
    MARKET_TRADE = 2


class OpenOrder:
    def __init__(self, json: dict):
        self.json = json

    @property
    def id(self) -> str:
        """The order id."""
        return str(self.json["id"])

    @property
    def datetime(self) -> datetime.datetime:
        """The creation datetime."""
        return dt_parse(self.json["datetime"]).replace(tzinfo=datetime.timezone.utc)

    @property
    def operation(self) -> OrderOperation:
        """The operation."""
        return helpers.order_type_to_order_operation(int(self.json["type"]))

    @property
    def limit_price(self) -> Decimal:
        """The limit price."""
        return Decimal(self.json["price"])

    @property
    def amount(self) -> Decimal:
        """The amount."""
        return Decimal(self.json["amount_at_create"])

    @property
    def amount_filled(self) -> Decimal:
        """The amount filled."""
        return Decimal(self.json["amount"])

    @property
    def pair(self) -> Pair:
        """The trading pair."""
        return Pair(*self.json["currency_pair"].split("/"))

    @property
    def client_order_id(self) -> Optional[str]:
        """The client order id."""
        return self.json.get("client_order_id")


class OrderStatusTransaction:
    def __init__(self, json: dict):
        self.json = json

    @property
    def tid(self) -> str:
        """The transaction id."""
        return str(self.json["tid"])

    @property
    def price(self) -> Decimal:
        """The price."""
        return Decimal(self.json["price"])

    @property
    def fee(self) -> Decimal:
        """The fee."""
        return Decimal(self.json["fee"])

    @property
    def type(self) -> TransactionType:
        """The transaction type."""
        return TransactionType(int(self.json["type"]))

    def __getattr__(self, item) -> Decimal:
        """Use for numeric, dynamic attributes, like btc, usd, usdt, etc."""
        ret = self.json.get(item)
        if ret is None:
            raise AttributeError("f{item} not found")
        return Decimal(ret)


class OrderStatus:
    def __init__(self, json: dict):
        self.json = json

    @property
    def id(self) -> str:
        """The order id."""
        return str(self.json["id"])

    @property
    def status(self) -> str:
        """The status. One of Open, Finished, Expired or Canceled"""
        return self.json["status"]

    @property
    def amount_remaining(self) -> Decimal:
        """The amount remaining to be filled."""
        return Decimal(self.json["amount_remaining"])

    @property
    def client_order_id(self) -> Optional[str]:
        """The client order id."""
        return self.json.get("client_order_id")

    @property
    def transactions(self) -> List[OrderStatusTransaction]:
        """The transactions.

        Check https://www.bitstamp.net/api/#order-status.
        """
        return [OrderStatusTransaction(tx) for tx in self.json.get("transactions", [])]


class OrderInfo:
    def __init__(self, pair: Pair, order_status: OrderStatus):
        self._pair = pair
        self._order_status = order_status

        fees = Decimal(0)
        self._filled_base_amount = Decimal(0)
        self._quote_amount_filled = Decimal(0)
        base_currency = pair.base_symbol.lower()
        quote_currency = pair.quote_symbol.lower()
        for tx in order_status.transactions:
            fees += tx.fee
            self._filled_base_amount += getattr(tx, base_currency)
            self._quote_amount_filled += getattr(tx, quote_currency)
        self._fees = {pair.quote_symbol: fees} if fees else {}

    @property
    def id(self) -> str:
        """The order id."""
        return self._order_status.id

    @property
    def is_open(self) -> bool:
        """True if the order is open, False otherwise."""
        # Not doing `order_status.status == "Open"` to detect unkown states.
        is_open = {
            "Open": True,
            "Finished": False,
            "Expired": False,
            "Canceled": False,
        }.get(self._order_status.status)
        assert is_open is not None, "No mapping for {} order status".format(
            self._order_status.status
        )
        return is_open

    @property
    def amount_filled(self) -> Decimal:
        """The amount filled."""
        return self._filled_base_amount

    @property
    def amount_remaining(self) -> Decimal:
        """The amount remaining to be filled."""
        return self._order_status.amount_remaining

    @property
    def quote_amount_filled(self) -> Decimal:
        """The amount filled in quote units."""
        return self._quote_amount_filled

    @property
    def fill_price(self) -> Optional[Decimal]:
        """The fill price."""
        fill_price = None
        if self.amount_filled:
            fill_price = self.quote_amount_filled / self.amount_filled
        return fill_price

    @property
    def fees(self) -> Dict[str, Decimal]:
        """The fees."""
        return self._fees


class Balance:
    def __init__(self, json: dict):
        self.json = json

    @property
    def available(self) -> Decimal:
        """The available balance."""
        return Decimal(self.json["available"])

    @property
    def total(self) -> Decimal:
        """The total balance (available + reserved)."""
        return Decimal(self.json["total"])

    @property
    def reserved(self) -> Decimal:
        """The reserved balance."""
        return Decimal(self.json["reserved"])


class CanceledOrder:
    def __init__(self, json: dict):
        self.json = json

    @property
    def id(self) -> str:
        """The order id."""
        return str(self.json["id"])

    @property
    def amount(self) -> Decimal:
        """The amount."""
        return Decimal(self.json["amount"])

    @property
    def limit_price(self) -> Decimal:
        """The limit price."""
        return Decimal(self.json["price"])

    @property
    def operation(self) -> OrderOperation:
        """The operation."""
        return helpers.order_type_to_order_operation(int(self.json["type"]))


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
        return dt_parse(self.json["datetime"]).replace(tzinfo=datetime.timezone.utc)

    @property
    def operation(self) -> OrderOperation:
        """The operation."""
        return helpers.order_type_to_order_operation(int(self.json["type"]))

    @property
    def price(self) -> Decimal:
        """The price."""
        return Decimal(self.json["price"])

    @property
    def amount(self) -> Decimal:
        """The amount."""
        return Decimal(self.json["amount"])

    @property
    def client_order_id(self) -> Optional[str]:
        """The client order id."""
        return self.json.get("client_order_id")


class RealTimeTradesToBar(bar.RealTimeTradesToBar):
    async def on_trade_event(self, trade_event: trades.TradeEvent):
        self.push_trade(trade_event.trade.datetime, trade_event.trade.price, trade_event.trade.amount)


class Exchange:
    """A client for `Bitstamp <https://www.bitstamp.net//>`_ crypto currency exchange.

    :param dispatcher: The event dispatcher.
    :param api_key: An optional api key. If not set only public endpoints can be used.
    :param api_secret: An optional api secret. If not set only public endpoints can be used.
    :param session: An optional client session, in case you want to reuse connections.
    :type session: aiohttp.ClientSession
    :param tb: An optional token bucket limiter, in case you want to throttle requests.
    :param config_overrides: An optional dictionary for overriding config settings.
    """
    def __init__(
            self, dispatcher: dispatcher.EventDispatcher, api_key: Optional[str] = None,
            api_secret: Optional[str] = None, session: Optional[aiohttp.ClientSession] = None,
            tb: Optional[token_bucket.TokenBucketLimiter] = None, config_overrides: dict = {}
    ):
        self._dispatcher = dispatcher
        self._api_key = api_key
        self._api_secret = api_secret
        self._cli = client.APIClient(api_key, api_secret, session=session, tb=tb, config_overrides=config_overrides)
        self._session = session
        self._tb = tb
        self._config_overrides = config_overrides
        self._pub_websocket: Optional[bitstamp_ws.PublicWebSocketClient] = None
        self._priv_websocket: Optional[bitstamp_ws.PrivateWebSocketClient] = None
        self._channel_to_event_source: Dict[str, event.EventSource] = {}
        self._pair_info_cache: Dict[Pair, PairInfo] = {}
        self._bar_event_source: Dict[Tuple[Pair, int, bool, float], RealTimeTradesToBar] = {}

    async def get_balance(self, symbol: str) -> Balance:
        """Returns the balance for a specific currency/symbol/etc..

        :param symbol: The currency/symbol/etc..
        """
        balance = await self._cli.get_account_balance(symbol.lower())
        return Balance(balance)

    async def get_bid_ask(self, pair: Pair) -> Tuple[Decimal, Decimal]:
        """Returns the current bid and ask price.

        :param pair: The trading pair.
        """
        ticker = await self._cli.get_ticker(helpers.pair_to_currency_pair(pair))
        return Decimal(ticker["bid"]), Decimal(ticker["ask"])

    async def fetch_pair_info(self):
        # Fetch trading pairs and fill the cache.
        bs_pairs_info = await self._cli.get_trading_pairs_info()
        for bs_pair_info in bs_pairs_info:
            pair = Pair(*bs_pair_info["name"].split("/"))
            pair_info = PairInfo(int(bs_pair_info["base_decimals"]), int(bs_pair_info["counter_decimals"]))
            self._pair_info_cache[pair] = pair_info

    async def get_pair_info(self, pair: Pair) -> PairInfo:
        """Returns information about a trading pair.

        :param pair: The trading pair.
        """
        if not self._pair_info_cache:
            await self.fetch_pair_info()
        return self._pair_info_cache[pair]

    async def create_order(self, order_request: requests.ExchangeOrder) -> CreatedOrder:
        created_order = await order_request.create_order(self._cli)
        return CreatedOrder(created_order)

    async def create_market_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, client_order_id: Optional[str] = None,
            **kwargs: Dict[str, Any]
    ) -> CreatedOrder:
        """Creates a market order.

        If the order can't be created a :class:`basana.external.bitstamp.exchange.Error` will be raised.

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
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> CreatedOrder:
        """Creates a limit order.

        If the order can't be created a :class:`basana.external.bitstamp.exchange.Error` will be raised.

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

    async def create_instant_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, amount_in_counter: bool = False,
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> CreatedOrder:
        """Creates an instant order.

        If the order can't be created a :class:`basana.external.bitstamp.exchange.Error` will be raised.

        :param operation: The order operation.
        :param pair: The pair to trade.
        :param amount: The amount to buy/sell.
        :param amount_in_counter: False if the amount is set in base currency (default), True if the amount is set in
                                  quote currency.
        :param client_order_id: A client order id.
        :param kwargs: Additional keyword arguments that will be forwarded.
        """
        return await self.create_order(requests.InstantOrder(
            operation, pair, amount, amount_in_counter=amount_in_counter, client_order_id=client_order_id, **kwargs
        ))

    async def cancel_order(self, order_id: Union[str, int]) -> CanceledOrder:
        """Cancels an order.

        If the order can't be canceled a :class:`basana.external.bitstamp.exchange.Error` will be raised.

        :param order_id: The order id.
        """
        canceled_order = await self._cli.cancel_order(int(order_id))
        return CanceledOrder(canceled_order)

    async def get_order_info(
            self, pair: Pair, order_id: Optional[Union[str, int]] = None, client_order_id: Optional[str] = None,
    ) -> Optional[OrderInfo]:
        """Returns information about an order.

        :param pair: The trading pair.
        :param order_id: The order id.
        :param client_order_id: The client order id.

        .. note::

          * Either order_id or client_order_id should be set, but not both.
          * For closed orders, this call only returns information for the last 30 days.
        """
        ret = None
        order_status = await self.get_order_status(order_id=order_id, client_order_id=client_order_id)
        if order_status:
            ret = OrderInfo(pair, order_status)
        return ret

    async def get_order_status(
            self, order_id: Optional[Union[str, int]] = None, client_order_id: Optional[str] = None,
            omit_transactions: Optional[bool] = None
    ) -> Optional[OrderStatus]:
        if order_id is not None:
            order_id = int(order_id)

        ret = None
        try:
            order_status = await self._cli.get_order_status(
                id=order_id, client_order_id=client_order_id, omit_transactions=omit_transactions
            )
            ret = OrderStatus(order_status)
        except client.Error as e:
            if e.reason != "Order not found.":
                raise
        return ret

    async def get_open_orders(self, pair: Optional[Pair] = None) -> List[OpenOrder]:
        """Returns open orders.

        :param pair: If set, only open orders matching this pair will be returned, otherwise all open orders will be
            returned.
        """
        currency_pair = None if pair is None else helpers.pair_to_currency_pair(pair)
        open_orders = await self._cli.get_open_orders(currency_pair=currency_pair)
        return [OpenOrder(open_order) for open_order in open_orders]

    async def get_balances(self) -> Dict[str, Balance]:
        """Returns all balances."""
        balances = await self._cli.get_account_balances()
        return {balance["currency"].upper(): Balance(balance) for balance in balances}

    def subscribe_to_bar_events(
            self, pair: Pair, bar_duration: int, event_handler: BarEventHandler, skip_first_bar: bool = True,
            flush_delay: float = 1
    ):
        """Registers an async callable that will be called when a new bar is available.


        :param pair: The trading pair.
        :param bar_duration: The bar duration in seconds.
        :param event_handler: An async callable that receives BarEvent.
        :param skip_first_bar: True if the first bar should be skipped. This is to avoid receiving incomplete bars
                               if subscription takes place in the middle of the period.
        :param flush_delay: The number of seconds to wait before generating the new bar, in case trades are delayed.

        .. note::

          * Under the hood, this will subscribe to trade events and will aggregate those into bars.
        """
        key = (pair, bar_duration, skip_first_bar, flush_delay)
        event_source = self._bar_event_source.get(key)
        if not event_source:
            event_source = RealTimeTradesToBar(
                pair, bar_duration, skip_first_bar=skip_first_bar, flush_delay=flush_delay
            )
            self.subscribe_to_public_trade_events(pair, event_source.on_trade_event)
            self._bar_event_source[key] = event_source
        self._dispatcher.subscribe(event_source, cast(dispatcher.EventHandler, event_handler))

    def subscribe_to_order_book_events(self, pair: Pair, event_handler: OrderBookEventHandler):
        """Registers an async callable that will be called when the order book is updated.

        :param pair: The trading pair.
        :param event_handler: An async callable that receives an OrderBookEvent.

        .. note::

          * Only the top 100 bids and asks are returned in the event.
        """
        channel = order_book.get_channel(pair)
        self._subscribe_to_ws_channel_events(
            channel, True, lambda ws_cli: order_book.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_public_order_events(self, pair: Pair, event_handler: OrderEventHandler):
        """Registers an async callable that will be called when any order is updated.

        :param pair: The trading pair.
        :param event_handler: An async callable that receives an OrderEvent.
        """
        channel = orders.get_public_channel(pair)
        self._subscribe_to_ws_channel_events(
            channel, True, lambda ws_cli: orders.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_private_order_events(self, pair: Pair, event_handler: OrderEventHandler):
        """Registers an async callable that will be called when your own orders are updated.

        :param pair: The trading pair.
        :param event_handler: An async callable that receives an OrderEvent.
        """
        channel = orders.get_private_channel(pair)
        self._subscribe_to_ws_channel_events(
            channel, False, lambda ws_cli: orders.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_public_trade_events(self, pair: Pair, event_handler: TradeEventHandler):
        """Registers an async callable that will be called for any new trade.

        :param pair: The trading pair.
        :param event_handler: An async callable that receives a TradeEvent.
        """
        channel = trades.get_public_channel(pair)
        self._subscribe_to_ws_channel_events(
            channel, True, lambda ws_cli: trades.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_private_trade_events(self, pair: Pair, event_handler: TradeEventHandler):
        """Registers an async callable that will be called for your own new trades.

        :param pair: The trading pair.
        :param event_handler: An async callable that receives a TradeEvent.
        """
        channel = trades.get_private_channel(pair)
        self._subscribe_to_ws_channel_events(
            channel, False, lambda ws_cli: trades.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def _subscribe_to_ws_channel_events(
            self, channel: str, is_public: bool,
            event_src_factory: Callable[[core_ws.WebSocketClient], core_ws.ChannelEventSource],
            event_handler: dispatcher.EventHandler
    ):
        # Get/create the event source for the channel.
        ws_cli = self._get_pub_ws_client() if is_public else self._get_priv_ws_client()
        event_source = ws_cli.get_channel_event_source(channel)
        if not event_source:
            event_source = event_src_factory(ws_cli)
            ws_cli.set_channel_event_source(channel, event_source)

        # Subscribe the event handler to the event source.
        self._dispatcher.subscribe(event_source, event_handler)

    def _get_pub_ws_client(self) -> bitstamp_ws.WebSocketClient:
        if self._pub_websocket is None:
            self._pub_websocket = bitstamp_ws.PublicWebSocketClient(
                session=self._session, config_overrides=self._config_overrides
            )
        return self._pub_websocket

    def _get_priv_ws_client(self) -> bitstamp_ws.WebSocketClient:
        assert self._api_key and self._api_secret, "Missing API key and/or secret"

        if self._priv_websocket is None:
            self._priv_websocket = bitstamp_ws.PrivateWebSocketClient(
                self._api_key, self._api_secret, session=self._session, config_overrides=self._config_overrides
            )
        return self._priv_websocket
