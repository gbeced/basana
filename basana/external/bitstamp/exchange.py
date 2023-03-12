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

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

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
OrderBookEventHandler = Callable[[order_book.OrderBookEvent], Awaitable[Any]]
OrderEventHandler = Callable[[orders.OrderEvent], Awaitable[Any]]
TradeEventHandler = Callable[[trades.TradeEvent], Awaitable[Any]]


@enum.unique
class TransactionType(enum.IntEnum):
    DEPOSIT = 0
    WITHDRAWAL = 1
    MARKET_TRADE = 2


class OpenOrder:
    def __init__(self, json: dict):
        self.json = json

    @property
    def id(self) -> str:
        return str(self.json["id"])

    @property
    def datetime(self) -> datetime.datetime:
        return dt_parse(self.json["datetime"]).replace(tzinfo=datetime.timezone.utc)

    @property
    def operation(self) -> OrderOperation:
        return helpers.order_type_to_order_operation(int(self.json["type"]))

    @property
    def limit_price(self) -> Decimal:
        return Decimal(self.json["price"])

    @property
    def amount(self) -> Decimal:
        return Decimal(self.json["amount_at_create"])

    @property
    def amount_filled(self) -> Decimal:
        return Decimal(self.json["amount"])

    @property
    def pair(self) -> Pair:
        return Pair(*self.json["currency_pair"].split("/"))

    @property
    def client_order_id(self) -> Optional[str]:
        return self.json.get("client_order_id")


class OrderStatusTransaction:
    def __init__(self, json: dict):
        self.json = json

    @property
    def tid(self) -> str:
        return str(self.json["tid"])

    @property
    def price(self) -> Decimal:
        return Decimal(self.json["price"])

    @property
    def fee(self) -> Decimal:
        return Decimal(self.json["fee"])

    @property
    def type(self) -> TransactionType:
        return TransactionType(int(self.json["type"]))

    def __getattr__(self, item) -> Decimal:
        # Use for dynamic attributes like btc, usd, usdt, etc.
        ret = self.json.get(item)
        if ret is None:
            raise AttributeError("f{item} not found")
        return Decimal(ret)


class OrderStatus:
    def __init__(self, json: dict):
        self.json = json

    @property
    def id(self) -> str:
        return str(self.json["id"])

    @property
    def status(self) -> str:
        return self.json["status"]

    @property
    def amount_remaining(self) -> Decimal:
        return Decimal(self.json["amount_remaining"])

    @property
    def client_order_id(self) -> Optional[str]:
        return self.json.get("client_order_id")

    @property
    def transactions(self) -> List[OrderStatusTransaction]:
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
        return self._order_status.id

    @property
    def is_open(self) -> bool:
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
        return self._filled_base_amount

    @property
    def amount_remaining(self) -> Decimal:
        return self._order_status.amount_remaining

    @property
    def quote_amount_filled(self) -> Decimal:
        return self._quote_amount_filled

    @property
    def fill_price(self) -> Optional[Decimal]:
        fill_price = None
        if self.amount_filled:
            fill_price = self.quote_amount_filled / self.amount_filled
        return fill_price

    @property
    def fees(self) -> Dict[str, Decimal]:
        return self._fees


class Balance:
    def __init__(self, json: dict):
        self.json = json

    @property
    def available(self) -> Decimal:
        return Decimal(self.json["available"])

    @property
    def total(self) -> Decimal:
        return Decimal(self.json["total"])

    @property
    def reserved(self) -> Decimal:
        return Decimal(self.json["reserved"])


class CanceledOrder:
    def __init__(self, json: dict):
        self.json = json

    @property
    def id(self) -> str:
        return str(self.json["id"])

    @property
    def amount(self) -> Decimal:
        return Decimal(self.json["amount"])

    @property
    def limit_price(self) -> Decimal:
        return Decimal(self.json["price"])

    @property
    def operation(self) -> OrderOperation:
        return helpers.order_type_to_order_operation(int(self.json["type"]))


class CreatedOrder:
    def __init__(self, json: dict):
        self.json = json

    @property
    def id(self) -> str:
        return str(self.json["id"])

    @property
    def datetime(self) -> datetime.datetime:
        return dt_parse(self.json["datetime"]).replace(tzinfo=datetime.timezone.utc)

    @property
    def operation(self) -> OrderOperation:
        return helpers.order_type_to_order_operation(int(self.json["type"]))

    @property
    def price(self) -> Decimal:
        return Decimal(self.json["price"])

    @property
    def amount(self) -> Decimal:
        return Decimal(self.json["amount"])

    @property
    def client_order_id(self) -> Optional[str]:
        return self.json.get("client_order_id")


class RealTimeTradesToBar(bar.RealTimeTradesToBar):
    async def on_trade_event(self, trade_event: trades.TradeEvent):
        self.push_trade(trade_event.trade.datetime, trade_event.trade.price, trade_event.trade.amount)


class Exchange:
    def __init__(
            self, dispatcher: dispatcher.EventDispatcher, api_key: str, api_secret: str,
            session: Optional[aiohttp.ClientSession] = None, tb: Optional[token_bucket.TokenBucketLimiter] = None,
            config_overrides: dict = {}
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
        balance = await self._cli.get_account_balance(symbol.lower())
        return Balance(balance)

    async def get_bid_ask(self, pair: Pair) -> Tuple[Decimal, Decimal]:
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
        return await self.create_order(requests.MarketOrder(
            operation, pair, amount, client_order_id=client_order_id, **kwargs
        ))

    async def create_limit_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, limit_price: Decimal,
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> CreatedOrder:
        return await self.create_order(requests.LimitOrder(
            operation, pair, amount, limit_price, client_order_id=client_order_id, **kwargs
        ))

    async def create_instant_order(
            self, operation: OrderOperation, pair: Pair, amount: Decimal, amount_in_counter: bool = False,
            client_order_id: Optional[str] = None, **kwargs: Dict[str, Any]
    ) -> CreatedOrder:
        return await self.create_order(requests.InstantOrder(
            operation, pair, amount, amount_in_counter=amount_in_counter, client_order_id=client_order_id, **kwargs
        ))

    async def cancel_order(self, order_id: Union[str, int]) -> CanceledOrder:
        canceled_order = await self._cli.cancel_order(int(order_id))
        return CanceledOrder(canceled_order)

    async def get_order_info(
            self, pair: Pair, order_id: Optional[Union[str, int]] = None, client_order_id: Optional[str] = None,
    ) -> Optional[OrderInfo]:
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
        currency_pair = None if pair is None else helpers.pair_to_currency_pair(pair)
        open_orders = await self._cli.get_open_orders(currency_pair=currency_pair)
        return [OpenOrder(open_order) for open_order in open_orders]

    async def get_balances(self) -> Dict[str, Balance]:
        balances = await self._cli.get_account_balances()
        return {balance["currency"].upper(): Balance(balance) for balance in balances}

    def subscribe_to_bar_events(
            self, pair: Pair, bar_duration: int, event_handler: BarEventHandler, skip_first_bar: bool = True,
            flush_delay: float = 1
    ):
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
        channel = order_book.get_channel(pair)
        self._subscribe_to_ws_channel_events(
            channel, True, lambda ws_cli: order_book.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_public_order_events(self, pair: Pair, event_handler: OrderEventHandler):
        channel = orders.get_public_channel(pair)
        self._subscribe_to_ws_channel_events(
            channel, True, lambda ws_cli: orders.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_private_order_events(self, pair: Pair, event_handler: OrderEventHandler):
        channel = orders.get_private_channel(pair)
        self._subscribe_to_ws_channel_events(
            channel, False, lambda ws_cli: orders.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_public_trade_events(self, pair: Pair, event_handler: TradeEventHandler):
        channel = trades.get_public_channel(pair)
        self._subscribe_to_ws_channel_events(
            channel, True, lambda ws_cli: trades.WebSocketEventSource(pair, ws_cli),
            cast(dispatcher.EventHandler, event_handler)
        )

    def subscribe_to_private_trade_events(self, pair: Pair, event_handler: TradeEventHandler):
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
