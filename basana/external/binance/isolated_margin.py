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
from typing import Dict

from . import helpers, margin, user_data, websocket_mgr
from .client import margin as margin_client
from basana.core.pair import Pair


# Forward declarations
OrderEvent = user_data.OrderEvent
OrderEventHandler = user_data.OrderEventHandler
OrderUpdate = user_data.OrderUpdate
UserDataEvent = user_data.Event
UserDataEventHandler = user_data.UserDataEventHandler


class IsolatedBalance:
    def __init__(self, json: dict):
        self.json = json

    @property
    def base_asset(self) -> str:
        """The base asset."""
        return self.json["baseAsset"]["asset"]

    @property
    def base_asset_balance(self) -> margin.Balance:
        """The base asset balance."""
        return margin.Balance(self.json["baseAsset"])

    @property
    def quote_asset(self) -> str:
        """The quote asset."""
        return self.json["quoteAsset"]["asset"]

    @property
    def quote_asset_balance(self) -> margin.Balance:
        """The quote asset balance."""
        return margin.Balance(self.json["quoteAsset"])


class Account(margin.Account):
    """Isolated margin account."""
    def __init__(self, cli: margin_client.IsolatedMarginAccount, ws_mgr: websocket_mgr.WebsocketManager):
        self._cli = cli
        self._ws_mgr = ws_mgr

    @property
    def client(self) -> margin_client.IsolatedMarginAccount:
        return self._cli

    async def get_balances(self) -> Dict[Pair, IsolatedBalance]:
        """Returns all balances."""
        account_info = await self.client.get_account_information()
        ret = {}
        for isolated_balance in account_info["assets"]:
            isolated_balance = IsolatedBalance(isolated_balance)
            pair = Pair(isolated_balance.base_asset, isolated_balance.quote_asset)
            ret[pair] = isolated_balance
        return ret

    async def transfer_from_spot_account(self, asset: str, pair: Pair, amount: Decimal) -> dict:
        """Transfer balances from the spot account to the isolated margin account.

        If the transfer can't be completed a :class:`basana.external.binance.exchange.Error` will be raised.

        :param asset: The asset to transfer.
        :param pair: The trading pair.
        :param amount: The amount to transfer.
        """
        return await self.client.transfer_from_spot_account(asset, helpers.pair_to_order_book_symbol(pair), amount)

    async def transfer_to_spot_account(self, asset: str, pair: Pair, amount: Decimal) -> dict:
        """Transfer balances from the isolated margin account to the spot account.

        If the transfer can't be completed a :class:`basana.external.binance.exchange.Error` will be raised.

        :param asset: The asset to transfer.
        :param pair: The trading pair.
        :param amount: The amount to transfer.
        """
        return await self.client.transfer_to_spot_account(asset, helpers.pair_to_order_book_symbol(pair), amount)

    def subscribe_to_user_data_events(self, pair: Pair, event_handler: UserDataEventHandler):
        """
        Registers an async callable that will be called for every new user data event.

        Works as defined in https://developers.binance.com/docs/margin_trading/trade-data-stream.

        :param pair: The trading pair.
        :param event_handler: The event handler.
        """

        self._ws_mgr.subscribe_to_isolated_margin_user_data_events(pair, event_handler)

    def subscribe_to_order_events(self, pair: Pair, event_handler: OrderEventHandler):
        """
        Registers an async callable that will be called for every new order update.

        Works as defined in https://developers.binance.com/docs/margin_trading/trade-data-stream/Event-Order-Update.

        :param pair: The trading pair.
        :param event_handler: The event handler.
        """

        self._ws_mgr.subscribe_to_isolated_margin_order_events(pair, event_handler)
