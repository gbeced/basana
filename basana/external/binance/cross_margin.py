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

from . import margin, user_data, websocket_mgr
from .client import margin as margin_client


# Forward declarations
OrderEvent = user_data.OrderEvent
OrderEventHandler = user_data.OrderEventHandler
OrderUpdate = user_data.OrderUpdate
UserDataEvent = user_data.Event
UserDataEventHandler = user_data.UserDataEventHandler


class Account(margin.Account):
    """Cross margin account."""
    def __init__(self, cli: margin_client.CrossMarginAccount, ws_mgr: websocket_mgr.WebsocketManager):
        self._cli = cli
        self._ws_mgr = ws_mgr

    @property
    def client(self) -> margin_client.CrossMarginAccount:
        return self._cli

    async def get_balances(self) -> Dict[str, margin.Balance]:
        """Returns all balances."""
        account_info = await self.client.get_account_information()
        return {balance["asset"].upper(): margin.Balance(balance) for balance in account_info["userAssets"]}

    async def transfer_from_spot_account(self, asset: str, amount: Decimal) -> dict:
        """Transfer balances from the spot account to the cross margin account.

        If the transfer can't be completed a :class:`basana.external.binance.exchange.Error` will be raised.

        :param asset: The asset to transfer.
        :param amount: The amount to transfer.
        """
        return await self.client.transfer_from_spot_account(asset, amount)

    async def transfer_to_spot_account(self, asset: str, amount: Decimal) -> dict:
        """Transfer balances from the cross margin account to the spot account.

        If the transfer can't be completed a :class:`basana.external.binance.exchange.Error` will be raised.

        :param asset: The asset to transfer.
        :param amount: The amount to transfer.
        """
        return await self.client.transfer_to_spot_account(asset, amount)

    def subscribe_to_user_data_events(self, event_handler: UserDataEventHandler):
        """
        Registers an async callable that will be called for every new user data event.

        Works as defined in https://developers.binance.com/docs/margin_trading/trade-data-stream.

        :param event_handler: The event handler.
        """

        self._ws_mgr.subscribe_to_cross_margin_user_data_events(event_handler)

    def subscribe_to_order_events(self, event_handler: OrderEventHandler):
        """
        Registers an async callable that will be called for every new order update.

        Works as defined in https://developers.binance.com/docs/margin_trading/trade-data-stream/Event-Order-Update.

        :param event_handler: The event handler.
        """

        self._ws_mgr.subscribe_to_cross_margin_order_events(event_handler)
