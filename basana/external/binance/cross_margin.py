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
from typing import Dict

from . import margin
from .client import margin as margin_client


class Account(margin.Account):
    """Cross margin account."""
    def __init__(self, cli: margin_client.CrossMarginAccount):
        self._cli = cli

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
