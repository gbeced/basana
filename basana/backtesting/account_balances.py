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
from typing import Dict, List
import itertools

from basana.backtesting import errors, value_map


class AccountBalances:
    def __init__(self, initial_balances: Dict[str, Decimal]):
        # Class invariants.
        # * balance >= 0
        # * hold >= 0
        # * borrowed >= 0
        # * hold <= balance

        self._balances = value_map.ValueMap({
            symbol: balance for symbol, balance in initial_balances.items() if balance >= 0
        })
        self._holds = value_map.ValueMap()
        self._borrowed = value_map.ValueMap({
            symbol: -balance for symbol, balance in initial_balances.items() if balance < 0
        })

    def update(
            self, balance_updates: Dict[str, Decimal] = {}, hold_updates: Dict[str, Decimal] = {},
            borrowed_updates: Dict[str, Decimal] = {}
    ):
        # Check balances first.
        symbols = set(itertools.chain(balance_updates.keys(), hold_updates.keys(), borrowed_updates.keys()))
        for symbol in symbols:
            balance = self._balances.get(symbol, Decimal(0))
            balance_update = balance_updates.get(symbol, Decimal(0))
            hold = self._holds.get(symbol, Decimal(0))
            hold_update = hold_updates.get(symbol, Decimal(0))
            borrowed = self._borrowed.get(symbol, Decimal(0))
            borrowed_update = borrowed_updates.get(symbol, Decimal(0))

            # Mantain class invariants.
            if (balance + balance_update) < Decimal(0):
                raise errors.NotEnoughBalance(f"Not enough {symbol} available", symbol, balance + balance_update)
            if (hold + hold_update) < Decimal(0):
                raise errors.Error(f"{symbol} hold update amount is invalid")
            if (borrowed + borrowed_update) < Decimal(0):
                raise errors.Error(f"{symbol} borrowed update amount is invalid")
            if (balance + balance_update) - (hold + hold_update) < Decimal(0):
                raise errors.NotEnoughBalance(
                    f"Not enough {symbol} available", symbol, (balance + balance_update) - (hold + hold_update)
                )

        # Update if no error ocurred.
        self._balances += balance_updates
        self._holds += hold_updates
        self._borrowed += borrowed_updates

    def get_symbols(self) -> List[str]:
        symbols = set(self._balances.keys())
        symbols.update(self._holds.keys())
        symbols.update(self._borrowed.keys())
        return list(symbols)

    def get_available_balance(self, symbol: str) -> Decimal:
        return self._balances.get(symbol, Decimal(0)) - self._holds.get(symbol, Decimal(0))

    def get_balance_on_hold(self, symbol: str) -> Decimal:
        return self._holds.get(symbol, Decimal(0))

    def get_borrowed_balance(self, symbol: str) -> Decimal:
        return self._borrowed.get(symbol, Decimal(0))
