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
from typing import List
import abc
import itertools

from basana.backtesting import errors
from basana.backtesting.value_map import ValueMap, ValueMapDict


class UpdateRule(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def check(self, updated_balances: ValueMap, updated_holds: ValueMap, updated_borrowed: ValueMap):
        raise NotImplementedError()


class NonZero(UpdateRule):
    def check(self, updated_balances: ValueMap, updated_holds: ValueMap, updated_borrowed: ValueMap):
        # balance >= 0
        for symbol, value in updated_balances.items():
            if value < Decimal(0):
                raise errors.NotEnoughBalance(f"Not enough {symbol} available")
        # hold >= 0
        for symbol, value in updated_holds.items():
            if value < Decimal(0):
                raise errors.Error(f"hold update amount for {symbol} is invalid")
        # borrowed >= 0
        for symbol, value in updated_borrowed.items():
            if value < Decimal(0):
                raise errors.Error(f"borrowed update amount for {symbol} is invalid")


class ValidHold(UpdateRule):
    # * hold <= balance
    def check(self, updated_balances: ValueMap, updated_holds: ValueMap, updated_borrowed: ValueMap):
        symbols = set(itertools.chain(updated_holds.keys(), updated_balances.keys()))
        for symbol in symbols:
            updated_hold = updated_holds.get(symbol, Decimal(0))
            updated_balance = updated_balances.get(symbol, Decimal(0))
            if updated_hold > updated_balance:
                raise errors.NotEnoughBalance(f"Not enough {symbol} available to hold")


class AccountBalances:
    def __init__(self, initial_balances: ValueMapDict):
        self.balances = ValueMap({
            symbol: balance for symbol, balance in initial_balances.items() if balance >= 0
        })
        self.holds = ValueMap()
        self.borrowed = ValueMap({
            symbol: -balance for symbol, balance in initial_balances.items() if balance < 0
        })
        self._update_rules: List[UpdateRule] = [
            NonZero(),
            ValidHold()
        ]

    def push_update_rule(self, update_rule: UpdateRule):
        self._update_rules.append(update_rule)

    def update(
            self, balance_updates: ValueMapDict = {}, hold_updates: ValueMapDict = {},
            borrowed_updates: ValueMapDict = {}
    ):
        updated_balances = self.balances + balance_updates
        updated_holds = self.holds + hold_updates
        updated_borrowed = self.borrowed + borrowed_updates

        for rule in self._update_rules:
            rule.check(updated_balances, updated_holds, updated_borrowed)

        # Update if no error ocurred.
        self.balances = updated_balances
        self.holds = updated_holds
        self.borrowed = updated_borrowed

    def get_symbols(self) -> List[str]:
        symbols = set(self.balances.keys())
        symbols.update(self.holds.keys())
        symbols.update(self.borrowed.keys())
        return list(symbols)

    def get_available_balance(self, symbol: str) -> Decimal:
        return self.balances.get(symbol, Decimal(0)) - self.holds.get(symbol, Decimal(0))

    def get_balance_on_hold(self, symbol: str) -> Decimal:
        return self.holds.get(symbol, Decimal(0))

    def get_borrowed_balance(self, symbol: str) -> Decimal:
        return self.borrowed.get(symbol, Decimal(0))
