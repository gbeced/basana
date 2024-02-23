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
import copy
import itertools

from basana.backtesting import errors, helpers, lending, orders


class AccountBalances:
    def __init__(self, initial_balances: Dict[str, Decimal]):
        # Class invariants.
        # * balance >= 0
        # * hold >= 0
        # * borrowed >= 0
        # * hold <= balance

        self._balances: Dict[str, Decimal] = {
            symbol: balance for symbol, balance in initial_balances.items() if balance >= 0
        }
        self._holds: Dict[str, Decimal] = {}
        self._borrowed: Dict[str, Decimal] = {
            symbol: -balance for symbol, balance in initial_balances.items() if balance < 0
        }
        self._holds_by_order: Dict[str, Dict[str, Decimal]] = {}

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
        for symbol, update in balance_updates.items():
            self._balances[symbol] = self._balances.get(symbol, Decimal(0)) + update
        for symbol, update in hold_updates.items():
            self._holds[symbol] = self._holds.get(symbol, Decimal(0)) + update
        for symbol, update in borrowed_updates.items():
            self._borrowed[symbol] = self._borrowed.get(symbol, Decimal(0)) + update

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

    def get_balance_on_hold_for_order(self, order_id: str, symbol: str) -> Decimal:
        return self._holds_by_order.get(order_id, {}).get(symbol, Decimal(0))

    def order_accepted(self, order: orders.Order, required_balances: Dict[str, Decimal]):
        assert order.is_open, "The order is not open"
        assert order.id not in self._holds_by_order, "The order was already accepted"

        # When an order gets accepted we need to hold any required balance that will be debited as the order gets
        # filled.
        if required_balances:
            self.update(hold_updates=required_balances)
            self._holds_by_order[order.id] = copy.copy(required_balances)

    def order_updated(self, order: orders.Order, balance_updates: Dict[str, Decimal]):
        # If we have holds associated with the order, it may be time to release some/all of those.
        hold_updates = {}
        order_holds = self._holds_by_order.get(order.id, {})
        if order_holds:
            if order.is_open:
                hold_updates = {
                    symbol: max(amount, -order_holds.get(symbol, Decimal(0)))
                    for symbol, amount in balance_updates.items()
                    if amount < Decimal(0) and symbol in order_holds
                }
            else:
                hold_updates = {symbol: -amount for symbol, amount in order_holds.items()}

        # Update holds and balances.
        self.update(balance_updates=balance_updates, hold_updates=hold_updates)

        # Update holds by order.
        if order_holds:
            if order.is_open:
                for symbol, update in hold_updates.items():
                    order_holds[symbol] += update
                    assert order_holds[symbol] >= Decimal(0)
            else:
                del self._holds_by_order[order.id]

    def accept_loan(self, loan: lending.Loan):
        assert loan.is_open, "The loan is not open"

        self.update(
            balance_updates={loan.borrowed_symbol: loan.borrowed_amount},
            borrowed_updates={loan.borrowed_symbol: loan.borrowed_amount},
            hold_updates=loan.required_collateral
        )

    def repay_loan(self, loan: lending.Loan, interest: Dict[str, Decimal]):
        self.update(
            balance_updates=helpers.sub_amounts(
                {loan.borrowed_symbol: -loan.borrowed_amount}, interest
            ),
            borrowed_updates={loan.borrowed_symbol: -loan.borrowed_amount},
            hold_updates={symbol: -amount for symbol, amount in loan.required_collateral.items()}
        )
