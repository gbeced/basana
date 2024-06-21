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
from typing import Dict, List, Optional
import copy

from basana.backtesting import errors, helpers as bt_helpers
from basana.backtesting.value_map import ValueMap
from basana.backtesting.lending import base as lending_base


class LoanManager:
    def __init__(
            self, lending_strategy: lending_base.LendingStrategy, exchange_ctx: lending_base.ExchangeContext
    ):
        self._loans = bt_helpers.ExchangeObjectContainer[lending_base.Loan]()
        self._ctx = exchange_ctx
        self._lending_strategy = lending_strategy
        self._collateral_by_loan: Dict[str, ValueMap] = {}
        self._lending_strategy.set_exchange_context(self, exchange_ctx)

    def create_loan(self, symbol: str, amount: Decimal) -> lending_base.LoanInfo:
        if amount <= 0:
            raise errors.Error("Invalid amount")

        # Create the loan and update balances.
        loan = self._lending_strategy.create_loan(symbol, amount, self._ctx.dispatcher.now())
        required_collateral = loan.calculate_collateral(self._ctx.prices)
        self._ctx.account_balances.update(
            balance_updates={loan.borrowed_symbol: loan.borrowed_amount},
            borrowed_updates={loan.borrowed_symbol: loan.borrowed_amount},
            hold_updates=required_collateral
        )

        # Save the loan now that balance updates succeeded.
        self._loans.add(loan)
        self._collateral_by_loan[loan.id] = ValueMap(required_collateral)

        return self._build_loan_info(loan)

    def get_loans(
            self, borrowed_symbol: Optional[str] = None, is_open: Optional[bool] = None
    ) -> List[lending_base.LoanInfo]:
        loans = self._loans.get_all()
        if borrowed_symbol:
            loans = filter(lambda loan: loan.borrowed_symbol == borrowed_symbol, loans)
        if is_open is not None:
            loans = filter(lambda loan: loan.is_open == is_open, loans)
        return [self._build_loan_info(loan) for loan in loans]

    def get_loan(self, loan_id: str) -> Optional[lending_base.LoanInfo]:
        loan = self._loans.get(loan_id)
        return None if loan is None else self._build_loan_info(loan)

    def repay_loan(self, loan_id: str):
        loan = self._get_open_loan(loan_id)

        interest = ValueMap()
        interest += loan.calculate_interest(self._ctx.dispatcher.now(), self._ctx.prices)
        interest.truncate(self._ctx.config)
        interest.prune()
        collateral = self._collateral_by_loan[loan_id]

        # Update balances.
        balance_updates = ValueMap({loan.borrowed_symbol: -loan.borrowed_amount})
        balance_updates -= interest
        self._ctx.account_balances.update(
            balance_updates=balance_updates,
            borrowed_updates={loan.borrowed_symbol: -loan.borrowed_amount},
            hold_updates={symbol: -amount for symbol, amount in collateral.items()}
        )

        # Close the loan now that balance updates succeeded.
        loan.add_paid_interest(interest)
        loan.close()
        self._collateral_by_loan.pop(loan_id)

    def cancel_loan(self, loan_id: str):
        loan = self._get_open_loan(loan_id)

        # Only loans that have just been created can be canceled
        assert loan.created_at == self._ctx.dispatcher.now()

        # Update balances.
        collateral = self._collateral_by_loan[loan_id]
        balance_updates = ValueMap({loan.borrowed_symbol: -loan.borrowed_amount})
        self._ctx.account_balances.update(
            balance_updates=balance_updates,
            borrowed_updates=balance_updates,
            hold_updates={symbol: -amount for symbol, amount in collateral.items()}
        )

        # Close the loan now that balance updates succeeded.
        loan.close()
        self._collateral_by_loan.pop(loan_id)

    def _get_open_loan(self, loan_id: str) -> lending_base.Loan:
        loan = self._loans.get(loan_id)
        if not loan:
            raise errors.NotFound("Loan not found")
        if not loan.is_open:
            raise errors.Error("Loan is not open")
        return loan

    def _build_loan_info(self, loan: lending_base.Loan) -> lending_base.LoanInfo:
        outstanding_interest = ValueMap()
        if loan.is_open:
            outstanding_interest += loan.calculate_interest(
                self._ctx.dispatcher.now(), self._ctx.prices
            )
            outstanding_interest.truncate(self._ctx.config)
            outstanding_interest.prune()

        return lending_base.LoanInfo(
            id=loan.id, is_open=loan.is_open, borrowed_symbol=loan.borrowed_symbol,
            borrowed_amount=loan.borrowed_amount, outstanding_interest=outstanding_interest,
            paid_interest=copy.copy(loan.paid_interest)
        )
