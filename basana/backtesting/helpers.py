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
import itertools

from basana.core.enums import OrderOperation


ZERO = Decimal(0)


def add_amounts(lhs: Dict[str, Decimal], rhs: Dict[str, Decimal]) -> Dict[str, Decimal]:
    keys = set(itertools.chain(lhs.keys(), rhs.keys()))
    ret = {key: lhs.get(key, ZERO) + rhs.get(key, ZERO) for key in keys}
    return ret


def remove_empty_amounts(amounts: Dict[str, Decimal]) -> Dict[str, Decimal]:
    return {key: value for key, value in amounts.items() if value}


def copy_sign(x: Decimal, y: Decimal) -> Decimal:
    assert isinstance(x, Decimal)
    assert isinstance(y, Decimal)

    ret = x
    if x > ZERO and y < ZERO or x < ZERO and y > ZERO:
        ret = -x
    return ret


def get_sign(value: Decimal) -> Decimal:
    return copy_sign(Decimal(1), value)


def get_base_sign_for_operation(operation: OrderOperation) -> Decimal:
    if operation == OrderOperation.BUY:
        base_sign = Decimal(1)
    else:
        assert operation == OrderOperation.SELL
        base_sign = Decimal(-1)
    return base_sign
