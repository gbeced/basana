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
from typing import Dict, Generator, Generic, Iterable, List, Optional, Protocol, TypeVar
import itertools

from basana.core.enums import OrderOperation


ZERO = Decimal(0)


def add_amounts(lhs: Dict[str, Decimal], rhs: Dict[str, Decimal]) -> Dict[str, Decimal]:
    keys = set(itertools.chain(lhs.keys(), rhs.keys()))
    ret = {key: lhs.get(key, ZERO) + rhs.get(key, ZERO) for key in keys}
    return ret


def sub_amounts(lhs: Dict[str, Decimal], rhs: Dict[str, Decimal]) -> Dict[str, Decimal]:
    keys = set(itertools.chain(lhs.keys(), rhs.keys()))
    ret = {key: lhs.get(key, ZERO) - rhs.get(key, ZERO) for key in keys}
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


class ExchangeObjectProto(Protocol):
    @property
    def id(self) -> str:
        ...

    @property
    def is_open(self) -> bool:
        ...


TExchangeObject = TypeVar('TExchangeObject', bound=ExchangeObjectProto)


class ExchangeObjectContainer(Generic[TExchangeObject]):
    def __init__(self):
        self._items: Dict[str, TExchangeObject] = {}  # Items by id.
        self._open_items: List[TExchangeObject] = []
        self._reindex_every = 50
        self._reindex_counter = 0

    def add(self, item: TExchangeObject):
        assert item.id not in self._items
        self._items[item.id] = item
        if item.is_open:
            self._open_items.append(item)

    def get(self, id: str) -> Optional[TExchangeObject]:
        return self._items.get(id)

    def get_open(self) -> Generator[TExchangeObject, None, None]:
        self._reindex_counter += 1
        new_open_items: Optional[List[TExchangeObject]] = None
        if self._reindex_counter % self._reindex_every == 0:
            new_open_items = []

        for item in self._open_items:
            if item.is_open:
                yield item
                if new_open_items is not None and item.is_open:
                    new_open_items.append(item)

        if new_open_items is not None:
            self._open_items = new_open_items

    def get_all(self) -> Iterable[TExchangeObject]:
        return self._items.values()
