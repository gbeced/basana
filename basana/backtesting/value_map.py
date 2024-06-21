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
import itertools


from basana.backtesting import config
from basana.core import helpers


ZERO = Decimal(0)
ValueMapDict = Dict[str, Decimal]


class ValueMap(ValueMapDict):
    def prune(self):
        keys = [key for key, value in self.items() if not value]
        for key in keys:
            del self[key]

    def truncate(self, config: config.Config):
        for symbol, amount in self.items():
            symbol_info = config.get_symbol_info(symbol)
            self[symbol] = helpers.truncate_decimal(amount, symbol_info.precision)

    def __add__(self, other: ValueMapDict) -> "ValueMap":
        keys = set(itertools.chain(self.keys(), other.keys()))
        return ValueMap({key: self.get(key, ZERO) + other.get(key, ZERO) for key in keys})

    def __iadd__(self, other: ValueMapDict) -> "ValueMap":
        keys = set(itertools.chain(self.keys(), other.keys()))
        for key in keys:
            self[key] = self.get(key, ZERO) + other.get(key, ZERO)
        return self

    def __radd__(self, other: ValueMapDict) -> "ValueMap":
        return self.__add__(other)

    def __sub__(self, other: ValueMapDict) -> "ValueMap":
        keys = set(itertools.chain(self.keys(), other.keys()))
        return ValueMap({key: self.get(key, ZERO) - other.get(key, ZERO) for key in keys})

    def __isub__(self, other: ValueMapDict) -> "ValueMap":
        keys = set(itertools.chain(self.keys(), other.keys()))
        for key in keys:
            self[key] = self.get(key, ZERO) - other.get(key, ZERO)
        return self

    def __rsub__(self, other: ValueMapDict) -> "ValueMap":
        keys = set(itertools.chain(self.keys(), other.keys()))
        return ValueMap({key: other.get(key, ZERO) - self.get(key, ZERO) for key in keys})

    def __mul__(self, other: ValueMapDict) -> "ValueMap":
        keys = set(itertools.chain(self.keys(), other.keys()))
        ret = ValueMap()
        for key in keys:
            ret[key] = self.get(key, ZERO) * other.get(key, ZERO)
        return ret

    def __imul__(self, other: ValueMapDict) -> "ValueMap":
        keys = set(itertools.chain(self.keys(), other.keys()))
        for key in keys:
            self[key] = self.get(key, ZERO) * other.get(key, ZERO)
        return self

    def __rmul__(self, other: ValueMapDict) -> "ValueMap":
        return self.__mul__(other)
