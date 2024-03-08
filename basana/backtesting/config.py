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

from typing import Dict, Optional
import dataclasses

from basana.backtesting import errors
from basana.core.pair import Pair, PairInfo


@dataclasses.dataclass(frozen=True)
class SymbolInfo:
    precision: int


class Config:
    def __init__(self, default_symbol_info: Optional[SymbolInfo] = None, default_pair_info: Optional[PairInfo] = None):
        self._symbol_info: Dict[str, SymbolInfo] = {}
        self._default_symbol_info = default_symbol_info
        self._pair_info: Dict[Pair, PairInfo] = {}
        self._default_pair_info = default_pair_info

    def set_pair_info(self, pair: Pair, pair_info: PairInfo):
        self._pair_info[pair] = pair_info

    def get_pair_info(self, pair: Pair) -> PairInfo:
        ret = self._pair_info.get(pair, self._default_pair_info)
        if ret is None:
            raise errors.Error(f"No config for {pair}")
        return ret

    def set_symbol_info(self, symbol: str, symbol_info: SymbolInfo):
        self._symbol_info[symbol] = symbol_info

    def get_symbol_info(self, symbol: str) -> SymbolInfo:
        ret = self._symbol_info.get(symbol, self._default_symbol_info)
        if ret is None:
            raise errors.Error(f"No config for {symbol}")
        return ret
