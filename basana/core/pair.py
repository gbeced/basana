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

import dataclasses


@dataclasses.dataclass(frozen=True)
class Pair:
    """A trading pair.

    :param base_symbol: The base symbol. It could be a stock, a crypto currency, a currency, etc.
    :param quote_symbol: The quote symbol. It could be a stock, a crypto currency, a currency, etc.
    """

    #: The base symbol.
    base_symbol: str

    #: The quote symbol.
    quote_symbol: str

    def __str__(self):
        return "{}/{}".format(self.base_symbol, self.quote_symbol)


@dataclasses.dataclass(frozen=True)
class PairInfo:
    """Information about a trading pair.

    :param base_precision: The precision for the base symbol.
    :param quote_precision: The precision for the quote symbol.
    """

    #: The precision for the base symbol.
    base_precision: int

    #: The precision for the quote symbol.
    quote_precision: int
