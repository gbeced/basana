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

import pytest

from basana.backtesting.config import Config, SymbolInfo
from basana.core.pair import Pair, PairInfo


@pytest.fixture
def config():
    ret = Config()
    ret.set_symbol_info("USD", SymbolInfo(precision=2))
    ret.set_pair_info(Pair("BTC", "USD"), PairInfo(base_precision=8, quote_precision=2))
    return ret


def test_pair_info(config):
    pair_info = config.get_pair_info(Pair("BTC", "USD"))
    assert pair_info.base_precision == 8
    with pytest.raises(Exception, match="No config"):
        config.get_pair_info(Pair("BTC", "USDT"))


def test_symbol_info(config):
    symbol_info = config.get_symbol_info("USD")
    assert symbol_info.precision == 2
    with pytest.raises(Exception, match="No config"):
        config.get_symbol_info("BTC")
