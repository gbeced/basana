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
import abc

from basana.core import bar


class LiquidityStrategy(metaclass=abc.ABCMeta):
    """Base class for liquidity strategies.

    A liquidity strategy defines how much of a :class:`basana.Bar`s volume can be used when processing an order and
    what is the price slippage.

    .. note::

        * This is a base class and should not be used directly.
        * Concrete strategies will be created by the :class:`basana.backtesting.exchange.Exchange` for each traded
          pair.
    """

    @abc.abstractmethod
    def on_bar(self, bar: bar.Bar):
        """Called when a new bar is available."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def available_liquidity(self) -> Decimal:
        """Returns the available liquidity."""
        raise NotImplementedError()

    @abc.abstractmethod
    def take_liquidity(self, amount: Decimal) -> Decimal:
        """Takes/consumes available liquidity.

        :param amount: The amount of liquidity to take. It must be <= available liquidity.
        :returns: The percentage of the price impact.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def calculate_price_impact(self, amount: Decimal) -> Decimal:
        """A read-only version of :meth:`take_liquidity`."""
        raise NotImplementedError()

    @abc.abstractmethod
    def calculate_amount(self, price_impact: Decimal) -> Decimal:
        """Returns the amount of liquidity that can be taken with an impact <= price_impact."""
        raise NotImplementedError()


class InfiniteLiquidity(LiquidityStrategy):
    """This class models infinite liquidity with no price impact."""

    def on_bar(self, bar: bar.Bar):
        pass

    @property
    def available_liquidity(self) -> Decimal:
        return Decimal("Infinity")

    def take_liquidity(self, amount: Decimal) -> Decimal:
        assert amount > Decimal(0)
        assert amount <= self.available_liquidity

        return Decimal(0)

    def calculate_price_impact(self, amount: Decimal) -> Decimal:
        assert amount > Decimal(0)
        assert amount <= self.available_liquidity

        return Decimal(0)

    def calculate_amount(self, price_impact: Decimal) -> Decimal:
        assert price_impact >= Decimal(0), f"Invalid price_impact {price_impact}"

        return Decimal("Infinity")


class VolumeShareImpact(LiquidityStrategy):
    """The price impact is calculated by multiplying the price impact constant by the square of the ratio of the used
    volume to the total volume.

    :param volume_limit_pct: Maximum percentage of volume that can be used from each bar.
    :param price_impact: Maximum price impact (percentage).
    """

    def __init__(self, volume_limit_pct: Decimal = Decimal("25"), price_impact: Decimal = Decimal("10")):
        assert volume_limit_pct >= Decimal(0), f"Invalid volume_limit_pct {volume_limit_pct}"
        assert price_impact >= Decimal(0), f"Invalid price_impact {price_impact}"

        self._volume_limit_pct = volume_limit_pct / Decimal(100)
        self._price_impact_pct = price_impact / Decimal(100)
        self._total_liquidity = Decimal(0)
        self._used_liquidity = Decimal(0)

    def on_bar(self, bar: bar.Bar):
        self._total_liquidity = bar.volume * self._volume_limit_pct
        self._used_liquidity = Decimal(0)

    def _volume_share_impact(self, used_liquidity: Decimal) -> Decimal:
        # impact = (used_liquidity / (used_liquidity + available_liquidity)) ** 2 * price_impact
        assert used_liquidity >= Decimal(0), f"Invalid used_liquidity {used_liquidity}"
        assert used_liquidity <= self._total_liquidity, f"Invalid used_liquidity {used_liquidity}"

        used_pct = used_liquidity / self._total_liquidity
        return used_pct ** Decimal(2) * self._price_impact_pct

    @property
    def available_liquidity(self) -> Decimal:
        return self._total_liquidity - self._used_liquidity

    def take_liquidity(self, amount: Decimal) -> Decimal:
        assert amount > 0, f"Invalid amount {amount}"
        assert amount <= self.available_liquidity, f"amount {amount} too high"

        impact_pre = self._volume_share_impact(self._used_liquidity)
        self._used_liquidity += amount
        impact_post = self._volume_share_impact(self._used_liquidity)
        diff = impact_post - impact_pre
        assert diff >= Decimal(0)
        return diff

    def calculate_price_impact(self, amount: Decimal) -> Decimal:
        assert amount >= Decimal(0), f"Invalid amount {amount}"
        assert amount <= self.available_liquidity, f"amount {amount} too high"

        return self._volume_share_impact(self._used_liquidity + amount)

    def calculate_amount(self, price_impact: Decimal) -> Decimal:
        assert price_impact >= Decimal(0), f"Invalid price_impact {price_impact}"

        # price_impact = (used_liquidity / self._total_liquidity) ** 2 * self._price_impact_pct
        # price_impact / self._price_impact_pct = (used_liquidity / self._total_liquidity) ** 2
        # sqrt(price_impact / self._price_impact_pct) = used_liquidity / self._total_liquidity
        # used_liquidity = self._total_liquidity * sqrt(price_impact / self._price_impact_pct)

        price_impact = min(price_impact, self._price_impact_pct)
        used_liquidity = self._total_liquidity * (price_impact / self._price_impact_pct).sqrt()

        assert used_liquidity <= self._total_liquidity
        return max(Decimal(0), used_liquidity - self._used_liquidity)
