basana.backtesting
==================

.. contents:: Table of Contents
   :depth: 2

Liquidity strategies
--------------------
 
Liquidity strategies are used by the backtesting exchange to determine:

* How much of a bar's volume can be consumed by an order.
* The price slippage.

.. module:: basana.backtesting.liquidity
.. autoclass:: basana.backtesting.liquidity.InfiniteLiquidity
.. autoclass:: basana.backtesting.liquidity.VolumeShareImpact

Fee strategies
--------------
 
Fee strategies are used by the backtesting exchange to determine the fee to charge to each trade.

.. module:: basana.backtesting.fees
.. autoclass:: basana.backtesting.fees.NoFee
.. autoclass:: basana.backtesting.fees.Percentage

Exchange
--------
 
.. module:: basana.backtesting.exchange
.. autoexception:: basana.backtesting.exchange.Error
    :show-inheritance:
.. autoclass:: basana.backtesting.exchange.Exchange
    :members:
.. autoclass:: basana.backtesting.exchange.Balance
    :members:
.. autoclass:: basana.backtesting.exchange.CreatedOrder
    :members:
.. autoclass:: basana.backtesting.exchange.CanceledOrder
    :members:
.. autoclass:: basana.backtesting.exchange.OrderInfo
    :members:
.. autoclass:: basana.backtesting.exchange.OpenOrder
    :members:
