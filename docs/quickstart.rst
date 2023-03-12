Quickstart
==========

Ready to get started ? This page gives a good introduction on how to get started with Basana.

.. _quickstart_installation:

Installation
------------

Basana requires Python 3.8.1 or above and to install the package you can use the following command:

.. code-block:: console

   (.venv) $ pip install basana

As mentioned before, technical indicators are not included and the examples that follow take advantage of
`TALIpp <https://github.com/nardew/talipp>`_ that you can install using the following command:

.. code-block:: console

   (.venv) $ pip install talipp

.. _quickstart_backtesting:

Backtesting
-----------

The examples that follow are structured like this:

* A trading **strategy** that implements a the set of rules that define when to enter or exit a trade based on market conditions.
* A **position manager** responsible for executing trades and managing positions. It receives trading signals from the trading
  strategy and submits orders to the exchange or broker.

The strategy that we're going to use for backtesting is based on `Bollinger Bands <https://www.investopedia.com/articles/trading/07/bollinger.asp>`_
and the purpose of this example is just to give you an overview on how to connect the different pieces together.

At a high level this is how this strategy will work:

* There are two types of events taking place in this example. Bars (OHLC) coming from the exchange and trading signals.
* When a new bar is received by the strategy, a technical indicator will be fed using the bar's closing price. If the technical indicator
  is ready we'll check if the price moved below the lower band or if the price moved above upper band, and we'll generate a buy or sell
  trading signal.
* When a trading signal is received by the position manager, a buy or sell market order will be submitted to the exchange in order to open
  or close a position.

We'll be using market orders to keep this example short, but you'll probably want to use limit orders when writing your own position managers.
Many examples, including those that use limit orders, are available in the `examples folder <https://github.com/gbeced/basana/tree/master/samples>`_.

The first thing we'll need in order to execute the backtest is historical data. Use the following command to download minute bars from Binance
for January 2021:

.. code-block:: console

    (.venv) $ python -m basana.external.binance.tools.download_bars -c BTC/USDT -p 1m -s 2021-01-01 -e 2021-01-31 > binance_btcusdt_min.csv

Next, save the following strategy code as *bbands.py*:

.. literalinclude:: ../samples/bbands.py
   :language: python
   :lines: 21-

Next, save the following code as *backtesting_bbands.py*:

.. literalinclude:: ../samples/backtesting_bbands.py
   :language: python
   :lines: 25-

and execute the backtest like this:

.. code-block:: console

    (.venv) $ python backtesting_bbands.py

.. note::

    The way the examples are structured is just one way to do it. You're free to structure things in any other way.

.. _quickstart_livetrading:

Live trading
------------

TODO
