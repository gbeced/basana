.. Basana documentation master file, created by
   sphinx-quickstart on Wed Mar  8 13:07:56 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Basana's documentation!
==================================

**Basana** is a Python **async and event driven** framework for **algorithmic trading**, with a focus on **crypto currencies**.

The framework has 3 main components:

* The core, where basic abstractions like events, event sources and the event dispatcher live.
* A backtesting exchange that you can use to validate your strategies before using real money.
* External integrations, where you'll find support for live trading at `Binance <https://www.binance.com/>`_ and
  `Bitstamp <https://www.bitstamp.net/>`_ crypto currency exchanges.

Basana doesn't ship with technical indicators. The `examples at GitHub <https://github.com/gbeced/basana/tree/master/samples>`_
take advantage of `TALIpp <https://github.com/nardew/talipp>`_ which is a good fit for event driven and real time applications,
but you're free to use any other library that you prefer.

Check out the :doc:`quickstart` section for further information, including how to :ref:`install <quickstart_installation>` the package,
do a :ref:`backtest <quickstart_backtesting>` and :ref:`live trade <quickstart_livetrading>`.

Contents
--------

.. toctree::
   :maxdepth: 2

   quickstart
   api
   help

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
