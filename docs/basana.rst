basana
======

This is the core of the event driven architecture. At a high level you have:

* **Events**. It could be a new bar, a new trade, an order book update, etc.
* **Event sources**, for example a websocket that pushes a new message when an order book is updated.
* **Event handlers** that are connected to certain event sources and are invoked when these generate new events.
* An **event dispatcher** that is responsible for running the event loop and invoking event handlers in the right
  order as events from different sources occur.

The trading signal source implements the set of rules that define when to enter or exit a trade based on the conditions
you define. Take a look at the :doc:`quickstart` section for examples on how to implement trading signal sources.

.. module:: basana

.. autoclass:: basana.Event
    :members:
.. autoclass:: basana.EventSource
    :members:
.. autoclass:: basana.FifoQueueEventSource
    :show-inheritance:
    :members:
.. autoclass:: basana.Producer
    :members:
.. autoclass:: basana.EventDispatcher
    :members:
.. autoclass:: basana.BacktestingDispatcher
    :show-inheritance:
    :members:
.. autoclass:: basana.RealtimeDispatcher
    :show-inheritance:
    :members:
.. autofunction:: basana.backtesting_dispatcher
.. autofunction:: basana.realtime_dispatcher

.. autoclass:: basana.Bar
    :members:
.. autoclass:: basana.BarEvent
    :show-inheritance:
    :members:
.. autoclass:: basana.OrderOperation
    :members:
.. autoclass:: basana.Pair
    :members:
.. autoclass:: basana.PairInfo
    :members:
.. autoclass:: basana.Position
    :members:

.. autoclass:: basana.TradingSignal
    :show-inheritance:
    :inherited-members:
    :members:
.. autoclass:: basana.TradingSignalSource
    :show-inheritance:
    :members:

.. autoclass:: basana.TokenBucketLimiter
.. autofunction:: basana.round_decimal
.. autofunction:: basana.truncate_decimal
.. autofunction:: basana.local_now
.. autofunction:: basana.utc_now
