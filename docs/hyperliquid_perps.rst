basana.external.hyperliquid.perps
================================

.. module:: basana.external.hyperliquid.perps

.. autoclass:: basana.external.hyperliquid.perps.Account
    :members:

.. note::

   ``subscribe_to_fill_events`` registers the fill channel with the shared
   websocket client **and** subscribes the provided async handler through the
   event dispatcher. As with other live streams in Basana, the dispatcher must
   be running for callbacks to fire.
.. autoclass:: basana.external.hyperliquid.perps.Position
    :show-inheritance:
    :members:
.. autoclass:: basana.external.hyperliquid.perps.OrderInfo
    :show-inheritance:
    :members:
