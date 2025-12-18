# Changelog

## TBD

### Bug fixes

* Backtesting stop orders were being canceled if the stop price was not hit.
* Backtesting order updates were sometimes being processed after bar events. Order update events should get processed first.
* Backtesting fills had the datetime of the event and now they have a datetime within the bar where the fill occurred.
* Backtesting `OrderInfo.fill_price` was being calculated using the base and quote amounts filled and that could lead to invalid values.

### Misc

* Added support for Python 3.14.
* Dropped support for Python 3.9.

## 1.9

### Features

* Added support for Binance order book diff events via websockets.
* Performance improvements for realtime dispatcher.

### Bug fixes

* `SchedulerQueue.peek_last_event_dt` was not returning the last event's datetime.

## 1.8

### Features

* Backtesting `OrderInfo` now includes pair and order fills.
* Backtesting exchange returns a full `OrderInfo` when creating an order.
* Backtesting exchange support for immediate order processing.

### Bug fixes

* Backtesting margin level calculations were wrong. `margin_requirement` was moved from `MarginLoanConditions` into `MarginLoans`

## 1.7.1

### Bug fixes

* No event was generated when a backtesting order got canceled.

## 1.7

### Features

* Added support for Binance order and user data events via websockets.
* Added support for backtesting order events.
* Added loan ids to order info.

### Bug fixes

* `bitstamp.orders.Order.amount` was returning the order amount left to be executed instead of the original amount.

### Misc

* Updated dependencies and minimum Python version.

## 1.6.2

### Bug fixes

* `VolumeShareImpact.calculate_price` and `VolumeShareImpact.calculate_amount` were failing when there was no available liquidity.

## 1.6.1

### Bug fixes

* Ignore events scheduled after the last event is procesed.

## 1.6

### Features

* Support for borrowing funds and opening short positions with the backtesting exchange.

### Bug fixes

* [Parse BOM when opening CSV files and set the encoding appropriately](https://github.com/gbeced/basana/issues/36)
* [Stop subtracting microseconds in RowParser](https://github.com/gbeced/basana/issues/37)

## 1.5.0

* `backtesting.fees.Percentage` now supports a minimum fee.

## 1.4.1

* Bug fix. Capture and log unhandled exceptions in scheduled jobs.
* Bug fix. Rounding in charts.

## 1.4.0

* Support for scheduling functions that will be executed at a given date and time.
* Improved dispatcher concurrency, specially for the realtime dispatcher.

## 1.3.2

* Bug fix in download_bars.

## 1.3.1

* Updated docs.
* Bug fix in samples.
* Bug fix in charts.

## 1.3.0

* Updates to Binance Bar events.
* Updated docs.
* Bug fix in samples.
* Support for charts.

## 1.2.2

* Updated docs.
* Bug fix in signal handling under Win32.

## 1.2.1

* Bug fix in datetime management when dealing with Bar events.

## 1.2.0

* Updated docs.

## 1.1.0

* Bug fix in samples.
* Added tool to download bars from Binance.

## 1.0.0
