# Changelog

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

* basana.backtesting.fees.Percentage now supports a minimum fee.

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
