# Basana Copilot Instructions

## Architecture Overview
Basana is an async event-driven framework for algorithmic trading focused on cryptocurrencies. It provides:
- **Backtesting Exchange**: Simulated trading environment in `basana/backtesting/exchange.py`
- **Live Trading**: Real-time integrations with Binance (`basana/external/binance/`) and Bitstamp (`basana/external/bitstamp/`)
- **Event System**: Core dispatcher (`basana/core/dispatcher.py`) handles events chronologically from multiple sources
- **Strategies**: Inherit from `TradingSignalSource`, process bar events, emit `TradingSignal` objects
- **Position Management**: Handles order execution, stop-loss/take-profit in `samples/backtesting/position_manager.py`

Key data flows: Bar events → Strategies → Trading signals → Position managers → Orders → Exchange execution.

## Development Workflow
- **Dependencies**: Use uv (`uv sync --locked --all-extras`). Python 3.10+ required.
- **Linting**: `uv run inv lint` runs mypy and ruff (line length 120, excludes `pocs/`)
- **Testing**: `uv run inv test` runs lint + pytest with 100% line coverage required.
- **Single test**: `uv run pytest tests/test_file.py::test_name -vv --no-cov`
- **Cleaning**: `uv run inv clean` removes caches and build artifacts
- **Documentation**: `uv run inv build-docs` generates Sphinx docs in `docs/_build/html/`

## Code Patterns
- **Async Everywhere**: All trading logic is async. Use `asyncio.gather()` for concurrent operations.
- **Decimal Precision**: Use `Decimal` for all monetary values. Set symbol precision with `exchange.set_symbol_precision()`.
- **Event Subscription**: Subscribe handlers with `exchange.subscribe_to_*_events(pair, handler)`. Example:
  ```python
  exchange.subscribe_to_bar_events(pair, strategy.on_bar_event)
  strategy.subscribe_to_trading_signals(position_mgr.on_trading_signal)
  ```
- **Trading Signals**: Strategies push signals with `self.push(TradingSignal(when, Position.LONG/SHORT, pair))`
- **Order Operations**: Use `OrderOperation.BUY/SELL` for market orders. Enable auto-borrow with `auto_borrow=True`.
- **Logging**: Use `StructuredMessage` for structured logs: `logger.info(StructuredMessage("Order created", id=order.id))`
- **Position Tracking**: Positions use signed decimals (positive=long, negative=short). Calculate PnL with bid/ask prices.

## Integration Points
- **Binance API**: Supports spot, margin, cross/isolated margin. Use websockets for real-time data.
- **Bitstamp API**: Similar structure to Binance but simpler feature set.
- **Technical Indicators**: Use TALib-compatible `talipp` library. Feed with `indicator.add(value)`.
- **Charts**: Optional Plotly integration for backtesting visualization (`charts.LineCharts`).

## Conventions
- **Imports**: Absolute imports from `basana` package. Core types from `basana.core.*`.
- **Error Handling**: Async exceptions logged with `logger.exception(e)`. Orders may fail - check `order.is_open`.
- **Backtesting Data**: Load CSV bars with `csv.BarSource(pair, filename, interval)`.
- **Live Trading**: Requires API keys. Use `realtime_dispatcher()` instead of `backtesting_dispatcher()`.
- **Precision Handling**: Use `round_decimal()`/`truncate_decimal()` for display/calculation precision.

## Testing Conventions
- `asyncio_mode = auto` (setup.cfg) — async test functions run without `@pytest.mark.asyncio`.
- Pytest fixtures `backtesting_dispatcher` and `realtime_dispatcher` are available globally via `tests/conftest.py`.
- Shared test pairs/pair-infos live in `tests/common.py` (e.g. `btc_pair`, `btc_pair_info`).
- 100% line coverage is required; use `# pragma: no cover` only for unreachable/platform-specific branches.

## Common Pitfalls
- Naive datetimes cause issues - always use timezone-aware (`dt.utc_now()`).
- Concurrent position modifications require locks (`asyncio.Lock` per pair).
- WebSocket reconnections handled automatically, but monitor for gaps.
- Borrowing disabled by default in samples - set `borrowing_disabled=False` to enable shorts.

## Permissions
- Always ask before installing new packages and do it inside a virtual environment.