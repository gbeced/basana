# AGENTS.md

Guidance for coding agents working on **Basana**.

Basana is a Python **async, event-driven** framework for **algorithmic trading**, focused on **cryptocurrencies**. It provides a backtesting exchange and live-trading integrations (Binance, Bitstamp, and 100+ exchanges via CCXT).

**This file is the single source of truth for all coding agents, regardless of tooling** (Claude Code, GitHub Copilot, Grok, etc.). Grok and other tools read `AGENTS.md` directly; the tool-specific entry points `CLAUDE.md` and `.github/copilot-instructions.md` are thin pointers back to this file. Keep guidance here — do not duplicate it into the pointer files.

Human-oriented setup notes also live in `DEVELOPERS.md`.

---

## Repository map

| Path | Role |
|------|------|
| `basana/` | Installable library package (this is what ships to PyPI) |
| `basana/core/` | Events, dispatchers, bars, pairs, enums, websockets, helpers |
| `basana/backtesting/` | Simulated exchange: orders, fees, liquidity, lending, charts |
| `basana/external/binance/` | Binance spot/margin REST + websockets + CSV bars + download tools |
| `basana/external/bitstamp/` | Bitstamp REST + websockets + CSV bars + download tools |
| `basana/external/ccxt/` | CCXT Pro-based multi-exchange live trading |
| `basana/external/yahoo/` | Yahoo Finance historical bars |
| `basana/external/common/` | Shared CSV bar utilities |
| `samples/` | Educational examples (strategies, position managers, live/backtest scripts) |
| `tests/` | Pytest suite, fixtures, and JSON/CSV fixtures under `tests/data/` |
| `docs/` | Sphinx docs (API reference + guides) |
| `tasks.py` | Invoke tasks: `lint`, `test`, `build-docs`, `clean`, `build-dist` |
| `pocs/`, `non-shippable/`, `nnet/` | Local/experimental; not part of the published package |
| `data/` | Local sample market data (gitignored content under `data/*`) |

**Do not treat `samples/` as production library code.** Prefer implementing reusable features under `basana/` with tests and docs. Samples illustrate usage only.

---

## Architecture (mental model)

Three main layers:

1. **Core** — `Event`, `EventSource` / `Producer`, `EventDispatcher`, bars, pairs, trading signals.
2. **Backtesting exchange** — `basana.backtesting.exchange.Exchange` consumes bar sources, simulates fills, balances, fees, lending.
3. **External integrations** — live exchanges expose similar subscription/order APIs on top of REST/websockets (or CCXT).

Primary data flow:

```
Bar / market / user events
  → Strategies (TradingSignalSource)
  → TradingSignal
  → Position managers
  → Orders
  → Exchange execution
```

Dispatchers:

- `basana.backtesting_dispatcher()` — chronological, deterministic; default `max_concurrent=1`.
- `basana.realtime_dispatcher()` — wall-clock live trading; default `max_concurrent=50`.

Both support `subscribe_event_loop_started` for one-shot startup handlers.

Public surface re-exported from `basana/__init__.py` includes: `Bar`, `BarEvent`, dispatchers, `utc_now` / `local_now`, `Event` / sources, `TradingSignal` / `TradingSignalSource`, `OrderOperation`, `Position`, `PrecisionMode`, `Pair` / `PairInfo`, decimal helpers, `TokenBucketLimiter`.

---

## Environment and toolchain

- **Python**: `>=3.10,<4` (CI matrix: 3.10–3.14 on Linux, macOS, Windows).
- **Package/deps manager**: [uv](https://docs.astral.sh/uv/). Lockfile: `uv.lock`.
- **Build backend**: `uv_build` (see `pyproject.toml`).
- **Task runner**: Invoke (`tasks.py`).
- **Lint/types**: Ruff + mypy.
- **Tests**: pytest + pytest-asyncio + pytest-cov + aioresponses + pytest-mock.
- **Optional extras**:
  - `charts` — Plotly + kaleido
  - `ccxt` — `ccxt>=4.4,<5` (library uses `ccxt.pro`)
- **Sample-only deps** (dev group): talipp, pandas, statsmodels, textual.

### Setup

```bash
uv sync --locked --all-extras
```

Always prefer the project venv via `uv run …`. **Ask before adding or upgrading dependencies.** If a dependency change is approved, update `pyproject.toml` and regenerate the lockfile with uv; do not hand-edit `uv.lock`.

### Common commands

| Goal | Command |
|------|---------|
| Lint (mypy + ruff) | `uv run inv lint` |
| Full test suite (lint + pytest + coverage) | `uv run inv test` |
| Coverage HTML report | `uv run inv test --html-report` |
| Single test (no coverage gate) | `uv run pytest tests/test_file.py::test_name -vv --no-cov` |
| Build Sphinx HTML | `uv run inv build-docs` |
| Clean caches / doc build | `uv run inv clean` |
| Build wheel/sdist | `uv run inv build-dist` |

CI (`.github/workflows/runtests.yml`) runs `uv sync --locked --all-extras --no-group docs --no-install-project` then `uv run inv test` on every supported OS/Python.

---

## Quality bars (non-negotiable)

1. **100% line coverage** of `basana/*` (`setup.cfg` → `fail_under = 100`). New library code needs tests.
2. **`uv run inv lint`** must pass: `mypy basana` and `ruff check`.
3. **Ruff**: line length **120**; `pocs/` excluded; `E501` enforced.
4. Prefer fixing root causes over `# pragma: no cover` or type ignores. Allowed coverage exclude patterns are only:
   - `raise NotImplementedError()`
   - `pragma: no cover` (unreachable / platform-specific only)

When iterating locally, use `--no-cov` for speed; always re-run `uv run inv test` before considering work done.

---

## Coding conventions

### Style and structure

- Match surrounding code: Apache-2.0 license header on source files, import order, naming, docstring style.
- Prefer **absolute imports** (`from basana.core…`, `from basana.backtesting…`).
- Consumers of the public API typically `import basana as bs`.
- Use type hints consistent with existing modules (`typing` + modern annotations as already used nearby).
- Keep modules focused; exchange-specific request/order helpers live next to that integration (see `spot_requests.py`, `margin_requests.py`, `external/ccxt/requests.py`).

### Async and concurrency

- Trading logic is **async**. Event handlers are coroutines unless the surrounding code clearly uses sync helpers.
- Prefer structured concurrency helpers already in the codebase (`TaskGroup`, `TaskPool` in `basana.core.helpers`) over ad-hoc task sprawl.
- Concurrent mutation of shared position/order state should use `asyncio.Lock` (often per pair), as samples do.
- Realtime dispatcher processes events concurrently; backtesting defaults to serial processing for determinism.

### Money and precision

- Use `decimal.Decimal` for **all** monetary quantities, sizes, prices, fees, balances.
- Never use `float` for money. Floats appear only at indicator boundaries (e.g. talipp inputs) in samples.
- Use `basana.round_decimal` / `basana.truncate_decimal` and exchange/pair precision (`PairInfo`, `PrecisionMode`, `set_symbol_precision` on the backtesting exchange).
- Backtesting: set symbol precision explicitly when initializing scenarios.

### Time

- Always use **timezone-aware** datetimes. Prefer `basana.utc_now()` / `basana.core.dt` helpers.
- Naive datetimes break ordering and logging. Event `when` fields must carry tzinfo.
- Backtesting logs can use `basana.core.logs.backtesting_log_mode(dispatcher)` so log timestamps follow simulated time.

### Events, strategies, orders

- Strategies subclass `basana.TradingSignalSource` and emit via `self.push(TradingSignal(...))`.
- Prefer `Position.LONG` / `SHORT` / `NEUTRAL` on signals. Passing `OrderOperation` into `TradingSignal` is deprecated.
- Subscribe with exchange methods like:
  - `subscribe_to_bar_events(...)`
  - `subscribe_to_order_events(...)` / private order streams on live exchanges
  - strategy `subscribe_to_trading_signals(handler)`
- Order sides use `OrderOperation.BUY` / `SELL`.
- Orders can fail or partially fill; inspect `order.is_open` / fill fields rather than assuming success.
- Logging: prefer `StructuredMessage` from `basana.core.logs` for structured key/value context.

### Backtesting specifics

- Bar history: CSV bar sources (e.g. `basana.external.binance.csv.BarSource`) or exchange `add_bar_source(...)`.
- Fees: `basana.backtesting.fees` (e.g. `Percentage`; base-currency fee charging is supported).
- Liquidity: pluggable strategies under `basana.backtesting.liquidity`.
- Lending/margin: `basana.backtesting.lending` (`MarginLoans`, etc.). Borrowing is opt-in via lending strategy configuration.
- Charts (optional extra): `basana.backtesting.charts.LineCharts` and related chart types (Plotly).

### Live integrations

| Integration | Package path | Notes |
|-------------|--------------|--------|
| Binance | `basana.external.binance` | Spot + cross/isolated margin; REST clients; user data + order book diff websockets; CSV + `tools.download_bars` |
| Bitstamp | `basana.external.bitstamp` | Spot-oriented; websockets for book/trades; CSV + download tool |
| CCXT | `basana.external.ccxt` | Wraps **CCXT Pro**; construct with exchange id + credentials; call `await exchange.close()` in `finally` |
| Yahoo | `basana.external.yahoo` | Historical bars for non-crypto symbols / research |

- Rate limiting: `TokenBucketLimiter` and integration-specific config where applicable.
- Websocket clients reconnect; still design for gaps (order-book mirror sample shows snapshot + diff + consistency checks).
- **Never commit real API keys or secrets.** Samples must use placeholders or env vars. Demo/sandbox flags (e.g. CCXT `enable_demo_trading`) are preferred for examples.

### Indicators and samples

- Basana does **not** ship technical indicators. Samples use [talipp](https://github.com/nardew/talipp).
- Strategy examples live under `samples/strategies/`; position managers under `samples/backtesting/`, `samples/binance/`, `samples/ccxt_util/`.

---

## Testing conventions

- Config: `setup.cfg` — `asyncio_mode = auto` (async tests do **not** need `@pytest.mark.asyncio`).
- Coverage scope: `basana/*` only (samples are not coverage-gated).
- Global fixtures via `tests/conftest.py` re-exports from `tests/fixtures/`:
  - `backtesting_dispatcher`, `realtime_dispatcher`
  - Binance / Bitstamp / CCXT HTTP/WS mocks and exchange fixtures
- Shared pairs: `tests/common.py` (`btc_pair`, `btc_pair_info`, etc.).
- Fixture payloads: `tests/data/` (JSON exchange info, CSV bars). Prefer loading via existing helpers rather than embedding large blobs in tests.
- Mock external HTTP with **aioresponses**; mock CCXT client methods with **pytest-mock** / `unittest.mock` as existing CCXT tests do.
- Name tests `tests/test_<area>.py` and functions `test_<behavior>`.
- Keep tests deterministic: no real network, no wall-clock sleeps unless mocking time/dispatcher behavior as existing tests do.

When adding a feature to an external integration, extend the corresponding fixture module under `tests/fixtures/` if shared setup grows.

---

## Documentation

- Sphinx sources in `docs/`. API pages are listed in `docs/api.rst`.
- New public classes/methods should have docstrings suitable for autodoc (follow nearby modules).
- If you add a public integration surface (e.g. new CCXT event type), add/update the matching `docs/*.rst` entry and toctree.
- Build with `uv run inv build-docs` (requires docs dependency group; Python ≥ 3.11 for current Sphinx pins).

---

## Changelog and versioning

- Version is set in `pyproject.toml` (`project.version`).
- User-facing changes go in `CHANGELOG.md` under the appropriate section (`Features`, `Bug fixes`, `Misc`). Unreleased work goes under `## TBD` until a release is cut.

---

## What to change (and what not to)

**Usually appropriate**

- Library code under `basana/` with tests and, when public, docs + changelog.
- Test fixtures and helpers under `tests/`.
- Invoke tasks / tooling configs when required by the change.
- Sample updates that demonstrate a new API (keep them minimal and secret-free).

**Avoid unless explicitly requested**

- Drive-by refactors unrelated to the task.
- Reformatting unrelated files.
- Editing `pocs/`, `nnet/`, `non-shippable/`, or generated `docs/_build` / `docs/generated`.
- Weakening coverage, lint, or CI matrices to make a change pass.
- Adding dependencies without approval.
- Committing credentials, local logs, or large binary artifacts.

---

## Agent workflow checklist

1. Read the nearest existing module/tests before editing; mirror patterns.
2. Implement the smallest change that satisfies the request.
3. Add/adjust unit tests for any `basana/` behavior change.
4. Run `uv run inv lint` and targeted pytest; finish with `uv run inv test` when practical.
5. Update `CHANGELOG.md` / Sphinx docs for user-visible API changes.
6. Do not install packages or alter lockfiles without asking.
7. Do not create commits or PRs unless the user asks.

---

## Common pitfalls

- **Naive datetimes** — always tz-aware (`dt.utc_now()`).
- **Float money** — use `Decimal` end-to-end in library code.
- **Assuming fills succeed** — check order state; handle partials and rejects.
- **Backtesting shorts without lending** — configure a lending strategy; samples often disable borrowing by default.
- **Forgetting CCXT cleanup** — `await xch.close()` in `finally`.
- **Coverage regressions** — uncovered new branches fail CI.
- **Importing from samples inside `basana/`** — dependency direction is one-way: samples → library, never the reverse.
- **Editing lockfile by hand** — use uv only.

---

## Permissions and safety

- Ask before installing or upgrading packages; use the project virtualenv via `uv`.
- Never run live orders against real funds unless the user explicitly requests it and understands the risk.
- Never commit or log secrets. If a sample currently contains placeholder credentials, leave placeholders—do not insert real keys.
- Prefer reversible local edits; confirm before destructive git operations, force-pushes, or publishing releases.
