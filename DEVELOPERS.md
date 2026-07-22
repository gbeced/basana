# For Developers

## Requirements

* Python 3.10 or greater.
* [uv](https://docs.astral.sh/uv/) for dependency and package management.

## Environment setup and testing

### Initialize the virtual environment and install dependencies.

```
$ uv sync --locked --all-extras
```

### Execute static checks

```
$ uv run inv lint
```

### Execute testcases (will also execute static checks)

```
$ uv run inv test
```

### Execute a single test

```
$ uv run pytest tests/test_backtesting_exchange.py::test_bid_ask -vv --no-cov
```

## Building docs

```
$ uv run inv build-docs
```
