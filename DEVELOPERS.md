# For Developers

## Requirements

* Python 3.10 or greater.
* [uv](https://docs.astral.sh/uv/) for dependency and package management.

## Environment setup and testing

### Using uv

1. Initialize the virtual environment and install dependencies.

	```
	$ uv sync --locked --all-extras
	```

1. Static checks

	```
	$ uv run mypy basana
	$ uv run ruff check
	```

1. Execute testcases

	```
	$ uv run pytest -vv --cov --cov-config=setup.cfg --durations=10
	```

### Using Invoke

Instead of running those commands manually, a couple of Invoke tasks are provided to wrap those. Invoke is installed as a dev dependency via `uv sync`.

1. Initialize the virtual environment and install dependencies.

	```
	$ uv run inv create-virtualenv
	```

1. Execute static checks and testcases 

	```
	$ uv run inv test
	```

## Building docs

```
$ uv run inv build-docs
```