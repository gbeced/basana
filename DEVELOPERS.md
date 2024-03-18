# For Developers

## Requirements

* Python 3.8.1 or greater.
* [Poetry](https://python-poetry.org/) for dependency and package management.
* Optionally, [Invoke](https://www.pyinvoke.org/).

## Environment setup and testing

### Using Poetry

1. Initialize the virtual environment and install dependencies.

	```
	$ poetry install --all-extras
	```

1. Static checks

	```
	$ poetry run -- mypy basana
	$ poetry run -- flake8
	```

1. Execute testcases

	```
	$ poetry run pytest -vv --cov --cov-config=setup.cfg --durations=10
	```

### Using Invoke

Instead of running those commands manually, a couple of Invoke tasks are provided to wrap those.

1. Initialize the virtual environment and install dependencies.

	```
	$ inv create-virtualenv
	```

1. Execute static checks and testcases 

	```
	$ inv test
	```

## Building docs

```
$ inv build-docs
```
