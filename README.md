# Basana

A Python event driven framework for algorithmic trading, with a focus on crypto.

Features:

* Backtesing
* Binance support
* Bitstamp support
* Asynchronous I/O

## For developers

### Requirements

* Python 3.8.1 or greater. [pyenv](https://github.com/pyenv/pyenv) is recommended.
* [Poetry](https://python-poetry.org/) for dependency and package management.
* [Invoke](https://www.pyinvoke.org/) instead of using a Makefile with PHONE targets.

### Environment setup and testing

#### Using Poetry

1. Initialize the virtual environment and install dependencies.

	```
	$ poetry install --no-root
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

#### Using Invoke

Instead of running those commands manually, a couple of Invoke tasks are provided to wrap those.

1. Initialize the virtual environment and install dependencies.

	```
	$ inv create-virtualenv
	```

1. Execute static checks and testcases 

	```
	$ inv test
	```
