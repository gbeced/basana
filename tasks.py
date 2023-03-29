from invoke import task


@task
def clean(c):
    patterns = [
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        "*.pyc",
    ]
    for pattern in patterns:
        c.run("find . -d -name '{}' -exec rm -rf {{}} \\;".format(pattern))

    with c.cd("docs"):
        c.run("poetry run -- make clean", pty=True)


@task
def lint(c):
    c.run("poetry run -- mypy basana", pty=True)
    c.run("poetry run -- flake8", pty=True)


@task(lint)
def test(c, html_report=False):
    # Execute testcases.
    cmd = "poetry run -- pytest -vv --cov --cov-config=setup.cfg --durations=10"
    if html_report:
        cmd += " --cov-report=html:cov_html"
    c.run(cmd, pty=True)


@task
def create_virtualenv(c):
    c.run("poetry install", pty=True)


@task
def build_docs(c):
    with c.cd("docs"):
        c.run("poetry run -- make html", pty=True)
