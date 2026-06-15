from invoke import task

cmd_echo = True


@task
def clean(c):
    patterns = [
        "__pycache__",
        ".coverage",
        ".pytest_cache",
        ".mypy_cache",
        "*.pyc",
    ]
    for pattern in patterns:
        c.run("find . -d -name '{}' -exec rm -rf {{}} \\;".format(pattern), echo=cmd_echo)

    with c.cd("docs"):
        c.run("make clean", echo=cmd_echo)


@task
def lint(c):
    c.run("mypy basana", echo=cmd_echo)
    c.run("ruff check", echo=cmd_echo)


@task(lint)
def test(c, html_report=False):
    # Execute testcases.
    cmd = "pytest -vv --cov --cov-config=setup.cfg --durations=10"
    if html_report:
        cmd += " --cov-report=html:cov_html"
    c.run(cmd, echo=cmd_echo)


@task
def build_docs(c):
    with c.cd("docs"):
        c.run("make html", echo=cmd_echo)


@task
def build_dist(c):
    c.run("rm -rf dist && uv build", echo=cmd_echo)
