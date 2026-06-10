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
        c.run("find . -d -name '{}' -exec rm -rf {{}} \\;".format(pattern), pty=True, echo=cmd_echo)

    with c.cd("docs"):
        c.run("uv run make clean", pty=True, echo=cmd_echo)


@task
def lint(c):
    c.run("uv run mypy basana", pty=True, echo=cmd_echo)
    c.run("uv run ruff check", pty=True, echo=cmd_echo)


@task(lint)
def test(c, html_report=False):
    # Execute testcases.
    cmd = "uv run pytest -vv --cov --cov-config=setup.cfg --durations=10"
    if html_report:
        cmd += " --cov-report=html:cov_html"
    c.run(cmd, pty=True, echo=cmd_echo)


@task
def create_virtualenv(c, all_extras=True):
    cmd = ["uv", "sync", "--locked"]
    if all_extras:
        cmd.append("--all-extras")
    c.run(" ".join(cmd), pty=True, echo=cmd_echo)


@task
def build_docs(c):
    with c.cd("docs"):
        c.run("uv run make html", pty=True, echo=cmd_echo)


@task
def build_dist(c):
    c.run("rm -rf dist && uv build", pty=True, echo=cmd_echo)