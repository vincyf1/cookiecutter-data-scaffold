"""pytest-cookies auto-registers the `cookies` fixture; run_cmd, lint_project,
and bake_with are shared helpers used across tests/test_bake_*.py."""

import subprocess

# Pin the linter these tests invoke, so a bad CI day is never someone else's
# release. But keep the pin at or above what a generated project actually
# resolves: its dev group floats `ruff>=0.6` with no lockfile, so linting the
# bake with an *older* ruff than the user gets makes this test worthless --
# it passed for the whole stretch where every generated project was failing
# `uv run ruff check .` with 15 errors.
#
# What made those 15 possible was the generated pyproject selecting no rules
# at all, so its rule set was whatever the installed ruff defaulted to. That
# is fixed at the source now -- both pyproject.toml files select explicitly --
# which is why this pin no longer has to defend against default-set growth
# (the earlier DTZ001 surprise). Bump it when ruff releases; it should not
# lag the floor by much.
RUFF_VERSION = "0.16.1"

DEFAULT_FLAGS = {
    "include_batch": True,
    "include_streaming": True,
    "include_lakehouse": True,
    "include_dbt": True,
    "include_agentic": True,
}


def run_cmd(cwd, *args):
    # check=False, then assert: a CalledProcessError says only "exit 1", while
    # this assertion puts the command's actual stdout/stderr in the test report.
    result = subprocess.run(args, cwd=cwd, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def lint_project(project_path):
    """Lint a generated project with a pinned ruff."""
    return run_cmd(project_path, "uvx", f"ruff@{RUFF_VERSION}", "check", ".")


def bake_with(cookies, **flags):
    extra_context = {**DEFAULT_FLAGS, **flags}
    return cookies.bake(extra_context=extra_context)
