"""pytest-cookies auto-registers the `cookies` fixture; run_cmd, lint_project,
and bake_with are shared helpers used across tests/test_bake_*.py."""

import subprocess

# Pin the linter these tests invoke. A bare `uvx ruff` resolves to whatever
# ruff released most recently, so a new default rule turns every lint test
# red with no change on our side -- which is exactly what happened when
# ruff 0.16 enabled DTZ001. Keep this in sync with the `ruff` floor in
# pyproject.toml's dev dependency group.
RUFF_VERSION = "0.15.20"

DEFAULT_FLAGS = {
    "include_batch": True,
    "include_streaming": True,
    "include_lakehouse": True,
    "include_dbt": True,
    "include_agentic": True,
}


def run_cmd(cwd, *args):
    result = subprocess.run(args, cwd=cwd, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def lint_project(project_path):
    """Lint a generated project with a pinned ruff."""
    return run_cmd(project_path, "uvx", f"ruff@{RUFF_VERSION}", "check", ".")


def bake_with(cookies, **flags):
    extra_context = {**DEFAULT_FLAGS, **flags}
    return cookies.bake(extra_context=extra_context)
