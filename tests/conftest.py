"""pytest-cookies auto-registers the `cookies` fixture; run_cmd and bake_with
are shared helpers used across tests/test_bake_*.py."""

import subprocess

DEFAULT_FLAGS = {
    "include_batch": True,
    "include_streaming": True,
    "include_lakehouse": True,
    "include_dbt": True,
}


def run_cmd(cwd, *args):
    result = subprocess.run(args, cwd=cwd, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def bake_with(cookies, **flags):
    extra_context = {**DEFAULT_FLAGS, **flags}
    return cookies.bake(extra_context=extra_context)
