"""pytest-cookies auto-registers the `cookies` fixture; run_cmd is shared
subprocess-running helper used across tests/test_bake_integration.py."""

import subprocess


def run_cmd(cwd, *args):
    result = subprocess.run(args, cwd=cwd, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    return result
