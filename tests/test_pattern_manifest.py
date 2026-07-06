"""Verifies hooks/post_gen_project.py's PATTERNS manifest is the single,
accurate source of truth for which template files a Pattern flag gates.

Each Pattern is checked through two independent mechanisms: Jinja
conditionals gate content inside files that survive, and PATTERNS'
"remove" lists prune whole files/dirs post-render. This test renders the
manifest and cross-checks its "gated" declarations against every
`cookiecutter.include_x` site actually present in the template tree, so a
new Pattern (or a forgotten/stale Jinja gate) fails here instead of
silently drifting out of agreement.
"""

from pathlib import Path

import jinja2

REPO_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE_ROOT = REPO_ROOT / "{{cookiecutter.project_slug}}"
HOOK_PATH = REPO_ROOT / "hooks" / "post_gen_project.py"
PLACEHOLDER_SLUG = "{{cookiecutter.project_slug}}"


def _load_patterns():
    context = {
        "cookiecutter": {
            "project_slug": "manifest_test_slug",
            "include_batch": True,
            "include_streaming": True,
            "include_lakehouse": True,
            "include_dbt": True,
        }
    }
    rendered = jinja2.Template(HOOK_PATH.read_text()).render(**context)
    namespace = {"__name__": "test_pattern_manifest_hook"}
    exec(compile(rendered, str(HOOK_PATH), "exec"), namespace)
    return namespace["PATTERNS"], namespace["SLUG"]


def _to_repo_relative(generated_path, slug):
    raw = generated_path.replace(slug, PLACEHOLDER_SLUG)
    return PLACEHOLDER_SLUG + "/" + raw


def _files_gating(flag_name):
    needle = "cookiecutter." + flag_name
    return {
        str(path.relative_to(REPO_ROOT))
        for path in TEMPLATE_ROOT.rglob("*")
        if path.is_file() and needle in path.read_text(errors="ignore")
    }


def test_pattern_manifest_gated_files_match_template_tree():
    patterns, slug = _load_patterns()
    for pattern in patterns:
        declared = {_to_repo_relative(path, slug) for path in pattern["gated"]}
        actual = _files_gating(pattern["flag_name"])
        assert declared == actual, (
            f"{pattern['flag_name']}: PATTERNS declares 'gated' files {declared} "
            f"but the template tree actually gates on cookiecutter.{pattern['flag_name']} "
            f"in {actual} — update hooks/post_gen_project.py's PATTERNS manifest"
        )
