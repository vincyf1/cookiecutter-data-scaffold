import os
import shutil

PROJECT_DIR = os.path.realpath(os.curdir)
SLUG = {{ cookiecutter.project_slug | tojson }}
INCLUDE_LAKEHOUSE = {{ cookiecutter.include_lakehouse }}
INCLUDE_DBT = {{ cookiecutter.include_dbt }}

# Canonical per-Pattern manifest. Every file a Pattern flag touches is
# declared under exactly one Pattern entry, under one of two keys:
#   - "remove": whole files/dirs deleted here when the flag is False.
#   - "gated": files that are kept either way but contain their own Jinja
#     conditional branch on this flag, so their content still varies.
# tests/test_pattern_manifest.py renders this dict and cross-checks "gated"
# against every `cookiecutter.include_x` site actually found in the
# template tree, so a new Pattern (or a forgotten Jinja gate) fails a test
# instead of silently drifting.
PATTERNS = [
    {
        "flag_name": "include_batch",
        "flag": {{ cookiecutter.include_batch }},
        "remove": [
            f"src/{SLUG}/batch",
            "tests/test_batch_dag.py",
        ],
        "gated": [
            "docker-compose.yml",
            "pyproject.toml",
        ],
    },
    {
        "flag_name": "include_streaming",
        "flag": {{ cookiecutter.include_streaming }},
        "remove": [
            f"src/{SLUG}/streaming",
            "tests/test_streaming_consumer.py",
            "tests/test_streaming_sinks.py",
        ],
        "gated": [
            "docker-compose.yml",
            "pyproject.toml",
        ],
    },
    {
        "flag_name": "include_lakehouse",
        "flag": {{ cookiecutter.include_lakehouse }},
        "remove": [
            f"src/{SLUG}/lakehouse",
            "tests/test_lakehouse_writer.py",
            "transformation/models/staging/sources.yml",
        ],
        "gated": [
            "docker-compose.yml",
            "pyproject.toml",
            f"src/{SLUG}/sinks.py",
            f"src/{SLUG}/batch/sinks.py",
            f"src/{SLUG}/streaming/sinks.py",
            "tests/test_streaming_sinks.py",
            "transformation/dbt_project.yml",
            "transformation/models/staging/stg_events.sql",
            "transformation/profiles.yml",
        ],
    },
    {
        "flag_name": "include_dbt",
        "flag": {{ cookiecutter.include_dbt }},
        "remove": [
            "transformation",
            ".sqlfluff",
        ],
        "gated": [
            ".github/workflows/ci.yml",
            ".pre-commit-config.yaml",
            "pyproject.toml",
        ],
    },
]

# src/{slug}/sinks.py is shared by the Batch and Streaming callers (not a
# Pattern of its own), so it's dropped only when neither caller survives.
COMPOUND_REMOVALS = [
    ({{ cookiecutter.include_batch or cookiecutter.include_streaming }}, [
        f"src/{SLUG}/sinks.py",
    ]),
]


def remove(relative_path):
    full_path = os.path.join(PROJECT_DIR, relative_path)
    if os.path.isdir(full_path):
        shutil.rmtree(full_path)
    elif os.path.isfile(full_path):
        os.remove(full_path)


def sync_lakehouse_ddl():
    """Copy create_tables.sql's DDL into dbt_project.yml's on-run-start hook.

    create_tables.sql is the schema's only hand-authored source; this keeps
    dbt's copy from drifting out of sync with it after each bake.
    """
    if not (INCLUDE_LAKEHOUSE and INCLUDE_DBT):
        return
    ddl_path = os.path.join(PROJECT_DIR, "src", SLUG, "lakehouse", "create_tables.sql")
    dbt_project_path = os.path.join(PROJECT_DIR, "transformation", "dbt_project.yml")
    with open(ddl_path) as f:
        statement = " ".join(f.read().split()).rstrip(";")
    with open(dbt_project_path) as f:
        content = f.read()
    content = content.replace("__LAKEHOUSE_CREATE_TABLE_DDL__", statement)
    with open(dbt_project_path, "w") as f:
        f.write(content)


def main():
    sync_lakehouse_ddl()
    for pattern in PATTERNS:
        if not pattern["flag"]:
            for path in pattern["remove"]:
                remove(path)
    for keep, paths in COMPOUND_REMOVALS:
        if not keep:
            for path in paths:
                remove(path)


if __name__ == "__main__":
    main()
