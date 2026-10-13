import os
import shutil

PROJECT_DIR = os.path.realpath(os.curdir)
SLUG = {{ cookiecutter.project_slug | tojson }}
INCLUDE_BATCH = {{ cookiecutter.include_batch }}
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
            "logs",
        ],
        "gated": [
            "README.md",
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
        ],
        "gated": [
            "README.md",
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
            f"src/{SLUG}/agentic/create_traces_table.sql",
        ],
        "gated": [
            "README.md",
            "docker-compose.yml",
            "pyproject.toml",
            "tests/test_parquet_sink.py",
            "tests/test_batch_dag.py",
            "tests/test_events.py",
            f"src/{SLUG}/sinks.py",
            f"src/{SLUG}/batch/dags/example_dag.py",
            f"src/{SLUG}/streaming/consumer.py",
            f"src/{SLUG}/agentic/tracing.py",
            "tests/test_agentic_tracing.py",
            "tests/test_agentic_healing.py",
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
    {
        "flag_name": "include_agentic",
        "flag": {{ cookiecutter.include_agentic }},
        "remove": [
            f"src/{SLUG}/agentic",
            "tests/test_agentic_contracts.py",
            "tests/test_agentic_tracing.py",
            "tests/test_agentic_tools.py",
            "tests/test_agentic_runner.py",
            "tests/test_agentic_publisher.py",
            "tests/test_agentic_healing.py",
            "evals",
            "artifacts",
        ],
        "gated": [
            "pyproject.toml",
            f"src/{SLUG}/batch/dags/example_dag.py",
            "tests/test_batch_dag.py",
            "docker-compose.yml",
            ".github/workflows/ci.yml",
            "README.md",
            ".gitignore",
        ],
    },
]

# Files whose existence depends on a *combination* of flags, so they belong
# to no single Pattern entry. Each item is (keep_it?, paths).
COMPOUND_REMOVALS = [
    # src/{slug}/sinks.py is shared by the Batch and Streaming callers (not a
    # Pattern of its own), so it's dropped only when neither caller survives.
    ({{ cookiecutter.include_batch or cookiecutter.include_streaming }}, [
        f"src/{SLUG}/sinks.py",
        "tests/test_parquet_sink.py",
    ]),
    ({{ cookiecutter.include_batch or cookiecutter.include_streaming or cookiecutter.include_lakehouse }}, [
        f"src/{SLUG}/events.py",
        "tests/test_events.py",
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


def write_airflow_env():
    """Pin AIRFLOW_UID to this host's uid in a .env docker compose will read.

    The airflow image runs as uid 50000, but ./logs and ./artifacts are
    bind-mounted host directories that keep their host ownership. On Linux
    that uid cannot write to them, so the demo's failing task never produces
    a log the healing agent can read, and the healing task never produces a
    patch.

    Compose's airflow-init service is what guarantees those mounts are
    writable, so this file is not what makes `docker compose up` work -- it
    is gitignored and absent on any machine that did not run cookiecutter.
    What it buys is ownership: with the host uid pinned here, init's chown is
    a no-op and the logs and patches stay readable and deletable as you,
    rather than as uid 50000. See the README for how to refresh it elsewhere.
    """
    if not INCLUDE_BATCH or not hasattr(os, "getuid"):
        # No airflow service means no uid to pin; and on Windows there is no
        # uid to read, where the image's own 50000 is already correct.
        return
    env_path = os.path.join(PROJECT_DIR, ".env")
    with open(env_path, "w") as f:
        f.write(
            "# Written at generation time by cookiecutter-data-scaffold, and\n"
            "# specific to the machine that generated this project. docker\n"
            "# compose reads this file automatically.\n"
            "#\n"
            "# The airflow image runs as uid 50000; the ./logs and ./artifacts\n"
            "# bind mounts keep their host ownership. Pinning the host uid is\n"
            "# what keeps those mounts writable from inside the container.\n"
            f"AIRFLOW_UID={os.getuid()}\n"
        )


def main():
    sync_lakehouse_ddl()
    write_airflow_env()
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
