# Installation Guide

This guide covers everything needed to go from a clean machine to a running, generated data-engineering project using `cookiecutter-data-scaffold`.

## 1. Prerequisites

| Requirement | Version | Why |
| --- | --- | --- |
| Python | 3.12+ | Required by the template and by every generated project (`requires-python = ">=3.12"`) |
| [`uv`](https://docs.astral.sh/uv/) | latest | Dependency management for both this template's dev environment and generated projects |
| `git` | any recent | Cloning the template / generated project repos |
| Docker (optional) | any recent | Only needed if you enable the **Batch** or **Streaming** Patterns and want to run their demo services (Airflow standalone, Kafka) |

Verify your Python and `uv` versions before continuing:

```bash
python3 --version   # should print 3.12.x or higher
uv --version
```

If `uv` isn't installed, follow the [official install instructions](https://docs.astral.sh/uv/getting-started/installation/) — on macOS/Linux:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## 2. Installing `cookiecutter`

You don't need to clone this repository to use the template — `cookiecutter` can pull it directly from GitHub. You just need `cookiecutter` itself available somewhere.

**Recommended — isolated `uv` environment:**

```bash
uv venv
source .venv/bin/activate   # on Windows (PowerShell): .venv\Scripts\Activate.ps1
uv pip install cookiecutter
```

**Alternative — `pipx` (no environment to manage):**

```bash
pipx install cookiecutter
```

**Alternative — plain `pip`:**

```bash
pip install --user cookiecutter
```

Any of these gives you a working `cookiecutter` command for the next step.

## 3. Generating a project

### Interactive

```bash
cookiecutter gh:vincyf1/cookiecutter-data-scaffold
```

You'll be prompted for each variable in [`cookiecutter.json`](../cookiecutter.json) in turn. Press Enter to accept the default shown in brackets.

### Non-interactive

Skip all prompts and accept every default (all Patterns enabled):

```bash
cookiecutter gh:vincyf1/cookiecutter-data-scaffold --no-input
```

Override specific values with `extra_context` key=value pairs after `--no-input`:

```bash
cookiecutter gh:vincyf1/cookiecutter-data-scaffold --no-input \
  project_name="Orders Pipeline" \
  author_name="Jane Doe" \
  include_streaming=false \
  include_dbt=false
```

### Available variables

| Variable | Default | Description |
| --- | --- | --- |
| `project_name` | `My Data Project` | Human-readable project name; also derives `project_slug` (lowercased, spaces/dashes → underscores) |
| `author_name` | `Your Name` | Used in generated metadata |
| `include_batch` | `true` | Scaffold a scheduled/triggered Airflow batch DAG |
| `include_streaming` | `true` | Scaffold a streaming consumer + sinks |
| `include_lakehouse` | `true` | Scaffold a DuckDB-native lakehouse storage layer |
| `include_dbt` | `true` | Scaffold a dbt Transformation project |
| `dbt_duckdb_version` | `1.8` | Pinned `dbt-duckdb` version in the generated project |
| `sqlfluff_version` | `3.1.0` | Pinned `sqlfluff` version in the generated project |
| `airflow_version` | `3.2.0` | Pinned `apache-airflow` version (also used as the Docker image tag for the Batch demo service) |

Patterns are independent and combinable — disabling one never requires or excludes another. See the README's [Patterns](../README.md#patterns) section for what each one scaffolds.

## 4. Setting up the generated project

```bash
cd my-data-project      # or whatever project_slug resolved to
uv sync
uv run pytest
```

`uv sync` installs dependencies pinned to the versions you selected (or accepted) in step 3. `uv run pytest` runs the example tests scaffolded for whichever Patterns you enabled — a project with no Patterns enabled will have an empty test suite.

Generated projects also ship a `.pre-commit-config.yaml`. If you use `pre-commit`, install the hooks once:

```bash
uv run pre-commit install
```

## 5. Optional: running the Docker demo services

If you enabled **Batch** and/or **Streaming**, the generated project includes a `docker-compose.yml` with demo infrastructure:

- **Batch** → `airflow-standalone` (image `apache/airflow:<airflow_version>`), exposing the Airflow UI on `http://localhost:8080`, with your `src/` mounted so the example DAG is picked up automatically.
- **Streaming** → `kafka` (image `confluentinc/cp-kafka:7.6.0`), a single-broker KRaft cluster exposed on `localhost:9092`.

Start them from inside the generated project:

```bash
docker compose up
```

If neither Pattern is enabled, `docker-compose.yml` still exists but declares no services.

## 6. Developing the template itself

If you're contributing to `cookiecutter-data-scaffold` rather than generating a project from it, clone the repo directly:

```bash
git clone git@github.com:vincyf1/cookiecutter-data-scaffold.git
cd cookiecutter-data-scaffold
uv sync
uv run pytest
```

The test suite includes `slow` tests that perform a real `uv sync` and execute a generated project's dependencies. For a fast local loop, deselect them:

```bash
uv run pytest -m "not slow"
```

Also run lint checks before opening a PR:

```bash
uv run ruff check .
```

## 7. Troubleshooting

- **`cookiecutter: command not found`** — the environment where you installed it (step 2) isn't active. Re-run `source .venv/bin/activate`, or reinstall with `pipx`/`pip --user` and ensure that install location is on your `PATH`.
- **Python version errors during `uv sync`** — check `python3 --version`; both this template and generated projects require 3.12+. `uv` can install a matching interpreter for you with `uv python install 3.12`.
- **Port already in use (`8080` or `9092`)** — another process (often a previous `docker compose up`) is still bound to that port. Run `docker compose down` in the generated project, or stop the conflicting process, before retrying.
- **Stale or broken `.venv`** — delete it and resync: `rm -rf .venv && uv sync`.
- **Pattern's files missing or unexpected extras present** — double-check the flags you baked with (`include_batch`, `include_streaming`, `include_lakehouse`, `include_dbt`); each Pattern only scaffolds files declared for it in [`hooks/post_gen_project.py`](../hooks/post_gen_project.py).
