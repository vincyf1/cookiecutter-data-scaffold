<p align="center">
  <img src="assets/logo.png" alt="cookiecutter-data-scaffold logo" width="160">
</p>

<h1 align="center">cookiecutter-data-scaffold</h1>

<p align="center">
  A flexible, multi-pattern <a href="https://cookiecutter.readthedocs.io/">cookiecutter</a> template for Data Engineering projects.
  <br>
  Pick the Patterns you need — Batch, Streaming, Lakehouse, Transformation — and generate a ready-to-run project.
</p>

<p align="center">
  <a href="https://github.com/vincyf1/cookiecutter-data-scaffold/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.12%2B-blue" alt="Python 3.12+"></a>
  <a href="https://github.com/cookiecutter/cookiecutter"><img src="https://img.shields.io/badge/built%20with-cookiecutter-D4AA00.svg" alt="Built with Cookiecutter"></a>
  <a href="https://github.com/vincyf1/cookiecutter-data-scaffold/issues"><img src="https://img.shields.io/github/issues/vincyf1/cookiecutter-data-scaffold" alt="Open issues"></a>
  <a href="https://github.com/vincyf1/cookiecutter-data-scaffold/stargazers"><img src="https://img.shields.io/github/stars/vincyf1/cookiecutter-data-scaffold?style=social" alt="GitHub stars"></a>
  <a href="#contributing"><img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" alt="PRs Welcome"></a>
</p>

---

## Introduction

Most data engineering templates force an early, exclusive choice: "batch project" *or* "streaming project" *or* "lakehouse project." In practice, real projects mix and match — a batch job that lands into a lakehouse, a streaming consumer transformed by dbt, or all of the above.

`cookiecutter-data-scaffold` models these as independent, composable **Patterns**. You toggle the ones you need and get a generated project scaffold with only that code — no dead branches, no commented-out placeholders for patterns you didn't pick.

## Features

- **Composable Patterns, not a single project type** — enable any combination of:
  - **Batch** — a scheduled/triggered ingestion pipeline (Airflow DAG)
  - **Streaming** — a continuous ingestion pipeline (consumer + sinks)
  - **Lakehouse** — a DuckDB-native storage/table layer that Batch and/or Streaming write into
  - **Transformation** — a dbt project for reshaping data already at rest
- **Clean generation, no leftovers** — disabling a Pattern removes its files entirely rather than leaving unused code behind, verified by a canonical Pattern manifest (`hooks/post_gen_project.py`) that's cross-checked against the template tree in CI-style tests.
- **Modern tooling out of the box** — [`uv`](https://docs.astral.sh/uv/) for dependency management, `ruff` for linting, `pytest` for tests, `pre-commit` hooks, and pinned versions for dbt-duckdb, sqlfluff, and Airflow.
- **Sensible defaults** — a working example DAG, consumer, and dbt staging model are generated so you have something runnable on day one, not just empty folders.

## Quickstart

Requires Python 3.12+ and [cookiecutter](https://cookiecutter.readthedocs.io/) 2.2+.

```bash
pip install cookiecutter
cookiecutter gh:vincyf1/cookiecutter-data-scaffold
```

You'll be prompted for your project name, author, and which Patterns to include. To skip the prompts and accept the defaults (all Patterns enabled):

```bash
cookiecutter gh:vincyf1/cookiecutter-data-scaffold --no-input
```

Then, inside the generated project:

```bash
cd my-data-project
uv sync
uv run pytest
```

## Detailed Usage

### Template options

These are the variables you'll be prompted for (see [`cookiecutter.json`](cookiecutter.json)):

| Variable            | Default          | Description                                             |
| -------------------- | ---------------- | -------------------------------------------------------- |
| `project_name`        | `My Data Project` | Human-readable project name                              |
| `author_name`         | `Your Name`       | Used in generated metadata                                |
| `include_batch`       | `true`            | Scaffold a scheduled Airflow batch DAG                    |
| `include_streaming`   | `true`            | Scaffold a streaming consumer + sinks                     |
| `include_lakehouse`   | `true`            | Scaffold a DuckDB-native lakehouse storage layer           |
| `include_dbt`         | `true`            | Scaffold a dbt Transformation project                      |
| `dbt_duckdb_version`  | `1.8`             | Pinned `dbt-duckdb` version                                |
| `sqlfluff_version`    | `3.1.0`           | Pinned `sqlfluff` version                                  |
| `airflow_version`     | `3.2.0`           | Pinned `apache-airflow` version                            |

Generate non-interactively with specific flags via `--no-input` and `extra_context`:

```bash
cookiecutter gh:vincyf1/cookiecutter-data-scaffold --no-input \
  project_name="Orders Pipeline" \
  include_streaming=false \
  include_dbt=false
```

### Patterns

Patterns are independent and combinable — enabling or disabling one never requires or excludes another:

- **Batch** scaffolds `src/<slug>/batch/`, an example Airflow DAG, and its tests.
- **Streaming** scaffolds `src/<slug>/streaming/`, a consumer and sinks, and its tests.
- **Lakehouse** scaffolds `src/<slug>/lakehouse/`, DuckDB table DDL, and a writer that Batch/Streaming sinks write through — it's a storage layer, not an ingestion mechanism.
- **Transformation** scaffolds `transformation/`, a dbt project with a staging model, and defaults to its own local DuckDB target so it works even without Lakehouse enabled.

### Running the template's own test suite

If you're developing the template itself (not a generated project):

```bash
git clone git@github.com:vincyf1/cookiecutter-data-scaffold.git
cd cookiecutter-data-scaffold
uv sync
uv run pytest
```

Slow tests (`-m slow`) run a real `uv sync` and execute a generated project's dependencies — deselect them for a fast loop:

```bash
uv run pytest -m "not slow"
```

## Feedback

Found a bug, have a Pattern idea, or something feels off? Please [open an issue](https://github.com/vincyf1/cookiecutter-data-scaffold/issues/new) — include the flags you baked with and what you expected to see.

## Contributing

Contributions are welcome, from typo fixes to new Patterns.

1. Fork the repo and create a branch for your change.
2. Run `uv run pytest` and `uv run ruff check .` before opening a PR.
3. If you add or change a Pattern, update the manifest in [`hooks/post_gen_project.py`](hooks/post_gen_project.py) and add coverage in `tests/test_bake_*.py` — `tests/test_pattern_manifest.py` will fail the build if a Pattern's files aren't declared there.
4. Open a PR describing the change and why it's needed.

See [`CONTEXT.md`](CONTEXT.md) for the project's domain language (what "Pattern" means, how Patterns relate to each other) before naming anything new.

## License

Distributed under the [MIT License](LICENSE).
