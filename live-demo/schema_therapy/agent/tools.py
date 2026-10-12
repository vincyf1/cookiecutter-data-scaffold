"""Concrete, narrowly scoped tools exposed to the demo agent."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]*\Z")
LOG_READER = """from pathlib import Path
import sys
dag, task = sys.argv[1:]
root = Path('/opt/airflow/logs') / ('dag_id=' + dag)
files = list(root.glob('run_id=*/task_id=' + task + '/attempt=*.log'))
if not files:
    raise SystemExit('No task logs found')
latest = max(files, key=lambda p: p.stat().st_mtime)
print('log_file=' + str(latest))
print(''.join(latest.read_text(errors='replace').splitlines(keepends=True)[-200:]))
"""


class DemoTools:
    def __init__(
        self,
        project: Path,
        *,
        runner=subprocess.run,
        client=None,
        request="",
        model=None,
        allow_rerun=False,
    ):
        self.project = project.resolve()
        self.dbt_project = self.project / "transformation"
        self.runner = runner
        self.client = client
        self.request = request
        self.model = model or os.environ.get("OPENAI_MODEL")
        self.allow_rerun = allow_rerun

    def _run(self, args: list[str], *, cwd: Path | None = None) -> dict:
        try:
            result = self.runner(
                args, cwd=cwd or self.project, capture_output=True, text=True, timeout=120
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return {"exit_code": None, "error": str(exc)}
        return {
            "exit_code": result.returncode,
            "stdout": result.stdout[-20000:],
            "stderr": result.stderr[-4000:],
        }

    @staticmethod
    def _name(value: str) -> str:
        if not NAME.fullmatch(value):
            raise ValueError("Name must be a simple DAG, task, dataset, or model identifier")
        return value

    @staticmethod
    def _json_output(result: dict) -> list:
        if result["exit_code"] != 0:
            raise RuntimeError(result.get("stderr") or result.get("stdout") or result.get("error"))
        for line in reversed(result["stdout"].splitlines()):
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, list):
                return value
        raise RuntimeError("Airflow returned no JSON list")

    def _airflow(self, *args: str) -> dict:
        return self._run(
            ["docker", "compose", "exec", "-T", "airflow-standalone", "airflow", *args]
        )

    def _manifest(self) -> dict:
        path = self.dbt_project / "target/manifest.json"
        if not path.exists():
            raise FileNotFoundError("dbt manifest missing; run dbt parse in transformation/")
        return json.loads(path.read_text())

    def _model(self, name: str) -> dict:
        self._name(name)
        key = f"model.schema_therapy.{name}"
        node = self._manifest()["nodes"].get(key)
        if node is None:
            raise ValueError(f"Unknown dbt model: {name}")
        return node

    def get_dag_status(self, dag_id: str) -> dict:
        self._name(dag_id)
        runs = self._json_output(self._airflow("dags", "list-runs", dag_id, "-o", "json"))
        return {"dag_id": dag_id, "latest_run": runs[0] if runs else None, "recent_runs": runs[:5]}

    def get_task_logs(self, dag_id: str, task_id: str) -> dict:
        self._name(dag_id)
        self._name(task_id)
        result = self._run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "airflow-standalone",
                "python",
                "-c",
                LOG_READER,
                dag_id,
                task_id,
            ]
        )
        return {"dag_id": dag_id, "task_id": task_id, **result}

    def get_recent_schema_changes(self, dataset: str) -> dict:
        self._name(dataset)
        import duckdb

        table = (
            dataset
            if dataset.startswith("raw_") or dataset == "customer_revenue_daily"
            else f"raw_{dataset}"
        )
        db_path = self.project / "data/lakehouse.duckdb"
        columns = []
        if db_path.exists():
            with duckdb.connect(str(db_path), read_only=True) as db:
                columns = [
                    row[0]
                    for row in db.execute(
                        "select column_name from information_schema.columns where table_name = ? order by ordinal_position",
                        [table],
                    ).fetchall()
                ]
        migrations = []
        for path in sorted((self.project / "supabase/migrations").glob("*.sql"), reverse=True):
            if dataset.removeprefix("raw_") in path.read_text():
                migrations.append(
                    {"file": str(path.relative_to(self.project)), "sql": path.read_text()[:8000]}
                )
        return {
            "dataset": dataset,
            "current_columns": columns,
            "migrations": migrations[:5],
            "history_available": False,
            "note": "Migration files show declared changes; no schema snapshots or Git history are available to date live changes.",
        }

    def get_lineage(self, model: str) -> dict:
        node = self._model(model)
        manifest = self._manifest()
        key = node["unique_id"]
        parents = manifest.get("parent_map", {}).get(key, [])
        children = manifest.get("child_map", {}).get(key, [])
        return {
            "model": model,
            "upstream_models": [
                item.rsplit(".", 1)[-1] for item in parents if item.startswith("model.")
            ],
            "upstream_sources": [
                item.removeprefix("source.schema_therapy.")
                for item in parents
                if item.startswith("source.")
            ],
            "downstream_models": [
                item.rsplit(".", 1)[-1] for item in children if item.startswith("model.")
            ],
        }

    def get_recent_git_changes(self, repo: str) -> dict:
        candidate = (
            self.project
            if repo in {".", "schema_therapy"}
            else (self.project.parent / repo).resolve()
        )
        if candidate != self.project and candidate != self.project.parent:
            return {"error": "repo_outside_demo", "repo": repo}
        check = self._run(["git", "rev-parse", "--show-toplevel"], cwd=candidate)
        if check["exit_code"] != 0:
            return {"error": "not_a_git_repository", "repo": repo}
        return self._run(
            [
                "git",
                "log",
                "-n",
                "10",
                "--date=iso-strict",
                "--pretty=format:%h %ad %s",
                "--name-status",
            ],
            cwd=candidate,
        )

    def read_dbt_model(self, model: str) -> dict:
        node = self._model(model)
        path = (self.dbt_project / node["original_file_path"]).resolve()
        if not path.is_relative_to(self.dbt_project / "models"):
            raise ValueError("Model path is outside the dbt models directory")
        return {
            "model": model,
            "path": str(path.relative_to(self.project)),
            "sql": path.read_text(),
        }

    def propose_patch(self, model: str) -> dict:
        source = self.read_dbt_model(model)
        if self.client is None or not self.model:
            raise RuntimeError("OpenAI client and OPENAI_MODEL are required to propose a patch")
        response = self.client.responses.create(
            model=self.model,
            instructions="Propose a minimal unified diff for the dbt SQL model. Return only the diff. Do not invent schema columns or edit files.",
            input=f"Current user request: {self.request}\nModel path: {source['path']}\nSQL:\n{source['sql']}",
        )
        patch = response.output_text.strip()
        if not patch.startswith("--- ") or "\n+++ " not in patch:
            raise RuntimeError("Model did not return a unified diff")
        return {"model": model, "patch": patch, "applied": False}

    def run_dbt_tests(self, model: str) -> dict:
        self._model(model)
        dbt_executable = Path(sys.executable).with_name("dbt")
        return self._run(
            [
                str(dbt_executable),
                "test",
                "--project-dir",
                str(self.dbt_project),
                "--profiles-dir",
                str(self.dbt_project),
                "--select",
                model,
            ],
            cwd=self.dbt_project,
        )

    def rerun_airflow_task(self, dag_id: str, task_id: str) -> dict:
        if not self.allow_rerun:
            raise PermissionError("Rerun requires the --allow-rerun CLI flag")
        self._name(dag_id)
        self._name(task_id)
        tasks = self._airflow("tasks", "list", dag_id)
        if tasks["exit_code"] != 0 or task_id not in tasks["stdout"].splitlines():
            raise ValueError(f"Unknown task {task_id} in DAG {dag_id}")
        runs = self._json_output(self._airflow("dags", "list-runs", dag_id, "-o", "json"))
        if not runs:
            raise ValueError(f"No DAG runs to rerun for {dag_id}")
        latest = runs[0]
        date = latest["logical_date"]
        result = self._airflow(
            "tasks", "clear", dag_id, "-t", f"^{re.escape(task_id)}$", "-s", date, "-e", date, "-y"
        )
        return {"dag_id": dag_id, "task_id": task_id, "run_id": latest["run_id"], **result}
