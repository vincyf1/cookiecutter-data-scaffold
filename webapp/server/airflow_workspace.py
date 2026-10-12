"""Staged, version-checked edits and deployment to the demo's mounted Airflow bundle."""

import ast
import difflib
import fcntl
import hashlib
import json
import os
import subprocess
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

DAG = "customer_revenue_daily"
DAG_FILE = f"src/schema_therapy/batch/dags/{DAG}.py"
FIXED_FILES = {
    DAG_FILE,
    "src/schema_therapy/batch/ingest.py",
    "src/schema_therapy/lakehouse/writer.py",
    "src/schema_therapy/lakehouse/create_tables.sql",
    "tests/test_batch_dag.py",
    "tests/test_customer_ingestion.py",
    "tests/test_lakehouse_writer.py",
}
MAX_BYTES = 60000

# Run a disposable copy inside the actual Airflow image. Never run tasks or dbt builds.
# Credentials are removed before importing edited Python; dbt parse uses an empty temp DB.
RUNTIME_CHECK = r'''
import ast, hashlib, json, os, shutil, subprocess, sys, tempfile
from pathlib import Path
payload = json.load(sys.stdin)
project = Path('/opt/airflow/project')
if payload['mode'] == 'status':
    hashes = {}
    for name in payload['files']:
        path = project / name
        if name.startswith('src/schema_therapy/batch/dags/'):
            path = Path('/opt/airflow/dags') / Path(name).name
        hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    print(json.dumps({'file_hashes': hashes}))
else:
    os.environ.pop('SUPABASE_DB_URL', None)
    with tempfile.TemporaryDirectory(prefix='schema-therapy-validate-') as folder:
        root = Path(folder)
        ignore = shutil.ignore_patterns('__pycache__', 'target', 'logs', '.git')
        for name in ['src', 'transformation']:
            shutil.copytree(project / name, root / name, ignore=ignore)
        for name, content in payload['changes'].items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        for path in (root / 'src').rglob('*.py'):
            ast.parse(path.read_text(), filename=str(path))
        sys.path.insert(0, str(root / 'src'))
        os.environ['LAKEHOUSE_DB_PATH'] = str(root / 'validation.duckdb')
        os.environ['DBT_PROJECT_DIR'] = str(root / 'transformation')
        from airflow.dag_processing.dagbag import DagBag
        bag = DagBag(dag_folder=str(root / payload['dag_file']), include_examples=False, safe_mode=False)
        if bag.import_errors:
            raise RuntimeError('DAG import errors: ' + json.dumps(bag.import_errors))
        dag = bag.dags.get('customer_revenue_daily')
        if dag is None:
            raise RuntimeError('customer_revenue_daily is missing')
        expected = {'ingest_supabase', 'transform_with_dbt'}
        if set(dag.task_ids) != expected:
            raise RuntimeError('Keep the demo task IDs: ingest_supabase and transform_with_dbt')
        if dag.get_task('transform_with_dbt').upstream_task_ids != {'ingest_supabase'}:
            raise RuntimeError('dbt must depend on ingestion')
        profile = root / 'profiles'
        profile.mkdir()
        (profile / 'profiles.yml').write_text('schema_therapy:\n  target: validation\n  outputs:\n    validation:\n      type: duckdb\n      path: ' + str(root / 'validation.duckdb') + '\n      threads: 1\n')
        parsed = subprocess.run(['dbt', 'parse', '--no-partial-parse', '--project-dir', str(root / 'transformation'), '--profiles-dir', str(profile)], capture_output=True, text=True, timeout=90)
        if parsed.returncode:
            raise RuntimeError('dbt parse failed: ' + (parsed.stdout + parsed.stderr)[-6000:])
        print(json.dumps({'valid': True, 'dag_id': dag.dag_id, 'tasks': sorted(dag.task_ids), 'dbt_parse': 'passed', 'note': 'Imports and dbt parsing passed. Tasks and database migrations were not executed.'}))
'''


def digest(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()


class AirflowWorkspace:
    def __init__(self, project: Path, state_dir: Path, *, runner=subprocess.run):
        self.project = project.resolve()
        self.state_dir = state_dir
        self.runner = runner

    @contextmanager
    def _locked(self):
        self.state_dir.mkdir(parents=True, exist_ok=True)
        with (self.state_dir / ".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    def _files(self) -> list[str]:
        paths = {name for name in FIXED_FILES if (self.project / name).is_file()}
        for path in (self.project / "transformation/models").rglob("*"):
            if path.is_file() and path.suffix in {".sql", ".yml", ".yaml"}:
                paths.add(str(path.relative_to(self.project)))
        return sorted(paths)

    def _path(self, name: str) -> Path:
        if not isinstance(name, str) or name not in self._files():
            raise ValueError("Path is not an editable downstream demo file; list_airflow_files first")
        path = self.project / name
        if path.resolve() != path or any(part.is_symlink() for part in [path, *path.parents] if part != self.project.parent):
            raise ValueError("Editable paths must not contain a symlink")
        return path

    @staticmethod
    def _dag(dag_id: str):
        if dag_id != DAG:
            raise ValueError("Only customer_revenue_daily is available")

    def _state(self):
        path = self.state_dir / "workspace.json"
        return json.loads(path.read_text()) if path.exists() else {"changes": {}, "last_deployment": None}

    def _write(self, path: Path, content: str):
        mode = path.stat().st_mode & 0o777 if path.exists() else 0o600
        with NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as file:
            temporary = Path(file.name)
            try:
                file.write(content)
                file.flush()
                os.fsync(file.fileno())
                temporary.chmod(mode)
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)

    def _save(self, state):
        self._write(self.state_dir / "workspace.json", json.dumps(state))

    @staticmethod
    def _revision(state) -> str:
        return digest(json.dumps(state["changes"], sort_keys=True))

    def _check_bases(self, state):
        for name, change in state["changes"].items():
            if digest(self._path(name).read_text()) != change["base_sha256"]:
                raise ValueError(f"{name} changed since staging; inspect and restage the patch")

    def list_airflow_files(self, dag_id: str):
        self._dag(dag_id)
        with self._locked():
            state = self._state()
            return {"dag_id": DAG, "files": self._files(), "staged_files": sorted(state["changes"]),
                    "workspace_revision": self._revision(state), "deployment_mode": "Docker bind mounts; staged edits are not live until deployed"}

    def read_airflow_file(self, path: str):
        with self._locked():
            live = self._path(path).read_text()
            if len(live.encode()) > MAX_BYTES:
                raise ValueError("File exceeds the editable size limit")
            change = self._state()["changes"].get(path)
            content = change["content"] if change else live
            return {"path": path, "content": content, "sha256": digest(content), "staged": bool(change), "live_sha256": digest(live)}

    def edit_airflow_file(self, path: str, expected_sha256: str, old_text: str, new_text: str):
        with self._locked():
            live = self._path(path).read_text()
            state = self._state()
            self._check_bases(state)
            content = state["changes"].get(path, {}).get("content", live)
            if digest(content) != expected_sha256:
                raise ValueError("File changed; read it again before editing")
            if not isinstance(old_text, str) or not old_text or content.count(old_text) != 1:
                raise ValueError("old_text must match exactly once; include enough surrounding context")
            if not isinstance(new_text, str):
                raise ValueError("new_text must be a string")
            edited = content.replace(old_text, new_text, 1)
            if len(edited.encode()) > MAX_BYTES:
                raise ValueError("Edited file exceeds the size limit")
            if path.endswith(".py"):
                ast.parse(edited, filename=path)
            if edited == live:
                state["changes"].pop(path, None)
            else:
                state["changes"][path] = {"content": edited, "base_sha256": digest(live)}
            self._save(state)
            diff = "".join(difflib.unified_diff(live.splitlines(True), edited.splitlines(True), fromfile=f"live/{path}", tofile=f"staged/{path}"))
            return {"path": path, "sha256": digest(edited), "workspace_revision": self._revision(state), "diff": diff,
                    "staged": edited != live, "deployed": False}

    def _run(self, args: list[str], *, payload=None):
        result = self.runner(["docker", "compose", "exec", "-T", "airflow-standalone", *args],
                             cwd=self.project, input=json.dumps(payload) if payload is not None else None,
                             capture_output=True, text=True, timeout=120)
        if result.returncode:
            raise RuntimeError((result.stderr + result.stdout)[-8000:] or "Airflow command failed")
        if payload is None:
            return {"exit_code": 0, "stdout": result.stdout[-4000:]}
        for line in reversed(result.stdout.splitlines()):
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                return value
        raise RuntimeError("Airflow runtime returned no JSON result")

    def _validate(self, state):
        self._check_bases(state)
        result = self._run(["python", "-c", RUNTIME_CHECK], payload={
            "mode": "validate", "dag_file": DAG_FILE,
            "changes": {name: change["content"] for name, change in state["changes"].items()},
        })
        if result.get("valid") is not True:
            raise RuntimeError("Airflow runtime validation did not pass")
        return result

    def validate_airflow_deployment(self, dag_id: str):
        self._dag(dag_id)
        with self._locked():
            state = self._state()
            return {**self._validate(state), "workspace_revision": self._revision(state), "staged_files": sorted(state["changes"])}

    def discard_airflow_changes(self, dag_id: str, workspace_revision: str):
        self._dag(dag_id)
        with self._locked():
            state = self._state()
            if self._revision(state) != workspace_revision:
                raise ValueError("Workspace revision changed; inspect it before discarding")
            discarded = sorted(state["changes"])
            state["changes"] = {}
            self._save(state)
            return {"discarded_files": discarded, "workspace_revision": self._revision(state), "message": "Staged edits discarded. Live files were not changed."}

    def _runtime_status(self, files):
        mounted = [name for name in files if not name.startswith("tests/")]
        result = self._run(["python", "-c", RUNTIME_CHECK], payload={"mode": "status", "files": mounted})
        expected = {name: digest(self._path(name).read_text()) for name in mounted}
        local_tests = {name: digest(self._path(name).read_text()) for name in files if name.startswith("tests/")}
        return {"runtime_files_match": result.get("file_hashes") == expected,
                "file_hashes": {**result.get("file_hashes", {}), **local_tests}, "local_test_files": sorted(local_tests)}

    def deploy_airflow_changes(self, dag_id: str, workspace_revision: str):
        self._dag(dag_id)
        with self._locked():
            state = self._state()
            if self._revision(state) != workspace_revision:
                raise ValueError("Workspace revision changed; validate the current patch again")
            if not state["changes"]:
                raise ValueError("No staged changes to deploy")
            validation = self._validate(state)
            # Catch a presenter edit that happened during the runtime validation.
            self._check_bases(state)
            before = {name: self._path(name).read_text() for name in state["changes"]}
            published = []
            try:
                for name, change in state["changes"].items():
                    self._write(self._path(name), change["content"])
                    published.append(name)
                runtime = self._runtime_status(sorted(before))
                if not runtime["runtime_files_match"]:
                    raise RuntimeError("Docker mounts do not expose the deployed files")
                refresh = self._run(["airflow", "dags", "reserialize", "--bundle-name=dags-folder"])
            except Exception as exc:
                for name in published:
                    self._write(self._path(name), before[name])
                try:
                    self._run(["airflow", "dags", "reserialize", "--bundle-name=dags-folder"])
                    restored = "Live files restored and Airflow refreshed."
                except Exception:
                    restored = "Live files restored; Airflow refresh failed. Check deployment status before running."
                raise RuntimeError(f"Deployment failed. {restored} Staged patch retained. {exc}") from exc
            deployment = {"dag_id": DAG, "deployed": True, "workspace_revision": workspace_revision,
                          "deployed_at": datetime.now(UTC).isoformat(), "files": sorted(before),
                          "deployed_hashes": runtime["file_hashes"], **runtime, "validation": validation,
                          "refresh": refresh, "message": "Files published through Docker bind mounts and DAGs reserialized. Run a fresh DAG to validate behavior."}
            state["changes"] = {}
            state["last_deployment"] = deployment
            self._save(state)
            return deployment

    def get_airflow_deployment_status(self, dag_id: str):
        self._dag(dag_id)
        with self._locked():
            state = self._state()
            deployment = state["last_deployment"]
            files = deployment["files"] if deployment else [DAG_FILE]
            runtime = self._runtime_status(files)
            return {"dag_id": DAG, "staged_files": sorted(state["changes"]), "workspace_revision": self._revision(state),
                    "last_deployment": deployment, **runtime,
                    "deployment_files_unchanged": bool(deployment) and runtime["file_hashes"] == deployment["deployed_hashes"],
                    "message": "Mounted-file checks do not prove pipeline success; inspect DAG run status and logs."}
