"""Execute the existing schema_therapy agent tools with a narrow JSON contract."""

import json
import os
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2] / "live-demo/schema_therapy"
sys.path.insert(0, str(PROJECT))

from agent.tools import DemoTools  # noqa: E402
from airflow_workspace import AirflowWorkspace  # noqa: E402

WORKSPACE_TOOLS = {
    "list_airflow_files": {"dag_id"},
    "read_airflow_file": {"path"},
    "edit_airflow_file": {"path", "expected_sha256", "old_text", "new_text"},
    "validate_airflow_deployment": {"dag_id"},
    "deploy_airflow_changes": {"dag_id", "workspace_revision"},
    "discard_airflow_changes": {"dag_id", "workspace_revision"},
    "get_airflow_deployment_status": {"dag_id"},
}

ALLOWED = {
    **WORKSPACE_TOOLS,
    "get_lineage": {"model"},
    "get_recent_schema_changes": {"dataset"},
    "get_recent_git_changes": {"repo"},
    "read_dbt_model": {"model"},
    "run_dbt_tests": {"model"},
    "propose_patch": {"model"},
    "rerun_airflow_task": {"dag_id", "task_id"},
    "get_supabase_schema": {"dataset"},
    "get_supabase_source_summary": {"dataset"},
}


def main():
    try:
        request = json.load(sys.stdin)
        name, args = request["name"], request["args"]
        if name not in ALLOWED or set(args) != ALLOWED[name]:
            raise ValueError("Invalid tool or arguments")
        if name in WORKSPACE_TOOLS:
            tools = AirflowWorkspace(PROJECT, Path(__file__).resolve().parents[1] / ".airflow-workspace")
            print(json.dumps(getattr(tools, name)(**args)))
            return
        if name == "rerun_airflow_task" and os.environ.get("ALLOW_RERUN") != "1":
            raise PermissionError("Rerun requires explicit confirmation")
        if name in {"get_supabase_schema", "get_supabase_source_summary"}:
            from supabase_source import inspect_source

            print(json.dumps(inspect_source(
                args["dataset"], include_stats=name == "get_supabase_source_summary"
            )))
            return
        client = None
        if name == "propose_patch":
            from openai import OpenAI

            client = OpenAI()
        tools = DemoTools(
            PROJECT,
            client=client,
            model=os.environ.get("OPENAI_MODEL"),
            request="Propose a minimal fix for the failed customer revenue dbt model.",
            allow_rerun=os.environ.get("ALLOW_RERUN") == "1",
        )
        result = getattr(tools, name)(**args)
        print(json.dumps(result))
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
