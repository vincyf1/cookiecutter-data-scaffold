import json
import subprocess
from pathlib import Path

import pytest
from agent.tools import DemoTools

PROJECT = Path(__file__).resolve().parents[1]


def test_read_model_and_lineage_use_dbt_project():
    tools = DemoTools(PROJECT)
    model = tools.read_dbt_model("customer_revenue_daily")
    lineage = tools.get_lineage("customer_revenue_daily")

    assert "stg_transactions" in model["sql"]
    assert {"stg_customers", "stg_transactions"} <= set(lineage["upstream_models"])
    assert lineage["model"] == "customer_revenue_daily"


def test_unknown_model_cannot_read_arbitrary_file():
    tools = DemoTools(PROJECT)
    with pytest.raises(ValueError, match="simple DAG"):
        tools.read_dbt_model("../../.env")


def test_schema_changes_reports_migration_and_current_columns():
    tools = DemoTools(PROJECT)
    result = tools.get_recent_schema_changes("customers")

    assert any("add_customer_segment" in change["file"] for change in result["migrations"])
    assert "customer_segment" in result["current_columns"]
    assert result["history_available"] is False


def test_git_changes_reports_missing_repository():
    tools = DemoTools(PROJECT)
    result = tools.get_recent_git_changes("schema_therapy")
    assert result["error"] == "not_a_git_repository"


def test_dbt_tests_select_only_known_model():
    calls = []

    def runner(args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, stdout="PASS=3 ERROR=0", stderr="")

    tools = DemoTools(PROJECT, runner=runner)
    result = tools.run_dbt_tests("customer_revenue_daily")

    assert result["exit_code"] == 0
    assert calls[0][-2:] == ["--select", "customer_revenue_daily"]
    with pytest.raises(ValueError, match="Unknown dbt model"):
        tools.run_dbt_tests("unknown")


def test_rerun_clears_only_latest_exact_task():
    calls = []
    runs = [
        {
            "run_id": "scheduled__2026-09-27T00:00:00+00:00",
            "logical_date": "2026-09-27T00:00:00+00:00",
            "state": "failed",
        }
    ]

    def runner(args, **kwargs):
        calls.append(args)
        if "list-runs" in args:
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps(runs), stderr="")
        if "list" in args:
            return subprocess.CompletedProcess(
                args, 0, stdout="ingest_supabase\ntransform_with_dbt\n", stderr=""
            )
        return subprocess.CompletedProcess(args, 0, stdout="Cleared 1 task", stderr="")

    tools = DemoTools(PROJECT, runner=runner, allow_rerun=True)
    result = tools.rerun_airflow_task("customer_revenue_daily", "transform_with_dbt")

    assert result["exit_code"] == 0
    assert "^transform_with_dbt$" in calls[-1]
    assert "-y" in calls[-1]
    assert "-s" in calls[-1]
    assert "-e" in calls[-1]


def test_propose_patch_returns_diff_without_writing_model():
    class FakeResponse:
        output_text = "--- a/customer_revenue_daily.sql\n+++ b/customer_revenue_daily.sql\n@@ -1 +1 @@\n-old\n+new"

    class FakeResponses:
        def create(self, **kwargs):
            return FakeResponse()

    class FakeClient:
        responses = FakeResponses()

    path = PROJECT / "transformation/models/marts/customer_revenue_daily.sql"
    before = path.read_text()
    result = DemoTools(
        PROJECT, client=FakeClient(), request="Clarify revenue", model="test-model"
    ).propose_patch("customer_revenue_daily")

    assert result["patch"].startswith("--- a/")
    assert path.read_text() == before
