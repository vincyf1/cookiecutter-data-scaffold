import json
from types import SimpleNamespace

from agent.run import run_agent


def test_agent_dispatches_tool_and_returns_final_answer():
    calls = []

    class FakeResponses:
        def create(self, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                return SimpleNamespace(
                    output=[
                        SimpleNamespace(
                            type="function_call",
                            name="get_lineage",
                            arguments='{"model":"customer_revenue_daily"}',
                            call_id="call_1",
                        )
                    ],
                    output_text="",
                )
            return SimpleNamespace(output=[], output_text="Revenue depends on staging models.")

    class FakeTools:
        def get_lineage(self, model):
            return {"model": model, "upstream_models": ["stg_transactions"]}

    answer = run_agent(
        SimpleNamespace(responses=FakeResponses()), "test-model", "Explain lineage", FakeTools()
    )

    assert answer == "Revenue depends on staging models."
    output = calls[1]["input"][-1]
    assert output["type"] == "function_call_output"
    assert output["call_id"] == "call_1"
    assert json.loads(output["output"])["upstream_models"] == ["stg_transactions"]
