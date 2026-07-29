"""Cases are data: adding a case means adding a YAML file, not writing a test.

That matters in a generated project, whose failure cases will differ from
the ones this template ships.
"""

import json
from dataclasses import dataclass
from pathlib import Path

import yaml

CASES_DIR = Path(__file__).parent / "cases"
CONFIDENCE_ORDER = ["low", "medium", "high"]


@dataclass
class EvalCase:
    id: str
    failure_mode: str
    expect: dict
    score: dict


def load_cases() -> list[EvalCase]:
    cases = []
    for path in sorted(CASES_DIR.glob("*.yaml")):
        raw = yaml.safe_load(path.read_text())
        cases.append(
            EvalCase(
                id=raw["id"],
                failure_mode=raw["failure_mode"],
                expect=raw["expect"],
                score=raw["score"],
            )
        )
    return cases


def _tool_calls(trace) -> list[str]:
    return [e.name for e in trace.elements if e.kind == "tool_call"]


def _tools_called(trace, expected: dict) -> bool:
    """Coverage and ordering, not an exact transcript.

    The criterion exists to prove the agent diagnosed from evidence: it
    reached for each expected tool, and it read the log *before* it proposed
    a patch. How many files it opened on the way is behaviour, not a
    requirement -- asserting the cassette's exact call list would fail a live
    model that read one file, or three, while behaving correctly. It still
    fails an agent that skips the log or proposes first, which is the whole
    point. `exact_tool_sequence` keeps the strict check for replay, where the
    transcript is fixed by construction.
    """
    actual = _tool_calls(trace)
    if not all(name in actual for name in expected.get("must_call", [])):
        return False
    for earlier, later in expected.get("must_precede", []):
        if earlier not in actual or later not in actual:
            return False
        if actual.index(earlier) > actual.index(later):
            return False
    return True


def _exact_tool_sequence(trace, expected: list[str]) -> bool:
    return _tool_calls(trace) == expected


def _patch_targets(published, expected: str) -> bool:
    if published.patch_path is None:
        return False
    meta = json.loads(Path(published.metadata_path).read_text())
    return any(edit["path"] == expected for edit in meta["edits"])


def _diagnosis_mentions(published, expected: list[str]) -> bool:
    meta = json.loads(Path(published.metadata_path).read_text())
    haystack = (meta["root_cause"] + " " + meta["body"]).lower()
    return all(term.lower() in haystack for term in expected)


def _min_confidence(published, expected: str) -> bool:
    meta = json.loads(Path(published.metadata_path).read_text())
    return CONFIDENCE_ORDER.index(meta["confidence"]) >= CONFIDENCE_ORDER.index(expected)


def score_case(case: EvalCase, trace, published, replay: bool = False) -> dict:
    """Score one case. `replay` adds the criteria only a cassette can satisfy.

    A replayed run's tool transcript is fixed by the cassette, so it can be
    asserted exactly -- and should be, since a drifted cassette is a real
    defect. A live run's is not, so the live tier gates on intent only.
    """
    checks = {
        "tools_called": lambda: _tools_called(trace, case.expect["tools_called"]),
        "exact_tool_sequence": lambda: _exact_tool_sequence(
            trace, case.expect["exact_tool_sequence"]
        ),
        "patch_targets": lambda: _patch_targets(published, case.expect["patch_targets"]),
        "diagnosis_mentions": lambda: _diagnosis_mentions(
            published, case.expect["diagnosis_mentions"]
        ),
        "min_confidence": lambda: _min_confidence(published, case.expect["min_confidence"]),
    }
    required_names = list(case.score["required"])
    if replay:
        required_names += case.score.get("replay_only", [])
    required = {name: checks[name]() for name in required_names}
    advisory = {name: checks[name]() for name in case.score["advisory"]}
    return {
        "case_id": case.id,
        "required": required,
        "advisory": advisory,
        "passed": all(required.values()),
    }


def patch_text(published) -> str:
    """The rendered diff, read back from the artifact the publisher wrote.

    Re-deriving it from a fresh ToolContext would yield an empty string --
    the staged edits live on the context the runner used, not a new one.
    """
    if published.patch_path is None:
        return ""
    return Path(published.patch_path).read_text()
