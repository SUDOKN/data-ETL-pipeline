"""Per-(field, stage) execution toggles (2026-08-13).

Testing one stage used to mean running all seven: the chain is a chain of
responsibility, so there was no way to say "search and relationship only, then
stop". These toggles add that, as a HARD STOP at the first disabled stage —
never a pass-through, because a stage that skips itself and hands its successor
an empty map produces a run whose later stages all silently saw nothing.

The structural test at the bottom is the important one: it fails when a new
node's ``execute`` forgets to consult the toggles, which is the way this feature
would otherwise rot.
"""

from __future__ import annotations

import ast
from datetime import datetime
from enum import Enum
from pathlib import Path

import pytest

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.screening import ScreeningVerdict
from core.models.pipeline_nodes.base.base_node import BaseNode, PipelineContext
from core.models.pipeline_nodes.base.pipeline_stage import (
    PipelineStage,
    StageToggles,
)
from core.utils.extraction_dump_util import build_partial_phrase_rows


class _Field(str, Enum):
    industries = "industries"
    conformity_attestations = "conformity_attestations"
    products = "products"
    business_desc = "business_desc"


class _NodeA(BaseNode):
    stage = PipelineStage.relationship

    async def execute(self, **_kwargs) -> None: ...

    @staticmethod
    async def get_result(**_kwargs) -> dict[str, str]:
        return {"CNC lathes": "operates them in-house"}


class _NodeB(BaseNode):
    stage = PipelineStage.screening

    async def execute(self, **_kwargs) -> None: ...


class _Subject:
    subject_unique_id = "example.com"


class _DeferredSubject:
    """No deferred field for any name, so the gate skips the partial dump."""


class _ScrapedText:
    """Only what the dump header reads off the scraped file."""

    s3_version_id = "text-version-1"
    num_tokens = 23757
    last_modified_on = datetime(2026, 8, 13, 19, 0, 0)


# --- StageToggles ---------------------------------------------------------


def test_default_toggles_enable_everything():
    toggles = StageToggles()
    assert not toggles.any_disabled()
    for stage in PipelineStage:
        assert toggles.is_enabled(_Field.industries, stage)


def test_disable_is_scoped_to_the_named_field():
    toggles = StageToggles().disable(
        PipelineStage.screening, fields=[_Field.industries]
    )
    assert not toggles.is_enabled(_Field.industries, PipelineStage.screening)
    # The other eleven pipelines are untouched — the whole point of keying on
    # (field, stage) rather than stage alone.
    assert toggles.is_enabled(_Field.conformity_attestations, PipelineStage.screening)
    assert toggles.is_enabled(_Field.industries, PipelineStage.relationship)


def test_disable_without_fields_applies_to_every_field():
    toggles = StageToggles().disable(PipelineStage.screening)
    assert not toggles.is_enabled(_Field.industries, PipelineStage.screening)
    assert not toggles.is_enabled(_Field.products, PipelineStage.screening)


def test_stop_after_disables_every_later_stage_including_reconcile():
    toggles = StageToggles().stop_after(PipelineStage.relationship)

    assert toggles.is_enabled(_Field.industries, PipelineStage.phrase_search)
    assert toggles.is_enabled(_Field.industries, PipelineStage.recursive_search)
    assert toggles.is_enabled(_Field.industries, PipelineStage.relationship)

    for later in (
        PipelineStage.screening,
        PipelineStage.initial_grounding,
        PipelineStage.freehand_grounding,
        PipelineStage.iterative_grounding,
        # Reconcile too: a partial run must not write a half-computed result
        # that the orchestrator would then read as "this field is done".
        PipelineStage.reconcile,
    ):
        assert not toggles.is_enabled(_Field.industries, later)


def test_stop_after_screening_stops_both_grounding_flavours():
    """Concept grounding and keyword grounding share a tier, so one cutoff
    covers both pipeline shapes."""
    toggles = StageToggles().stop_after(PipelineStage.screening)
    assert toggles.is_enabled(_Field.industries, PipelineStage.screening)
    assert not toggles.is_enabled(_Field.industries, PipelineStage.initial_grounding)
    assert not toggles.is_enabled(_Field.products, PipelineStage.freehand_grounding)


def test_prefill_cannot_be_disabled():
    with pytest.raises(ValueError, match="chunked request map"):
        StageToggles().disable(PipelineStage.prefill)
    # ...and stop_after, which sweeps a range, must not sneak it off either.
    toggles = StageToggles().stop_after(PipelineStage.phrase_search)
    assert toggles.is_enabled(_Field.industries, PipelineStage.prefill)


def test_explicit_only_drops_blanket_toggles_but_keeps_named_ones():
    """A blanket stop_after must not take the prerequisite pipelines down with
    it — the orchestrator refuses to continue without their results."""
    blanket = StageToggles().stop_after(PipelineStage.relationship)
    scoped = blanket.explicit_only(_Field.business_desc)
    assert scoped.is_enabled(_Field.business_desc, PipelineStage.reconcile)

    named = StageToggles().stop_after(
        PipelineStage.single_stage_extraction, fields=[_Field.business_desc]
    )
    scoped = named.explicit_only(_Field.business_desc)
    assert not scoped.is_enabled(_Field.business_desc, PipelineStage.reconcile)


def test_disabled_stages_unions_blanket_and_named():
    toggles = (
        StageToggles()
        .disable(PipelineStage.reconcile)
        .disable(PipelineStage.screening, fields=[_Field.industries])
    )
    assert toggles.disabled_stages(_Field.industries) == {
        PipelineStage.reconcile,
        PipelineStage.screening,
    }
    assert toggles.disabled_stages(_Field.products) == {PipelineStage.reconcile}


# --- PipelineContext ------------------------------------------------------


def test_context_records_completed_stages_in_order_without_duplicates():
    context = PipelineContext()
    context[_NodeA] = {"a": "req"}
    context[_NodeB] = {"b": "req"}
    # The recursive nodes re-publish; the dump must not then parse a stage twice.
    context[_NodeA] = {"a": "req", "a2": "req"}

    assert context.stages_completed == [
        (PipelineStage.relationship, _NodeA),
        (PipelineStage.screening, _NodeB),
    ]
    assert context.node_class_for(PipelineStage.screening) is _NodeB
    assert context.node_class_for(PipelineStage.iterative_grounding) is None


# --- the gate -------------------------------------------------------------


@pytest.mark.asyncio
async def test_gate_lets_an_enabled_stage_through():
    node = _NodeA(field_type=_Field.industries, next_node=None)
    stopped = await node.stop_if_stage_disabled(
        subject=_Subject(),
        deferred_subject=_DeferredSubject(),
        scraped_text_file=_ScrapedText(),
        timestamp=None,
        pipeline_context=PipelineContext(),
    )
    assert stopped is False


@pytest.mark.asyncio
async def test_gate_stops_a_disabled_stage():
    node = _NodeA(field_type=_Field.industries, next_node=None)
    context = PipelineContext(
        stage_toggles=StageToggles().stop_after(
            PipelineStage.recursive_search, fields=[_Field.industries]
        )
    )
    stopped = await node.stop_if_stage_disabled(
        subject=_Subject(),
        deferred_subject=_DeferredSubject(),
        scraped_text_file=_ScrapedText(),
        timestamp=None,
        pipeline_context=context,
    )
    assert stopped is True


@pytest.mark.asyncio
async def test_gate_is_per_field():
    """The same stage, two fields, one run: only the named field stops."""
    context = PipelineContext(
        stage_toggles=StageToggles().disable(
            PipelineStage.relationship, fields=[_Field.industries]
        )
    )
    gated = _NodeA(field_type=_Field.industries, next_node=None)
    ungated = _NodeA(field_type=_Field.conformity_attestations, next_node=None)

    assert await gated.stop_if_stage_disabled(
        subject=_Subject(),
        deferred_subject=_DeferredSubject(),
        scraped_text_file=_ScrapedText(),
        timestamp=None,
        pipeline_context=context,
    )
    assert not await ungated.stop_if_stage_disabled(
        subject=_Subject(),
        deferred_subject=_DeferredSubject(),
        scraped_text_file=_ScrapedText(),
        timestamp=None,
        pipeline_context=context,
    )


# --- partial dump rows ----------------------------------------------------


def _verdict(passed: bool) -> ScreeningVerdict:
    return ScreeningVerdict(
        passed=passed,
        identified_entity="CNC lathes" if passed else None,
        applied_rules=[
            AppliedRule(rule_id="R1", outcome="yes", explanation="stated on the page")
        ],
    )


def test_partial_rows_omit_stages_that_did_not_run():
    """The distinction the whole dump exists for: "screening rejected it" and
    "screening never happened" must not read the same."""
    rows = build_partial_phrase_rows(
        search_rounds={0: set(), 1: {"CNC lathes", "waterjet"}},
        relationship_flat={"CNC lathes": "operates them in-house"},
        screening_flat=None,
        grounding_flat=None,
    )
    by_phrase = {row["phrase"]: row for row in rows}

    assert set(by_phrase) == {"CNC lathes", "waterjet"}
    assert "screening" not in by_phrase["CNC lathes"]
    assert "grounding" not in by_phrase["CNC lathes"]
    # Relationship RAN and had nothing for waterjet — that is an explicit null,
    # not an absent key.
    assert by_phrase["waterjet"]["relationship"] is None
    assert by_phrase["CNC lathes"]["relationship"] == "operates them in-house"


def test_partial_rows_carry_search_provenance_and_screening_verdicts():
    rows = build_partial_phrase_rows(
        search_rounds={0: {"brute phrase"}, 1: {"CNC lathes"}, 2: {"waterjet"}},
        relationship_flat={},
        screening_flat={"CNC lathes": _verdict(True), "waterjet": _verdict(False)},
        grounding_flat=None,
    )
    by_phrase = {row["phrase"]: row for row in rows}

    assert by_phrase["brute phrase"]["provenance"] == "brute"
    assert by_phrase["CNC lathes"]["provenance"] == "llm_round_1"
    assert by_phrase["waterjet"]["search_round"] == 2
    assert by_phrase["CNC lathes"]["screening"]["passed"] is True
    assert by_phrase["waterjet"]["screening"]["passed"] is False


def test_partial_rows_survive_a_run_stopped_right_after_search():
    """A search-only run still has phrases to show; driving rows off the later
    stages alone would dump an empty file."""
    rows = build_partial_phrase_rows(
        search_rounds={1: {"CNC lathes"}},
        relationship_flat=None,
        screening_flat=None,
        grounding_flat=None,
    )
    assert [row["phrase"] for row in rows] == ["CNC lathes"]
    assert set(rows[0]) == {"phrase", "search_round", "provenance"}


def test_partial_rows_name_the_grounding_stage_that_ran():
    rows = build_partial_phrase_rows(
        search_rounds={1: {"CNC lathes"}},
        relationship_flat=None,
        screening_flat=None,
        grounding_flat={
            "CNC lathes": {
                "Machining": [
                    AppliedRule(rule_id="G1", outcome="yes", explanation="stated")
                ]
            }
        },
        grounding_stage=PipelineStage.freehand_grounding.value,
    )
    assert rows[0]["freehand_grounding"]["Machining"][0]["rule_id"] == "G1"


# --- the partial dump writer ----------------------------------------------


class _Bundle:
    llm_phrase_recursive_search_req_ids: list[str] = []
    llm_phrase_relationship_req_ids: list[str] = [
        "example.com>industries>llm_phrase_relationship>group>0>chunk>0-100>"
        "gpt-4o-mini|temperature=0.0|pv=PROMPT-VERSION-1|gs=50"
    ]


class _Metadata:
    @staticmethod
    def model_dump(mode: str = "python") -> dict[str, object]:
        return {"llm_phrase_relationship": {"prompt_version_id": "PROMPT-VERSION-1"}}


class _ExtractionRequests:
    chunked_request_map = {"0-100": _Bundle()}
    metadata = _Metadata()


@pytest.mark.asyncio
async def test_partial_dump_header_says_what_ran_and_what_was_switched_off(
    monkeypatch,
):
    """The header is read before any row, so it has to carry the stage list —
    a reader who does not know which stages are missing will read an absent
    stage as an absent result."""
    written: dict[str, object] = {}

    def _capture(**kwargs):
        written.update(kwargs)

    monkeypatch.setattr(
        "core.services.pipeline_nodes.partial_run_dump.write_extraction_dump",
        _capture,
    )

    from core.services.pipeline_nodes.partial_run_dump import write_partial_run_dump

    context = PipelineContext()
    context[_NodeA] = {"req-1": object()}

    await write_partial_run_dump(
        subject_unique_id="example.com",
        field_type=_Field.industries,
        stopped_at=PipelineStage.screening,
        disabled_stages={PipelineStage.screening, PipelineStage.reconcile},
        extraction_requests=_ExtractionRequests(),
        scraped_text_file=_ScrapedText(),
        pipeline_context=context,
        timestamp=None,
    )

    run = written["run_provenance"]
    assert run["partial"] is True
    assert run["stopped_at"] == "screening"
    assert run["stages_run"] == ["relationship"]
    assert run["stages_disabled"] == ["reconcile", "screening"]
    assert written["name_suffix"] == "__partial"
    assert written["field_type"] is _Field.industries
    # A partial dump carries the same provenance a full one does — which text was
    # read, and the metadata every request custom_id was built from.
    assert run["scraped_text"]["s3_version_id"] == "text-version-1"
    assert (
        run["extraction_metadata"]["llm_phrase_relationship"]["prompt_version_id"]
        == "PROMPT-VERSION-1"
    )
    # The stage that ran is parsed into rows; search never ran, so there is no
    # provenance to report and the key stays absent.
    row = written["chunked_contents"]["0-100"]["rows"][0]
    assert row["phrase"] == "CNC lathes"
    assert row["relationship"] == "operates them in-house"
    assert "screening" not in row
    # The dump prices its requests off the completed maps, merged across stages.
    assert "req-1" in written["completed_requests"]


class _SingleStageNode(BaseNode):
    stage = PipelineStage.single_stage_extraction

    async def execute(self, **_kwargs) -> None: ...

    @staticmethod
    async def get_result(**_kwargs) -> dict[str, object]:
        return {"answer": True, "confidence": 4}


class _BrokenSingleStageNode(BaseNode):
    stage = PipelineStage.single_stage_extraction

    async def execute(self, **_kwargs) -> None: ...

    @staticmethod
    async def get_result(**_kwargs) -> dict[str, object]:
        raise ValueError("response is not the schema it promised")


@pytest.mark.asyncio
async def test_partial_dump_for_a_single_stage_field_carries_the_result(
    monkeypatch,
):
    """Single-stage fields have no phrase stages, so their chunk content is the
    parsed result. Until 2026-08-14 these dumps were header-only shells with
    empty requests and empty rows."""
    written: dict[str, object] = {}

    def _capture(**kwargs):
        written.update(kwargs)

    monkeypatch.setattr(
        "core.services.pipeline_nodes.partial_run_dump.write_extraction_dump",
        _capture,
    )

    from core.services.pipeline_nodes.partial_run_dump import write_partial_run_dump

    context = PipelineContext()
    context[_SingleStageNode] = {"req-single-1": object()}

    await write_partial_run_dump(
        subject_unique_id="example.com",
        field_type=_Field.business_desc,
        stopped_at=PipelineStage.reconcile,
        disabled_stages={PipelineStage.reconcile},
        extraction_requests=_ExtractionRequests(),
        scraped_text_file=_ScrapedText(),
        pipeline_context=context,
        timestamp=None,
    )

    assert written["chunked_contents"]["0-100"] == {
        "result": {"answer": True, "confidence": 4}
    }
    assert written["run_provenance"]["stages_run"] == ["single_stage_extraction"]
    assert "req-single-1" in written["completed_requests"]


@pytest.mark.asyncio
async def test_partial_dump_survives_a_single_stage_result_that_wont_parse(
    monkeypatch,
):
    """A chunk whose response won't parse still appears — a null result with a
    note beats silently losing the whole file."""
    written: dict[str, object] = {}

    def _capture(**kwargs):
        written.update(kwargs)

    monkeypatch.setattr(
        "core.services.pipeline_nodes.partial_run_dump.write_extraction_dump",
        _capture,
    )

    from core.services.pipeline_nodes.partial_run_dump import write_partial_run_dump

    context = PipelineContext()
    context[_BrokenSingleStageNode] = {"req-single-1": object()}

    await write_partial_run_dump(
        subject_unique_id="example.com",
        field_type=_Field.business_desc,
        stopped_at=PipelineStage.reconcile,
        disabled_stages={PipelineStage.reconcile},
        extraction_requests=_ExtractionRequests(),
        scraped_text_file=_ScrapedText(),
        pipeline_context=context,
        timestamp=None,
    )

    assert written["chunked_contents"]["0-100"] == {
        "result": None,
        "note": "result_parse_failed",
    }


@pytest.mark.asyncio
async def test_partial_dump_never_raises(monkeypatch):
    """A run that already stopped on purpose must not then fail on the way out
    and lose the stages it did complete."""

    def _explode(**_kwargs):
        raise RuntimeError("disk gone")

    monkeypatch.setattr(
        "core.services.pipeline_nodes.partial_run_dump.write_extraction_dump",
        _explode,
    )

    from core.services.pipeline_nodes.partial_run_dump import write_partial_run_dump

    await write_partial_run_dump(
        subject_unique_id="example.com",
        field_type=_Field.industries,
        stopped_at=PipelineStage.screening,
        disabled_stages={PipelineStage.screening},
        extraction_requests=_ExtractionRequests(),
        scraped_text_file=_ScrapedText(),
        pipeline_context=PipelineContext(),
        timestamp=None,
    )


# --- the tripwire ---------------------------------------------------------

_NODE_PACKAGES = (
    Path(__file__).resolve().parents[3]
    / "core/src/core/models/pipeline_nodes",
    Path(__file__).resolve().parents[4]
    / "apps/data_etl_app/src/data_etl_app/models/pipeline_nodes",
)

# Prefill builds the chunked request map every later stage indexes into, so it
# is not toggleable and must not consult the toggles.
_EXEMPT_CLASSES = {
    "BaseNode",  # declares the abstract execute
    "PrefillNode",
    "ConceptExtractionPrefillNode",
    "KeywordExtractionPrefillNode",
    "SingleStageExtractionPrefillNode",
}


def _execute_methods() -> list[tuple[str, str, ast.AsyncFunctionDef]]:
    found = []
    for package in _NODE_PACKAGES:
        for path in sorted(package.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for class_node in ast.walk(tree):
                if not isinstance(class_node, ast.ClassDef):
                    continue
                for item in class_node.body:
                    if (
                        isinstance(item, ast.AsyncFunctionDef)
                        and item.name == "execute"
                    ):
                        found.append((path.name, class_node.name, item))
    return found


def test_every_execute_consults_the_stage_toggles():
    """Every node that runs work must be gateable.

    A node whose ``execute`` never calls ``stop_if_stage_disabled`` runs no
    matter what the toggles say — and because it then calls its successor, so
    does the entire rest of the chain. That failure is invisible at runtime (you
    get results, just not the ones you asked for), so it is caught here instead.
    """
    missing = []
    for file_name, class_name, method in _execute_methods():
        if class_name in _EXEMPT_CLASSES:
            continue
        calls = {
            node.func.attr
            for node in ast.walk(method)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        if "stop_if_stage_disabled" not in calls:
            missing.append(f"{file_name}::{class_name}")

    assert not missing, (
        "these execute() overrides ignore the stage toggles, so disabling a "
        f"stage would not stop them: {missing}"
    )


def test_the_tripwire_actually_inspects_something():
    """Guards the guard: a broken path glob would make the test above vacuous."""
    inspected = {class_name for _file, class_name, _method in _execute_methods()}
    assert "ConceptReconcileNode" in inspected
    assert "BaseLLMExtractionNode" in inspected
    assert len(inspected - _EXEMPT_CLASSES) >= 5
