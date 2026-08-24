"""Phase 3.3 of pipeline v3 (PIPELINE_V3_PLAN.md, D16): the downstream re-key.

The synthesis stage's per-group records are what grounding, OOV, screening,
descent and reconcile consume — one ``GroupRecord`` (focal form + synthesis)
per SYNTHESIZED group, keyed by the opaque ``group_id``. These tests pin the
derivation, the identical-synthesis tripwire (§10, a dump counter, never a
retry trigger), and the group rows the reconcile dump writes.
"""

from core.models.extraction_schemas.grounding import RecordGroundingEntry
from core.models.extraction_schemas.screening import CandidateScreeningVerdict
from core.models.extraction_schemas.synthesis import SynthesisRecordInput
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    ChunkGroundingAnswer,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    ChunkAnswer,
    ChunkSynthesisResult,
    downstream_group_records,
)
from core.utils.aggregation_fold import WindowInput, fold_document
from core.utils.extraction_dump_util import (
    build_keyword_group_rows,
    build_partial_group_rows,
)
from core.utils.synthesis_dump_util import build_synthesis_dump

SEP = "#" * 50
WINDOW = (
    f"{SEP}\nhttps://acme.example/materials\n\n"
    "We stock Aluminum and Brass. Sample Lead Time: 2 weeks.\n"
    "aluminum alloys ship daily.\n"
)


def _result(syntheses_for: dict[int, str] | None = None) -> ChunkSynthesisResult:
    """A chunk with three non-empty groups (Aluminum, Brass, Lead Time) and
    one empty bundle (ghost); *syntheses_for* maps record INDEX → synthesis
    text (default: all three answered distinctly)."""
    fold = fold_document(
        [WindowInput(WINDOW, ["Aluminum", "Brass", "Lead Time", "ghost"], {})]
    )
    records: list[SynthesisRecordInput] = fold.synthesis_records()
    ids = [r.record_id for r in records]
    if syntheses_for is None:
        syntheses_for = {0: "stocks aluminum.", 1: "stocks brass.", 2: "quotes lead time."}
    answer = ChunkAnswer(
        sent_ids=ids,
        syntheses={ids[i]: text for i, text in syntheses_for.items()},
        unknown_answer_ids=[],
        retried_record_ids=[],
    )
    return ChunkSynthesisResult(
        fold=fold,
        records=records,
        include_location=True,
        answer=answer,
        group_request_count=1,
        retry_request_count=0,
    )


def test_downstream_group_records_keep_bundle_order_and_skip_the_unsynthesized():
    result = _result(syntheses_for={0: "stocks aluminum.", 2: "quotes lead time."})
    groups = downstream_group_records(result)

    ids = [r.record_id for r in result.records]
    # Bundle order preserved (group membership downstream is request identity);
    # the unanswered Brass group is absent — the v3 analog of the v2
    # records_with_mentions filter — and so is the empty ghost bundle.
    assert list(groups) == [ids[0], ids[2]]
    assert groups[ids[0]].focal_form == "Aluminum"
    assert groups[ids[0]].synthesis == "stocks aluminum."
    assert groups[ids[2]].focal_form == "Lead Time"


def test_identical_synthesis_tripwire_flags_same_request_twins_only():
    # Aluminum and Brass share one byte-identical synthesis string; Lead Time
    # differs. With everything in ONE request (large cap) both twins are
    # flagged; with one record per request, nothing is.
    result = _result(
        syntheses_for={0: "one sentence.", 1: "one sentence.", 2: "another."}
    )
    ids = [r.record_id for r in result.records]

    dump = build_synthesis_dump(result, max_entries_per_request=100)
    rows = {r["group_id"]: r for r in dump["records"]}
    assert rows[ids[0]]["identical_synthesis_in_request"] == 0
    assert rows[ids[1]]["identical_synthesis_in_request"] == 0
    assert "identical_synthesis_in_request" not in rows[ids[2]]
    assert dump["summary"]["identical_synthesis_records_in_request"] == 2

    # cap of 1 entry: records pack one per request, so the identical strings
    # never share a request and the counter reads zero.
    dump_split = build_synthesis_dump(result, max_entries_per_request=1)
    assert dump_split["summary"]["identical_synthesis_records_in_request"] == 0

    # no cap known (older callers): the counter is simply absent.
    assert "identical_synthesis_records_in_request" not in build_synthesis_dump(
        result
    )["summary"]


def _verdict(passed: bool) -> CandidateScreeningVerdict:
    return CandidateScreeningVerdict(passed=passed, applied_rules=[])


def test_keyword_group_rows_cover_every_branch_a_group_can_take():
    result = _result(syntheses_for={0: "stocks aluminum.", 2: "quotes lead time."})
    ids = [r.record_id for r in result.records]

    rows = build_keyword_group_rows(
        synthesis_result=result,
        freehand_flat={
            ids[0]: RecordGroundingEntry(tags={"Aluminum": []}),
            ids[2]: RecordGroundingEntry(tags={}, explanation="nothing to mint"),
        },
        screening_flat={ids[0]: {"Aluminum": _verdict(False)}},
        search_rounds={1: {"Aluminum", "Brass", "Lead Time", "ghost"}},
        subject_name="Acme Example",
    )
    by_id = {r["group_id"]: r for r in rows}

    # One row per GROUP the fold built, the empty ghost bundle included.
    assert len(rows) == 4
    aluminum = by_id[ids[0]]
    assert aluminum["record"] == {
        "focal_form": "Aluminum",
        "synthesis": "stocks aluminum.",
    }
    assert aluminum["forms"] == ["Aluminum", "aluminum"]
    assert aluminum["provenance"] == "llm_round_1"
    assert aluminum["status"] == "screened_out"
    assert by_id[ids[1]]["status"] == "not_synthesized"  # sent, never answered
    assert by_id[ids[2]]["status"] == "no_candidates"  # grounding declined
    ghost = next(r for r in rows if r["key"] == "ghost")
    assert ghost["status"] == "no_mentions" and ghost["record"] is None


def test_partial_group_rows_omit_status_and_distinguish_never_ran_from_null():
    result = _result()
    ids = [r.record_id for r in result.records]

    rows = build_partial_group_rows(
        search_rounds={1: {"Aluminum", "Brass", "Lead Time"}},
        synthesis_result=result,
        grounding_by_stage={
            "freehand_grounding": {ids[0]: RecordGroundingEntry(tags={"Aluminum": []})}
        },
        screening_flat=None,
    )
    by_id = {r["group_id"]: r for r in rows}

    assert all("status" not in r for r in rows)
    # ran-but-nothing is an explicit null; never-ran is an omitted key.
    assert by_id[ids[1]]["freehand_grounding"] is None
    assert "screening" not in by_id[ids[0]]
    assert by_id[ids[0]]["freehand_grounding"] == {"tags": {"Aluminum": []}}


def test_chunk_grounding_answer_reports_what_is_still_missing():
    answer = ChunkGroundingAnswer(
        sent_ids=["g1", "g2", "g3"],
        results={"g2": RecordGroundingEntry(tags={"X": []})},
        retried_record_ids=["g3"],
    )
    assert answer.missing_ids == ["g1", "g3"]
