"""The Step 2 concept dump (cutover 6c, 2026-09-22): the rows show each
record's descent LEVEL BY LEVEL and what the reconcile step shipped for it;
the chunk block shows each wave across records; both come from the stored
trail and the decision, never from re-parsing.

Records (fold-built): "CNC machining" (R1), "coating" (R4), "assembly" (R5).
Trail: wave 1 Machining (R1 accepted → descent reached Conventional
Machining) and Joining (R5 accepted → leaf step proposed "Fastened
Assembly"); wave 2 Conventional Machining (R1 accepted → reached CNC
Machining, false child Painting) and Coating (R4 REJECTED); wave 3 CNC
Machining (R1 accepted, leaf step declined) and Painting removed on R4
under the failed Coating; proposal wave: Fastened Assembly rejected,
Cerakote Coating (the grounding call's, on R4) accepted.
"""

from __future__ import annotations

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.descent import DescentTrail, WaveTrail
from core.models.extraction_schemas.grounding import RecordGroundingEntry
from core.models.extraction_schemas.screening import CandidateScreeningVerdict
from core.models.extraction_schemas.synthesis import SynthesisAnswer
from core.models.pipeline_nodes.base.pipeline_stage import PipelineStage
from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    ChunkAnswer,
    ChunkSynthesisResult,
)
from core.services.pipeline_nodes.partial_run_dump import _GROUNDING_STAGE_ROW_KEYS, _SCREENING_STAGES
from core.utils.aggregation_fold import WindowInput, fold_document
from core.utils.descent_dump_util import build_descent_trail_dump, build_step2_concept_group_rows

WINDOW = "#" * 50 + "\nhttps://acme.example/\n\nWe offer CNC machining, coating and assembly.\n"


def _result() -> tuple[ChunkSynthesisResult, dict[str, str]]:
    fold = fold_document([WindowInput(WINDOW, ["CNC machining", "coating", "assembly"])])
    records = fold.synthesis_records()
    by_form = {r.focal_form: r.record_id for r in records}
    answer = ChunkAnswer(
        sent_ids=[r.record_id for r in records],
        syntheses={r.record_id: SynthesisAnswer(synthesis=f"offers {r.focal_form}") for r in records},
        unknown_answer_ids=[], retried_record_ids=[],
    )
    return ChunkSynthesisResult(fold=fold, records=records, wire_text=WINDOW, answer=answer, group_request_count=1, retry_request_count=0), by_form


def _rules(quote: str) -> list[AppliedRule]:
    return [AppliedRule(rule_id="GR-E1", outcome="satisfied", explanation=quote)]


def _v(passed: bool, failed_rule: str | None = None) -> CandidateScreeningVerdict:
    return CandidateScreeningVerdict(passed=passed, applied_rules=[], evidence="named" if passed else None, failed_rule=failed_rule, quote="q")


def _trail(r1: str, r4: str, r5: str) -> DescentTrail:
    trail = DescentTrail(max_depth=3)
    trail.waves[1] = WaveTrail(
        units={"Machining": [r1], "Joining": [r5]},
        screening={r1: {"Machining": _v(True)}, r5: {"Joining": _v(True)}},
        descent={"Machining": {r1: RecordGroundingEntry(tags={"Conventional Machining": _rules("CNC machining")})}},
        leaf={"Joining": {r5: RecordGroundingEntry(tags={"Fastened Assembly": _rules("assembly")})}},
    )
    trail.waves[2] = WaveTrail(
        units={"Conventional Machining": [r1], "Coating": [r4]},
        screening={r1: {"Conventional Machining": _v(True)}, r4: {"Coating": _v(False, "SCR-2")}},
        descent={"Conventional Machining": {r1: RecordGroundingEntry(tags={"CNC Machining": _rules("CNC machining")})}},
        false_children={"Conventional Machining": {r1: ["Painting"]}},
    )
    trail.waves[3] = WaveTrail(
        units={"CNC Machining": [r1]},
        screening={r1: {"CNC Machining": _v(True)}},
        leaf={"CNC Machining": {r1: RecordGroundingEntry(tags={}, explanation="nothing narrower")}},
        removed_under_failed_ancestor={"Painting": [r4]},
    )
    trail.proposal_units = {"Cerakote Coating": [r4], "Fastened Assembly": [r5]}
    trail.proposal_sources = {"Cerakote Coating": ["grounding"], "Fastened Assembly": ["leaf:Joining"]}
    trail.proposal_screening = {r4: {"Cerakote Coating": _v(True)}, r5: {"Fastened Assembly": _v(False, "SCR-0")}}
    return trail


def _rows(proposal_pass_ran: bool = True):
    result, ids = _result()
    r1, r4, r5 = ids["CNC machining"], ids["coating"], ids["assembly"]
    trail = _trail(r1, r4, r5)
    grounding = {
        r1: RecordGroundingEntry(tags={"Machining": _rules("CNC machining")}),
        r4: RecordGroundingEntry(tags={"Coating": _rules("coating")}),
        r5: RecordGroundingEntry(tags={"Joining": _rules("assembly")}),
    }
    proposals = {r4: RecordGroundingEntry(tags={"Cerakote Coating": _rules("Cerakote")})}
    shipped = {r1: ["CNC Machining"], r5: ["Joining"]}
    rows = build_step2_concept_group_rows(
        synthesis_result=result, grounding_flat=grounding, proposals_flat=proposals,
        proposal_pass_flat={} if proposal_pass_ran else None, trail=trail, shipped_by_record=shipped,
        search_rounds={1: {"CNC machining", "coating", "assembly"}},
    )
    return {row["focal_form"]: row for row in rows}, trail, shipped


def test_a_row_shows_its_record_level_by_level_down_to_what_shipped():
    rows, _trail, _shipped = _rows()
    r1 = rows["CNC machining"]
    assert r1["grounding"]["tags"]["Machining"][0]["explanation"] == "CNC machining"
    assert r1["proposals"] is None
    assert [n["label"] for n in r1["descent_levels"][1]] == ["Machining"]
    assert r1["descent_levels"][1][0]["verdict"] == {"passed": True, "evidence": "named", "quote": "q"}
    assert r1["descent_levels"][1][0]["descent"] == {"reached": {"Conventional Machining": "CNC machining"}}
    assert r1["descent_levels"][2][0]["false_children"] == ["Painting"]
    assert r1["descent_levels"][3][0]["leaf_step"] == {"declined": "nothing narrower"}
    assert r1["proposal_wave"] is None
    assert r1["shipped"] == {"in_vocab": ["CNC Machining"], "out_of_vocab": []}
    assert r1["status"] == "grounded"


def test_a_rejected_record_shows_the_failed_rule_and_the_pair_removed_under_it():
    rows, _trail, _shipped = _rows()
    r4 = rows["coating"]
    assert r4["descent_levels"][2] == [{"label": "Coating", "verdict": {"passed": False, "failed_rule": "SCR-2", "quote": "q"}}]
    assert r4["descent_levels"][3] == [{"label": "Painting", "removed_under_failed_ancestor": True}]
    # the grounding call's proposal on this record was accepted at the proposal wave
    assert r4["proposals"]["tags"]["Cerakote Coating"][0]["explanation"] == "Cerakote"
    assert r4["proposal_wave"]["Cerakote Coating"]["passed"] is True
    assert r4["shipped"] == {"in_vocab": [], "out_of_vocab": ["Cerakote Coating"]}
    assert r4["status"] == "grounded"


def test_a_leaf_step_proposal_rejected_at_the_proposal_wave_leaves_the_parent_standing():
    rows, _trail, _shipped = _rows()
    r5 = rows["assembly"]
    assert r5["descent_levels"][1][0]["leaf_step"] == {"proposed": {"Fastened Assembly": "assembly"}}
    assert r5["proposal_wave"]["Fastened Assembly"]["failed_rule"] == "SCR-0"
    assert r5["shipped"] == {"in_vocab": ["Joining"], "out_of_vocab": []}
    assert r5["status"] == "grounded"


def test_the_proposal_pass_key_is_present_exactly_when_the_pass_ran():
    with_pass, _t, _s = _rows(proposal_pass_ran=True)
    without, _t, _s = _rows(proposal_pass_ran=False)
    assert with_pass["coating"]["proposal_pass"] is None  # ran, saw nothing for this record
    assert "proposal_pass" not in without["coating"]


def test_the_chunk_block_shows_every_wave_across_records():
    rows, trail, shipped = _rows()
    r1, r4, r5 = rows["CNC machining"]["group_id"], rows["coating"]["group_id"], rows["assembly"]["group_id"]
    block = build_descent_trail_dump(trail, shipped, ["Cerakote Coating"])
    assert block["max_depth"] == 3 and list(block["waves"]) == [1, 2, 3]
    assert block["waves"][1]["units"] == {"Joining": [r5], "Machining": [r1]}
    assert block["waves"][1]["verdicts"]["Machining"] == {"accepted": [r1], "rejected": {}}
    assert block["waves"][1]["descent"]["Machining"][r1] == {"reached": {"Conventional Machining": "CNC machining"}}
    assert block["waves"][1]["leaf_step"]["Joining"][r5] == {"proposed": {"Fastened Assembly": "assembly"}}
    assert block["waves"][2]["verdicts"]["Coating"] == {"accepted": [], "rejected": {r4: "SCR-2"}}
    assert block["waves"][2]["false_children"] == {"Conventional Machining": {r1: ["Painting"]}}
    assert block["waves"][3]["removed_under_failed_ancestor"] == {"Painting": [r4]}
    assert block["waves"][3]["leaf_step"]["CNC Machining"][r1] == {"declined": "nothing narrower"}
    assert block["proposal_wave"]["Cerakote Coating"] == {"sources": ["grounding"], "records": [r4], "accepted": [r4], "rejected": {}}
    assert block["proposal_wave"]["Fastened Assembly"]["rejected"] == {r5: "SCR-0"}
    assert block["shipped"] == {"in_vocab_by_record": {r1: ["CNC Machining"], r5: ["Joining"]}, "out_of_vocab": ["Cerakote Coating"]}


def test_the_partial_dump_knows_the_step2_stages():
    assert _GROUNDING_STAGE_ROW_KEYS[PipelineStage.grounding] == "grounding"
    assert _GROUNDING_STAGE_ROW_KEYS[PipelineStage.proposal] == "proposal_pass"
    assert PipelineStage.unit_screening in _SCREENING_STAGES
