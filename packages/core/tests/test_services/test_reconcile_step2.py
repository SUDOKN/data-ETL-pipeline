"""The reconcile step over the descent trail (Step 2 cutover 6b, 2026-09-22):
what ships is decided HERE, from the stored trail, never inside the descent
node — the deepest accepted label per record replaces its ancestors, the
proposal wave's accepted proposals ship out of vocabulary, and every proposal
lands in ``vocabulary_candidates`` with its records, quotes, sources and
verdict.

The trail below is the walkthrough's (test_descent_node_step2): Machining >
Conventional Machining > CNC Machining; Surface Finishing > Coating >
Painting; Joining (other name Assembly) > Mechanical Joining.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from core.db_models.vocabulary_candidates import VocabularyCandidate
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.descent import DescentTrail, WaveTrail
from core.models.extraction_schemas.grounding import RecordGroundingEntry
from core.models.extraction_schemas.screening import CandidateScreeningVerdict
from core.models.extraction_schemas.synthesis import GroupRecord
from core.models.skos_concept import Concept
from core.services.pipeline_nodes.multi_stage.llm_descent_node_service import (
    Vocabulary,
    accepted_proposals,
    leaf_step_view,
    merged_screening,
    quote_of,
    shipped_labels_by_record,
    split_grounding_results,
    vocabulary_candidates_from,
)
from core.services.vocabulary_candidate_service import encode_vocabulary_candidate

T0 = datetime(2026, 9, 22, 12, 0, 0)


def _concept(name: str, level: int, ancestors: list[str], alt: list[str] | None = None, definition: str = "") -> Concept:
    return Concept(name=name, uri=f"urn:{name}", level=level, ancestors=ancestors, altLabels=alt or [], definition=definition, children=[])


@pytest.fixture(scope="module")
def vocab() -> Vocabulary:
    return Vocabulary({
        _concept("Machining", 1, []), _concept("Conventional Machining", 2, ["Machining"]),
        _concept("CNC Machining", 3, ["Machining", "Conventional Machining"]),
        _concept("Surface Finishing", 1, []), _concept("Coating", 2, ["Surface Finishing"]),
        _concept("Painting", 3, ["Surface Finishing", "Coating"]),
        _concept("Joining", 1, [], alt=["Assembly"]), _concept("Mechanical Joining", 2, ["Joining"]),
    })


@pytest.fixture(autouse=True, scope="module")
def offline_candidate_settings():
    from beanie.odm.settings.document import DocumentSettings

    settings_class = getattr(VocabularyCandidate, "Settings")  # noqa: B009
    VocabularyCandidate._document_settings = DocumentSettings(
        **{a: getattr(settings_class, a) for a in dir(settings_class) if not a.startswith("__")}
    )


def _rules(quote: str, outcome: str = "satisfied") -> list[AppliedRule]:
    return [AppliedRule(rule_id="GR-E1", outcome=outcome, explanation=quote), AppliedRule(rule_id="GR-M1", outcome="chosen", explanation="")]


def _verdict(passed: bool, failed_rule: str | None = None) -> CandidateScreeningVerdict:
    return CandidateScreeningVerdict(passed=passed, applied_rules=[], failed_rule=failed_rule, quote="q")


def _trail() -> DescentTrail:
    trail = DescentTrail(max_depth=3)
    trail.waves[1] = WaveTrail(
        units={"Machining": ["r2"], "Joining": ["r5"]},
        screening={"r2": {"Machining": _verdict(True)}, "r5": {"Joining": _verdict(True)}},
        descent={"Machining": {"r2": RecordGroundingEntry(tags={"Conventional Machining": _rules("machining")})}},
        leaf={},
        false_children={"Machining": {"r2": ["Painting"]}},
    )
    trail.waves[2] = WaveTrail(
        units={"Conventional Machining": ["r2"], "Coating": ["r4", "r7"], "Mechanical Joining": ["r5"]},
        screening={
            "r2": {"Conventional Machining": _verdict(True)},
            "r4": {"Coating": _verdict(False, "SCR-2")},
            "r7": {"Coating": _verdict(True)},
            "r5": {"Mechanical Joining": _verdict(True)},
        },
        descent={
            "Conventional Machining": {"r2": RecordGroundingEntry(tags={"CNC Machining": _rules("CNC")})},
            "Coating": {"r7": RecordGroundingEntry(tags={"Ceramic Coating": _rules("Cerakote ceramic", "unverified")})},
        },
        leaf={"Mechanical Joining": {"r5": RecordGroundingEntry(tags={"Fastened Assembly": _rules("fastened")})}},
    )
    trail.waves[3] = WaveTrail(
        units={"CNC Machining": ["r1", "r2"]},
        screening={"r1": {"CNC Machining": _verdict(True)}, "r2": {"CNC Machining": _verdict(True)}},
        leaf={"CNC Machining": {"r1": RecordGroundingEntry(tags={}, explanation="nothing narrower")}},
        removed_under_failed_ancestor={"Painting": ["r4"]},
    )
    trail.proposal_units = {"Ceramic Coating": ["r7"], "Cerakote Coating": ["r7"], "Fastened Assembly": ["r5"]}
    trail.proposal_sources = {"Ceramic Coating": ["descent:Coating"], "Cerakote Coating": ["grounding"], "Fastened Assembly": ["leaf:Mechanical Joining"]}
    trail.proposal_screening = {
        "r7": {"Ceramic Coating": _verdict(True), "Cerakote Coating": _verdict(False, "SCR-1")},
        "r5": {"Fastened Assembly": _verdict(False, "SCR-0")},
    }
    return trail


def test_the_deepest_accepted_label_replaces_its_ancestors_per_record(vocab):
    shipped = shipped_labels_by_record(vocab, _trail())
    assert shipped == {
        "r1": ["CNC Machining"],
        "r2": ["CNC Machining"],  # Machining and Conventional Machining accepted too, replaced
        "r5": ["Mechanical Joining"],
        "r7": ["Coating"],  # its child was never screened on r7: the parent stands
    }
    assert "r4" not in shipped  # Coating failed on r4 and Painting was removed under it


def test_only_accepted_proposals_ship_out_of_vocabulary():
    assert accepted_proposals(_trail()) == {"Ceramic Coating"}


def test_merged_screening_holds_every_wave_and_the_proposal_wave():
    merged = merged_screening(_trail())
    assert set(merged["r2"]) == {"Machining", "Conventional Machining", "CNC Machining"}
    assert merged["r4"]["Coating"].failed_rule == "SCR-2"
    assert merged["r7"]["Cerakote Coating"].passed is False and merged["r7"]["Coating"].passed is True


def test_the_leaf_step_view_is_keyed_by_leaf():
    view = leaf_step_view(_trail())
    assert set(view) == {"Mechanical Joining", "CNC Machining"}
    assert view["CNC Machining"]["r1"].explanation == "nothing narrower"


def test_split_grounding_results_keeps_the_declination_and_the_dropped_lists(vocab):
    results = {
        "r2": RecordGroundingEntry(tags={"Assembly": _rules("assembly"), "Cerakote Coating": _rules("cerakote")}, dropped_options=["Cerakote Coating"]),
        "r3": RecordGroundingEntry(tags={}, explanation="names nothing the vocabulary holds"),
    }
    in_vocab, proposals = split_grounding_results(vocab, results)
    assert set(in_vocab["r2"].tags) == {"Assembly"} and in_vocab["r2"].dropped_options == ["Cerakote Coating"]
    assert in_vocab["r3"].explanation == "names nothing the vocabulary holds"
    assert set(proposals) == {"r2"} and set(proposals["r2"].tags) == {"Cerakote Coating"}


def test_quote_of_reads_the_evidence_condition_verified_or_not():
    assert quote_of(_rules("the words", "satisfied")) == "the words"
    assert quote_of(_rules("the words", "unverified")) == "the words"
    assert quote_of([AppliedRule(rule_id="GR-M1", outcome="chosen", explanation="x")]) == ""


def test_every_proposal_becomes_a_candidate_with_its_verdict_sources_and_quotes():
    records = {
        "r5": GroupRecord(focal_form="assembly", synthesis="s"),
        "r7": GroupRecord(focal_form="Cerakote", synthesis="s"),
    }
    grounding_proposals = {"r7": RecordGroundingEntry(tags={"Cerakote Coating": _rules("Cerakote finishes")})}
    candidates = {c.label: c for c in vocabulary_candidates_from(
        trail=_trail(), group_records=records, grounding_proposals=[grounding_proposals],
        subject_unique_id="acme", field_name="process_caps", run_timestamp=T0, ontology_version_id="ont-1", created_at=T0,
    )}
    assert set(candidates) == {"Ceramic Coating", "Cerakote Coating", "Fastened Assembly"}
    ceramic = candidates["Ceramic Coating"]
    assert (ceramic.parent_label, ceramic.sources, ceramic.accepted, ceramic.failed_rule) == ("Coating", ["descent:Coating"], True, None)
    assert [(e.record_id, e.focal_form, e.quote) for e in ceramic.records] == [("r7", "Cerakote", "Cerakote ceramic")]
    cerakote = candidates["Cerakote Coating"]
    assert (cerakote.parent_label, cerakote.accepted, cerakote.failed_rule) == ("", False, "SCR-1")
    assert cerakote.records[0].quote == "Cerakote finishes"
    fastened = candidates["Fastened Assembly"]
    assert (fastened.parent_label, fastened.accepted, fastened.failed_rule) == ("Mechanical Joining", False, "SCR-0")
    assert fastened.records[0].quote == "fastened"


def test_a_candidate_encodes_for_bson_without_an_id():
    record = GroupRecord(focal_form="assembly", synthesis="s")
    [candidate] = vocabulary_candidates_from(
        trail=_trail(), group_records={"r5": record}, grounding_proposals=[],
        subject_unique_id="acme", field_name="process_caps", run_timestamp=T0, ontology_version_id="ont-1", created_at=T0,
    )[2:]
    document = encode_vocabulary_candidate(candidate)
    assert "_id" not in document and "id" not in document
    assert document["run_timestamp"] == T0 and document["records"][0]["record_id"] == "r5"
    assert VocabularyCandidate.model_validate(document).label == "Fastened Assembly"
