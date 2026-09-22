"""Phases 2.3–2.5 of pipeline v2 (PIPELINE_V2_PLAN.md): the shared grounding
parse — record-keyed, vocabulary-held for the in-vocab pass, minted labels for
OOV/freehand, and the structural declination preserved into storage."""

import json

import pytest

from core.models.rule_catalog import RuleCatalog

# The per-rule option/candidate parse under test predates Step 2 and still
# serves the freehand pass; the stage names here are labels for the two
# catalog shapes (a ladder of matching branches vs minted candidates).
STAGE_OPTION = "phrase_freehand_grounding"
STAGE_CANDIDATE = "phrase_freehand_grounding"
from core.services.applied_rule_validation import AppliedRuleValidationError
from core.services.phrase_blocks_contract import render_record_blocks
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    DUMMY_GROUNDINGS_RESPONSE_CONTENT,
    build_group_record_payloads,
    parse_record_grounding_result,
    retry_record_payloads,
)
from core.models.extraction_schemas.grounding import RecordGroundingEntry
from core.models.extraction_schemas.synthesis import GroupRecord


def _catalog(stage: str, *, with_ladder: bool) -> RuleCatalog:
    sections = [
        {
            "section_id": "evidence",
            "heading": "For each {{entity_noun}}:",
            "combinator": "all",
            "rules": [
                {
                    "id": "TG-E1",
                    "kind": "condition",
                    "reportable": True,
                    "report_when": "always",
                    "text": "The record evidences it.",
                }
            ],
        }
    ]
    vocab = {"condition": ["satisfied", "failed", "not_triggered"]}
    if with_ladder:
        sections.append(
            {
                "section_id": "matching",
                "heading": "Match by the first that applies:",
                "combinator": "ordered",
                "rules": [
                    {
                        "id": "TG-M1",
                        "kind": "preference",
                        "reportable": True,
                        "report_when": "when_chosen",
                        "text": "Exact match chosen.",
                    },
                    {
                        "id": "TG-M2",
                        "kind": "preference",
                        "reportable": True,
                        "report_when": "when_chosen",
                        "text": "Generalization chosen.",
                    },
                ],
            }
        )
        vocab["preference"] = ["chosen"]
    return RuleCatalog.model_validate(
        {
            "catalog_version": f"test_grounding_{stage}.1",
            "prompt_name": f"test_grounding_{stage}",
            "stage": stage,
            "field_types": ["industries"],
            "entity_noun": "industry",
            "entity_relationships": {
                "base": "serve",
                "third_person": "serves",
                "gerund": "serving",
            },
            "outcome_vocab": vocab,
            "sections": sections,
            "published": {},
        }
    )


IN_VOCAB = _catalog(STAGE_OPTION, with_ladder=True)
OOV = _catalog(STAGE_CANDIDATE, with_ladder=False)
VOCAB_LABELS = ["Aerospace Industry", "Machining"]


def _option(label: str) -> dict:
    return {
        "option": label,
        "TG-E1": {"outcome": "satisfied", "explanation": "e"},
        "chosen": {"rule_id": "TG-M1", "explanation": "e"},
    }


def _candidate(label: str) -> dict:
    return {
        "candidate": label,
        "TG-E1": {"outcome": "satisfied", "explanation": "e"},
    }


def test_the_dummy_content_parses_to_an_empty_map():
    assert (
        parse_record_grounding_result(
            DUMMY_GROUNDINGS_RESPONSE_CONTENT,
            catalog=IN_VOCAB,
            allowed_labels=VOCAB_LABELS,
        )
        == {}
    )


def test_group_record_payloads_carry_the_record_and_optionally_the_prior_results():
    # v3 (3.3, D16): the downstream record is the group's focal form + its
    # synthesis; the opaque group_id is the key and never rides inside.
    groups = {
        "g1": GroupRecord(focal_form="Aerospace", synthesis="s"),
    }
    plain = build_group_record_payloads(groups)
    assert plain["g1"] == {"focal_form": "Aerospace", "synthesis": "s"}

    with_prior = build_group_record_payloads(
        groups, already_identified={"g1": ["Machining", "Aerospace Industry"]}
    )
    assert with_prior["g1"]["already_identified"] == [
        "Aerospace Industry",
        "Machining",
    ]
    # Renders into the fenced blocks without complaint.
    assert "<<<RECORDS" in render_record_blocks(with_prior)


def test_retry_record_payloads_restrict_to_the_stored_ids_and_raise_on_drift():
    payloads = build_group_record_payloads(
        {
            "g1": GroupRecord(focal_form="Aerospace", synthesis="s"),
            "g2": GroupRecord(focal_form="Machining", synthesis="t"),
        }
    )
    assert set(retry_record_payloads("x", "sub", "f", "0:10", payloads, ["g2"])) == {
        "g2"
    }
    with pytest.raises(ValueError, match="changed under the stored"):
        retry_record_payloads("x", "sub", "f", "0:10", payloads, ["g3"])


# --- Step 2: the record-major structural wire (2026-09-21) -------------------

from core.models.rule_catalog import STAGE_GROUNDING  # noqa: E402
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (  # noqa: E402
    parse_record_grounding_structural_result,
    split_vocabulary_and_proposals,
    subject_keyed_payloads,
)
from core.services.pipeline_nodes.multi_stage.stage_derivations import units_for_screening  # noqa: E402


def _structural_catalog() -> RuleCatalog:
    def rule(rid, kind, text):
        return {
            "id": rid, "kind": kind, "reportable": True,
            "report_when": {"condition": "always", "preference": "when_chosen", "proposal": "when_chosen"}[kind],
            "text": text,
        }

    return RuleCatalog.model_validate(
        {
            "catalog_version": "test_structural_grounding.1",
            "prompt_name": "test_structural_grounding",
            "stage": STAGE_GROUNDING,
            "field_types": ["industries"],
            "entity_noun": "industry",
            "entity_relationships": {"base": "serve", "third_person": "serves", "gerund": "serving"},
            "reporting": "structural",
            "outcome_vocab": {},
            "sections": [
                {"section_id": "evidence", "heading": "Both must hold:", "combinator": "all",
                 "rules": [rule("GR-E1", "condition", "Quote it."), rule("GR-K1", "condition", "Of the kind.")]},
                {"section_id": "matching", "heading": "In order:", "combinator": "ordered",
                 "rules": [rule("GR-M1", "preference", "Exact."), rule("GR-M2", "preference", "Generalizing."), rule("GR-P1", "proposal", "Propose.")]},
            ],
            "published": {},
        }
    )


STRUCTURAL = _structural_catalog()
LABELS = ["Aerospace Industry", "Automotive", "Medical Devices"]
RECORDS = {
    "raaaaaa1": {"subject": "aerospace", "synthesis": "The site lists aerospace among the industries it serves."},
    "raaaaaa2": {"subject": "Car care", "synthesis": "Car care products are listed under private label offerings."},
    "raaaaaa3": {"subject": "Welding", "synthesis": "Welding is a capability, not a market."},
}


def _entry(rid, options=(), proposals=(), explanation=None):
    return {
        "record_id": rid,
        "options": [{"option": o, "quote": q, "match": m} for o, q, m in options],
        "proposals": [{"label": l, "quote": q, "explanation": e} for l, q, e in proposals],
        "explanation": explanation,
    }


def _parse(*entries):
    return parse_record_grounding_structural_result(
        json.dumps({"groundings": list(entries)}), catalog=STRUCTURAL, allowed_labels=LABELS, sent_records=RECORDS
    )


def test_structural_options_store_the_quote_and_the_branch_and_proposals_ride_beside():
    parsed = _parse(
        _entry("raaaaaa1", options=[("aerospace industry", "lists aerospace among the industries it serves", "GR-M1")]),
        _entry("raaaaaa2", proposals=[("Car Care Products", "Car care products are listed", "no option names it")]),
        _entry("raaaaaa3", explanation="a capability, not a market"),
    )
    assert set(parsed) == {"raaaaaa1", "raaaaaa2", "raaaaaa3"}
    rules = parsed["raaaaaa1"].tags["Aerospace Industry"]  # casing repaired to the vocabulary's spelling
    assert [(r.rule_id, r.outcome) for r in rules] == [("GR-E1", "satisfied"), ("GR-M1", "chosen")]
    assert rules[0].explanation == "lists aerospace among the industries it serves"
    assert rules[1].explanation == ""  # the per-match prose was dropped by decision
    assert [(r.rule_id, r.outcome) for r in parsed["raaaaaa2"].tags["Car Care Products"]] == [("GR-E1", "satisfied"), ("GR-P1", "chosen")]
    assert parsed["raaaaaa3"].tags == {} and parsed["raaaaaa3"].explanation == "a capability, not a market"
    in_vocab, proposals = split_vocabulary_and_proposals(parsed, LABELS)
    assert set(in_vocab) == {"raaaaaa1"} and set(proposals) == {"raaaaaa2"}
    assert units_for_screening(in_vocab, proposals) == {"Aerospace Industry": ["raaaaaa1"], "Car Care Products": ["raaaaaa2"]}


def test_structural_membership_reroutes_a_non_label_option_and_folds_a_label_shaped_proposal():
    parsed = _parse(
        _entry("raaaaaa1", options=[("Aerospace & Defence Sector", "lists aerospace among", "GR-M2")]),
        _entry("raaaaaa2", proposals=[("automotive (also: cars)", "Car care products", "no option")]),
    )
    e1 = parsed["raaaaaa1"]
    assert list(e1.tags) == ["Aerospace & Defence Sector"] and e1.dropped_options == ["Aerospace & Defence Sector"]
    assert e1.tags["Aerospace & Defence Sector"][1].rule_id == "GR-P1"  # rerouted to the proposal branch
    assert list(parsed["raaaaaa2"].tags) == ["Automotive"]  # folded into the vocabulary under its spelling
    in_vocab, proposals = split_vocabulary_and_proposals(parsed, LABELS)
    assert set(in_vocab) == {"raaaaaa2"} and set(proposals) == {"raaaaaa1"}


def test_structural_quote_not_in_the_record_keeps_the_unit_marked_unverified_and_an_empty_quote_drops_it():
    """User decision 2026-09-21: a quote the record does not contain verbatim
    keeps its option (the model's copy may differ by a spelling) with the
    evidence rule marked ``unverified``; only a unit offered with NO quote is
    dropped, and a record that offered nothing but empty quotes is left for
    the retry."""
    parsed = _parse(
        _entry("raaaaaa1", options=[("Aerospace Industry", "serves the aerospace market", "GR-M1"), ("Automotive", "lists aerospace among the industries", "GR-M2")]),
        _entry("raaaaaa2", options=[("Automotive", "", "GR-M1")]),
        _entry("raaaaaa3", proposals=[("Welding Services", "Welding is a capabilty", "no option")]),  # a misspelt copy
    )
    e1 = parsed["raaaaaa1"]
    assert list(e1.tags) == ["Aerospace Industry", "Automotive"] and e1.dropped_quotes == []
    assert [r.outcome for r in e1.tags["Aerospace Industry"]] == ["unverified", "chosen"]
    assert [r.outcome for r in e1.tags["Automotive"]] == ["satisfied", "chosen"]
    assert "raaaaaa2" not in parsed  # every unit offered no quote: unanswered, not declined
    assert [r.outcome for r in parsed["raaaaaa3"].tags["Welding Services"]] == ["unverified", "chosen"]


def test_structural_quote_matching_tolerates_case_whitespace_and_an_ellipsis():
    parsed = _parse(_entry("raaaaaa1", options=[("Aerospace Industry", "LISTS  aerospace … it serves", "GR-M1")]))
    assert list(parsed["raaaaaa1"].tags) == ["Aerospace Industry"]


def test_structural_empty_record_without_a_reason_and_a_repeated_id_are_left_for_the_retry():
    parsed = _parse(
        _entry("raaaaaa1", explanation=None),
        _entry("raaaaaa2", options=[("Automotive", "Car care products", "GR-M2")]),
        _entry("raaaaaa2", explanation="nothing"),
        _entry("raaaaaa3", options=[("Automotive", "Welding is a capability", "GR-M1")], explanation="volunteered beside a tag"),
    )
    assert set(parsed) == {"raaaaaa3"}
    assert parsed["raaaaaa3"].explanation is None


def test_structural_a_label_listed_twice_for_one_record_keeps_the_first():
    parsed = _parse(_entry("raaaaaa1", options=[("Aerospace Industry", "lists aerospace", "GR-M1"), ("aerospace industry", "industries it serves", "GR-M2")]))
    assert parsed["raaaaaa1"].tags["Aerospace Industry"][1].rule_id == "GR-M1"


def test_subject_keyed_payloads_renders_the_phrase_as_subject_and_keeps_storage_keys_apart():
    payloads = {"g1": {"focal_form": "Drilling", "synthesis": "…"}}
    assert subject_keyed_payloads(payloads) == {"g1": {"subject": "Drilling", "synthesis": "…"}}
    assert payloads == {"g1": {"focal_form": "Drilling", "synthesis": "…"}}


def test_units_for_screening_folds_casings_across_records_and_sorts():
    a = {"r2": RecordGroundingEntry(tags={"Coating": []}), "r1": RecordGroundingEntry(tags={"coating": [], "Painting": []})}
    assert units_for_screening(a) == {"Coating": ["r1", "r2"], "Painting": ["r1"]}  # first spelling seen kept
