"""Phase 2.6 of pipeline v2 (PIPELINE_V2_PLAN.md): per-candidate screening —
derived verdicts, both axes held exactly — plus the pure stage derivations
(2.7/2.8 halves)."""

import json

import pytest

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.grounding import RecordGroundingEntry
from core.models.extraction_schemas.relationship import (
    MaskedPhraseRelationshipRecord,
    PhraseMention,
    PhraseRelationshipRecord,
)
from core.models.extraction_schemas.screening import CandidateScreeningVerdict
from core.models.rule_catalog import STAGE_RELATIONSHIP_SCREENING, RuleCatalog
from core.services.phrase_blocks_contract import render_record_blocks
from core.services.pipeline_nodes.multi_stage.llm_screening_node_service_v2 import (
    DUMMY_SCREENINGS_RESPONSE_CONTENT,
    build_screening_payloads,
    hold_candidates_to_sent_records,
    parse_record_screening_result,
)
from core.services.pipeline_nodes.multi_stage.pipeline_v2_derivations import (
    candidates_for_screening,
    candidates_that_passed,
    descent_seed_tagging_results,
    passed_candidates_by_record,
)

CATALOG = RuleCatalog.model_validate(
    {
        "catalog_version": "test_screening.1",
        "prompt_name": "test_screening",
        "stage": STAGE_RELATIONSHIP_SCREENING,
        "field_types": ["industries"],
        "entity_noun": "industry",
        "entity_relationships": {
            "base": "serve",
            "third_person": "serves",
            "gerund": "serving",
        },
        "outcome_vocab": {
            "condition": ["satisfied", "failed", "not_triggered"],
            "guard": ["violated"],
        },
        "sections": [
            {
                "section_id": "pass_conditions",
                "heading": "Conditions:",
                "combinator": "all",
                "rules": [
                    {
                        "id": "TS-1",
                        "kind": "condition",
                        "reportable": True,
                        "report_when": "always",
                        "text": "Substantive.",
                    },
                    {
                        "id": "TS-2",
                        "kind": "condition",
                        "reportable": True,
                        "report_when": "always",
                        "text": "The manufacturer's own.",
                    },
                ],
            },
            {
                "section_id": "guards",
                "heading": "Guards:",
                "combinator": "any",
                "rules": [
                    {
                        "id": "TS-G1",
                        "kind": "guard",
                        "reportable": True,
                        "report_when": "on_violation",
                        "text": "Aspirational only.",
                    }
                ],
            },
        ],
        "published": {},
    }
)


def _unit(candidate: str, *, ts2: str = "satisfied", guard: bool = False) -> dict:
    return {
        "candidate": candidate,
        "TS-1": {"outcome": "satisfied", "explanation": "e"},
        "TS-2": {
            "outcome": ts2,
            "explanation": "e",
        },
        "guards": (
            [{"rule_id": "TS-G1", "explanation": "e"}] if guard else []
        ),
    }


def _response(entries: list[dict]) -> str:
    return json.dumps({"screenings": entries})


def test_parse_derives_passed_per_candidate():
    response = _response(
        [
            {
                "record_id": "raaaaaa1",
                "candidates": [
                    _unit("Aerospace Industry"),
                    _unit("Machining", ts2="failed"),
                ],
            },
            {
                "record_id": "rbbbbbb2",
                "candidates": [_unit("Aerospace Industry", guard=True)],
            },
        ]
    )
    parsed = parse_record_screening_result(response, catalog=CATALOG)
    first = parsed["raaaaaa1"]
    assert first["Aerospace Industry"].passed is True
    assert first["Machining"].passed is False
    # Every condition held but the guard fired: rejected by the guard alone.
    assert parsed["rbbbbbb2"]["Aerospace Industry"].passed is False
    assert [
        r.rule_id for r in parsed["rbbbbbb2"]["Aerospace Industry"].applied_rules
    ] == ["TS-1", "TS-2", "TS-G1"]


def test_duplicate_records_and_candidates_raise():
    entry = {"record_id": "raaaaaa1", "candidates": [_unit("Machining")]}
    with pytest.raises(ValueError, match="Duplicate record id"):
        parse_record_screening_result(_response([entry, entry]), catalog=CATALOG)
    doubled = {
        "record_id": "raaaaaa1",
        "candidates": [_unit("Machining"), _unit("Machining")],
    }
    with pytest.raises(ValueError, match="twice"):
        parse_record_screening_result(_response([doubled]), catalog=CATALOG)


def test_the_dummy_content_parses_to_an_empty_map():
    assert (
        parse_record_screening_result(
            DUMMY_SCREENINGS_RESPONSE_CONTENT, catalog=CATALOG
        )
        == {}
    )


def _verdict(passed: bool) -> CandidateScreeningVerdict:
    return CandidateScreeningVerdict(
        passed=passed,
        applied_rules=[
            AppliedRule(rule_id="TS-1", outcome="satisfied", explanation="e")
        ],
    )


def test_candidate_axis_holds_to_the_requests_own_payload():
    payloads = {
        "raaaaaa1": {
            "mentions": [],
            "synthesis": "s",
            "candidates": ["Aerospace Industry", "Machining"],
        }
    }
    message = render_record_blocks(payloads)

    hold_candidates_to_sent_records(
        user_message=message,
        held_results={
            "raaaaaa1": {
                "Aerospace Industry": _verdict(True),
                "Machining": _verdict(False),
            }
        },
        where="test",
    )

    with pytest.raises(ValueError, match="no verdict came back"):
        hold_candidates_to_sent_records(
            user_message=message,
            held_results={"raaaaaa1": {"Aerospace Industry": _verdict(True)}},
            where="test",
        )

    with pytest.raises(ValueError, match="never sent"):
        hold_candidates_to_sent_records(
            user_message=message,
            held_results={
                "raaaaaa1": {
                    "Aerospace Industry": _verdict(True),
                    "Machining": _verdict(False),
                    "Space Tourism": _verdict(True),
                }
            },
            where="test",
        )


def _masked(record_id_phrase: dict[str, str]):
    return {
        rid: MaskedPhraseRelationshipRecord(
            phrase=phrase,
            record=PhraseRelationshipRecord(
                mentions=[PhraseMention(form=phrase, page="/", account="a")],
                synthesis="s",
            ),
        )
        for rid, phrase in record_id_phrase.items()
    }


def test_screening_payloads_carry_evidence_plus_sorted_candidates():
    masked = _masked({"raaaaaa1": "aerospace"})
    payloads = build_screening_payloads(
        masked, {"raaaaaa1": ["Machining", "Aerospace Industry"]}
    )
    assert payloads["raaaaaa1"]["candidates"] == ["Aerospace Industry", "Machining"]
    assert "phrase" not in payloads["raaaaaa1"]

    with pytest.raises(ValueError, match="unknown record id"):
        build_screening_payloads(masked, {"rZZZZZZ9": ["X"]})

    assert build_screening_payloads(masked, {"raaaaaa1": []}) == {}


# --- derivations (2.7/2.8 halves) ------------------------------------------


def _grounding(entries: dict[str, dict[str, list]]) -> dict:
    return {
        rid: RecordGroundingEntry(tags=tags) if tags else RecordGroundingEntry(
            tags={}, explanation="nothing"
        )
        for rid, tags in entries.items()
    }


def test_candidates_for_screening_unions_and_dedupes_preferring_in_vocab():
    in_vocab = _grounding({"raaaaaa1": {"Aerospace Industry": []}, "rbbbbbb2": {}})
    oov = _grounding(
        {"raaaaaa1": {"aerospace industry": [], "Space Tourism": []}}
    )
    merged = candidates_for_screening(in_vocab, oov)
    # The OOV mint that restates the vocabulary label folded into it; the
    # declined record contributes nothing and is absent.
    assert merged == {"raaaaaa1": ["Aerospace Industry", "Space Tourism"]}


def test_descent_seed_groups_passed_in_vocab_tags_across_records():
    in_vocab = _grounding(
        {
            "raaaaaa1": {"Aerospace Industry": [], "Machining": []},
            "rbbbbbb2": {"Aerospace Industry": []},
        }
    )
    screening = {
        "raaaaaa1": {
            "Aerospace Industry": _verdict(True),
            "Machining": _verdict(False),
        },
        "rbbbbbb2": {"Aerospace Industry": _verdict(True)},
    }
    seed = descent_seed_tagging_results(in_vocab, screening)
    assert [tr.group_id for tr in seed] == ["Aerospace Industry"]
    assert set(seed[0].phrase_rules_map) == {"raaaaaa1", "rbbbbbb2"}


def test_candidates_that_passed_needs_one_qualifying_record():
    oov = _grounding(
        {
            "raaaaaa1": {"Space Tourism": []},
            "rbbbbbb2": {"Space Tourism": [], "Vertical Farming": []},
        }
    )
    screening = {
        "raaaaaa1": {"Space Tourism": _verdict(False)},
        "rbbbbbb2": {
            "Space Tourism": _verdict(True),
            "Vertical Farming": _verdict(False),
        },
    }
    assert candidates_that_passed(oov, screening) == {"Space Tourism"}
    assert passed_candidates_by_record(screening) == {
        "rbbbbbb2": {"Space Tourism"}
    }
