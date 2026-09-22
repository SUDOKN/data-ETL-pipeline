"""Phase 2.6 of pipeline v2 (PIPELINE_V2_PLAN.md): per-candidate screening —
derived verdicts, both axes held exactly — plus the pure stage derivations
(2.7/2.8 halves)."""

import json

import pytest

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.grounding import RecordGroundingEntry
from core.models.extraction_schemas.synthesis import GroupRecord
from core.models.extraction_schemas.screening import CandidateScreeningVerdict
from core.models.rule_catalog import STAGE_RELATIONSHIP_SCREENING, RuleCatalog
from core.services.phrase_blocks_contract import render_record_blocks
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    DUMMY_SCREENINGS_RESPONSE_CONTENT,
    build_screening_payloads,
    hold_candidates_to_sent_records,
    parse_record_screening_result,
)
from core.services.pipeline_nodes.multi_stage.stage_derivations import (
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


def _groups(group_id_focal: dict[str, str]):
    # v3 (3.3, D16): screening's records are the synthesis stage's per-group
    # records — focal form + synthesis, keyed by the opaque group_id.
    return {
        gid: GroupRecord(focal_form=focal, synthesis="s")
        for gid, focal in group_id_focal.items()
    }


def test_screening_payloads_carry_evidence_plus_sorted_candidates():
    groups = _groups({"g1": "aerospace"})
    payloads = build_screening_payloads(
        groups, {"g1": ["Machining", "Aerospace Industry"]}
    )
    assert payloads["g1"]["candidates"] == ["Aerospace Industry", "Machining"]
    assert payloads["g1"]["focal_form"] == "aerospace"
    assert payloads["g1"]["synthesis"] == "s"

    with pytest.raises(ValueError, match="unknown group id"):
        build_screening_payloads(groups, {"gZ": ["X"]})

    assert build_screening_payloads(groups, {"g1": []}) == {}


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


# --- Step 2: unit-major structural screening (2026-09-21) --------------------

from core.models.rule_catalog import STAGE_UNIT_SCREENING  # noqa: E402
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (  # noqa: E402
    parse_unit_screening_result,
    render_units_block,
)


def _unit_catalog() -> RuleCatalog:
    def rule(rid, kind, text):
        return {"id": rid, "kind": kind, "reportable": True, "report_when": {"condition": "always", "guard": "on_violation"}[kind], "text": text}

    return RuleCatalog.model_validate(
        {
            "catalog_version": "test_unit_screening.1",
            "prompt_name": "test_unit_screening",
            "stage": STAGE_UNIT_SCREENING,
            "field_types": ["industries"],
            "entity_noun": "industry",
            "entity_relationships": {"base": "serve", "third_person": "serves", "gerund": "serving"},
            "reporting": "structural",
            "outcome_vocab": {},
            "sections": [
                {"section_id": "conditions", "heading": "All must hold:", "combinator": "all",
                 "rules": [rule("SCR-0", "condition", "Kind."), rule("SCR-1", "condition", "Named."), rule("SCR-2", "condition", "The manufacturer's.")]},
                {"section_id": "guards", "heading": "Any rules out:", "combinator": "any", "rules": [rule("SCR-G1", "guard", "Not current.")]},
            ],
            "published": {},
        }
    )


UNIT_CATALOG = _unit_catalog()
UNIT_RECORDS = {
    "r1": {"subject": "aerospace", "synthesis": "Acme serves the aerospace market with machined parts."},
    "r2": {"subject": "cars", "synthesis": "A customer of Acme makes cars."},
}
UNITS = {"Aerospace Industry": ["r1"], "Automotive": ["r1", "r2"]}


def _screen_unit(option, accepted=(), not_accepted=()):
    return {
        "option": option,
        "accepted": [{"record_id": r, "evidence": e, "quote": q} for r, e, q in accepted],
        "not_accepted": [{"record_id": r, "failed_rule": f, "quote": q} for r, f, q in not_accepted],
    }


def _parse_units(*units):
    return parse_unit_screening_result(json.dumps({"screenings": list(units)}), catalog=UNIT_CATALOG, units=UNITS, sent_records=UNIT_RECORDS)


def test_unit_screening_stores_passed_with_evidence_and_failed_with_the_rule_and_quote():
    results = _parse_units(
        _screen_unit("Aerospace Industry", accepted=[("r1", "named", "serves the aerospace market")]),
        _screen_unit("Automotive", accepted=[("r1", "inferred", "machined parts")], not_accepted=[("r2", "SCR-2", "A customer of Acme makes cars")]),
    )
    assert results["r1"]["Aerospace Industry"].passed and results["r1"]["Aerospace Industry"].evidence == "named"
    assert results["r1"]["Aerospace Industry"].applied_rules == []
    v = results["r2"]["Automotive"]
    assert not v.passed and v.failed_rule == "SCR-2" and v.quote == "A customer of Acme makes cars"
    assert [(r.rule_id, r.outcome) for r in v.applied_rules] == [("SCR-2", "failed")]


@pytest.mark.parametrize(
    "units, message",
    [
        ([_screen_unit("Aerospace Industry", accepted=[("r1", "named", "serves the aerospace market")])], "never answered"),
        ([_screen_unit("Aerospace Industry", accepted=[("r1", "named", "serves the aerospace")]), _screen_unit("Automotive", accepted=[("r1", "named", "machined")]), _screen_unit("Steel", accepted=[])], "never sent"),
        ([_screen_unit("Aerospace Industry", accepted=[("r1", "named", "serves the aerospace")]), _screen_unit("Automotive", accepted=[("r1", "named", "machined")], not_accepted=[("r1", "SCR-1", "x")])], "both accepted and not"),
        ([_screen_unit("Aerospace Industry", accepted=[("r1", "named", "serves the aerospace")]), _screen_unit("Automotive", not_accepted=[("r2", "SCR-2", "cars")])], "not the sent records"),
        ([_screen_unit("Aerospace Industry", accepted=[("r1", "named", "serves the lunar market")]), _screen_unit("Automotive", not_accepted=[("r1", "SCR-0", "a"), ("r2", "SCR-2", "b")])], "not in the record"),
    ],
)
def test_unit_screening_holds_fail_the_response(units, message):
    with pytest.raises(ValueError, match=message):
        _parse_units(*units)


def test_units_block_round_trips_through_json():
    block = render_units_block([{"option": "Automotive", "meaning": "cars", "records": ["r1", "r2"]}])
    body = block.split("<<<UNITS\n", 1)[1].rsplit("\nUNITS>>>", 1)[0]
    assert json.loads(body) == [{"option": "Automotive", "meaning": "cars", "records": ["r1", "r2"]}]
