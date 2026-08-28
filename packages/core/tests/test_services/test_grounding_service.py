"""Phases 2.3–2.5 of pipeline v2 (PIPELINE_V2_PLAN.md): the shared grounding
parse — record-keyed, vocabulary-held for the in-vocab pass, minted labels for
OOV/freehand, and the structural declination preserved into storage."""

import json

import pytest

from core.models.rule_catalog import (
    STAGE_INITIAL_GROUNDING,
    STAGE_OOV_GROUNDING,
    RuleCatalog,
)
from core.services.applied_rule_validation import AppliedRuleValidationError
from core.services.phrase_blocks_contract import render_record_blocks
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    DUMMY_GROUNDINGS_RESPONSE_CONTENT,
    build_group_record_payloads,
    parse_record_grounding_result,
    retry_record_payloads,
)
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


IN_VOCAB = _catalog(STAGE_INITIAL_GROUNDING, with_ladder=True)
OOV = _catalog(STAGE_OOV_GROUNDING, with_ladder=False)
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


def test_in_vocab_parse_stores_tags_and_flattens_rules():
    response = json.dumps(
        {
            "groundings": [
                {
                    "record_id": "raaaaaa1",
                    "options": [_option("Aerospace Industry")],
                    "explanation": None,
                }
            ]
        }
    )
    parsed = parse_record_grounding_result(
        response, catalog=IN_VOCAB, allowed_labels=VOCAB_LABELS
    )
    entry = parsed["raaaaaa1"]
    assert list(entry.tags) == ["Aerospace Industry"]
    assert [r.rule_id for r in entry.tags["Aerospace Industry"]] == ["TG-E1", "TG-M1"]
    assert entry.explanation is None


def test_case_drift_is_repaired_to_the_vocabulary_spelling():
    response = json.dumps(
        {
            "groundings": [
                {
                    "record_id": "raaaaaa1",
                    "options": [_option("aerospace industry")],
                    "explanation": None,
                }
            ]
        }
    )
    parsed = parse_record_grounding_result(
        response, catalog=IN_VOCAB, allowed_labels=VOCAB_LABELS
    )
    assert list(parsed["raaaaaa1"].tags) == ["Aerospace Industry"]


def test_a_non_vocabulary_option_is_dropped_not_raised():
    """Fork F4 still holds — vocabulary-or-nothing, and a drifted or invented
    label is never a discovered out-of-vocab tag. What changed 2026-08-25 is
    the cost: the label is dropped onto ``dropped_options`` instead of failing
    the request and, through the recursive loop, the whole subject.

    The model volunteered no declination here, so the drop has to supply one:
    the stored entry's validator requires a reason whenever tags are empty.
    """
    response = json.dumps(
        {
            "groundings": [
                {
                    "record_id": "raaaaaa1",
                    "options": [_option("Aerospace & Defence Sector")],
                    "explanation": None,
                }
            ]
        }
    )
    parsed = parse_record_grounding_result(
        response, catalog=IN_VOCAB, allowed_labels=VOCAB_LABELS
    )
    entry = parsed["raaaaaa1"]
    assert entry.tags == {}
    assert entry.dropped_options == ["Aerospace & Defence Sector"]
    assert entry.explanation is not None
    assert "Aerospace & Defence Sector" in entry.explanation


def test_a_dropped_option_keeps_the_models_own_declination():
    """A record that volunteered a reason keeps it — the synthesized one is a
    fallback for silence, not a replacement for what the model said."""
    response = json.dumps(
        {
            "groundings": [
                {
                    "record_id": "raaaaaa1",
                    "options": [_option("Aerospace & Defence Sector")],
                    "explanation": "the record evidences no industry at all",
                }
            ]
        }
    )
    entry = parse_record_grounding_result(
        response, catalog=IN_VOCAB, allowed_labels=VOCAB_LABELS
    )["raaaaaa1"]
    assert entry.tags == {}
    assert entry.dropped_options == ["Aerospace & Defence Sector"]
    assert entry.explanation == "the record evidences no industry at all"


def test_a_dropped_option_beside_a_kept_one_leaves_the_record_grounded():
    """The drop is per option, not per record: a vocabulary label alongside an
    invented one still lands as a tag, and the record carries no explanation
    because it is not a declination."""
    response = json.dumps(
        {
            "groundings": [
                {
                    "record_id": "raaaaaa1",
                    "options": [
                        _option("Aerospace Industry"),
                        _option("Aerospace & Defence Sector"),
                    ],
                    "explanation": None,
                }
            ]
        }
    )
    entry = parse_record_grounding_result(
        response, catalog=IN_VOCAB, allowed_labels=VOCAB_LABELS
    )["raaaaaa1"]
    assert list(entry.tags) == ["Aerospace Industry"]
    assert entry.dropped_options == ["Aerospace & Defence Sector"]
    assert entry.explanation is None


def test_an_empty_record_with_no_reason_still_fails():
    """The drop path must not weaken the declination invariant for a record
    that simply yielded nothing and said nothing."""
    response = json.dumps(
        {
            "groundings": [
                {"record_id": "raaaaaa1", "options": [], "explanation": None}
            ]
        }
    )
    with pytest.raises(AppliedRuleValidationError, match="must say why"):
        parse_record_grounding_result(
            response, catalog=IN_VOCAB, allowed_labels=VOCAB_LABELS
        )


def test_minted_stages_accept_any_label():
    response = json.dumps(
        {
            "groundings": [
                {
                    "record_id": "raaaaaa1",
                    "candidates": [_candidate("Space Tourism")],
                    "explanation": None,
                }
            ]
        }
    )
    parsed = parse_record_grounding_result(response, catalog=OOV)
    assert list(parsed["raaaaaa1"].tags) == ["Space Tourism"]


def test_declination_keeps_its_explanation_and_silence_fails():
    declined = json.dumps(
        {
            "groundings": [
                {"record_id": "raaaaaa1", "options": [], "explanation": "nothing here"}
            ]
        }
    )
    parsed = parse_record_grounding_result(
        declined, catalog=IN_VOCAB, allowed_labels=VOCAB_LABELS
    )
    assert parsed["raaaaaa1"].tags == {}
    assert parsed["raaaaaa1"].explanation == "nothing here"

    silent = json.dumps(
        {"groundings": [{"record_id": "raaaaaa1", "options": [], "explanation": None}]}
    )
    with pytest.raises(AppliedRuleValidationError, match="must say why"):
        parse_record_grounding_result(
            silent, catalog=IN_VOCAB, allowed_labels=VOCAB_LABELS
        )


def test_an_explanation_volunteered_beside_units_is_dropped_not_fatal():
    response = json.dumps(
        {
            "groundings": [
                {
                    "record_id": "raaaaaa1",
                    "options": [_option("Machining")],
                    "explanation": "extra words",
                }
            ]
        }
    )
    parsed = parse_record_grounding_result(
        response, catalog=IN_VOCAB, allowed_labels=VOCAB_LABELS
    )
    assert parsed["raaaaaa1"].explanation is None


def test_duplicate_records_and_duplicate_labels_raise():
    entry = {
        "record_id": "raaaaaa1",
        "options": [_option("Machining")],
        "explanation": None,
    }
    with pytest.raises(ValueError, match="Duplicate record id"):
        parse_record_grounding_result(
            json.dumps({"groundings": [entry, entry]}),
            catalog=IN_VOCAB,
            allowed_labels=VOCAB_LABELS,
        )

    doubled_label = json.dumps(
        {
            "groundings": [
                {
                    "record_id": "raaaaaa1",
                    "options": [_option("Machining"), _option("machining")],
                    "explanation": None,
                }
            ]
        }
    )
    with pytest.raises(ValueError, match="twice"):
        parse_record_grounding_result(
            doubled_label, catalog=IN_VOCAB, allowed_labels=VOCAB_LABELS
        )


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
