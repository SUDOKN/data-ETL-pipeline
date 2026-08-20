"""Catalog-driven template inflation, branch by branch.

A synthetic catalog covers every silence/report/failure branch; the REAL
catalogs (loaded from the app's JSON files as data, not as an import) prove the
round-trip: inflating a stored verdict must construct a ScreeningLLMCopy whose
own passed-cache validator agrees with ``passed_implied_by``, and survive
serialization.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from core.models.extraction_results.keyword_extraction_results import (
    KeywordExtractionStats,
)
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.iterative_tagging import (
    IterativelyTaggedPhraseGroup,
)
from core.models.extraction_schemas.screening import ScreeningVerdict
from core.models.ground_truth.stage_blocks import ScreeningLLMCopy
from core.models.rule_catalog import RuleCatalog
from core.services.applied_rule_validation import passed_implied_by
from core.services.ground_truth.catalog_template_inflation import (
    UNREPORTED_GUARD_OUTCOME,
    InflationError,
    inflate_applied_rules,
    inflate_chunk,
    inflate_in_vocab_grounding,
    inflate_screening_llm_copy,
    inflate_tag_groundings,
    split_sentinel_tag_rules,
)

_CATALOG_DIR = (
    Path(__file__).parents[4]
    / "apps/data_etl_app/src/data_etl_app/knowledge/prompts/rule_catalog"
)


def _real_catalog(name: str) -> RuleCatalog:
    return RuleCatalog.model_validate(
        json.loads((_CATALOG_DIR / f"{name}.json").read_text())
    )


def _synthetic_catalog(
    prompt_name: str = "synthetic_screening",
    stage: str = "phrase_relationship_screening",
    id_prefix: str = "SCR",
) -> RuleCatalog:
    """Every kind and every report_when in one small catalog."""
    return RuleCatalog.model_validate(
        {
            "catalog_version": f"{prompt_name}.1",
            "prompt_name": prompt_name,
            "stage": stage,
            "field_types": ["equipments"],
            "entity_noun": "entity",
            "entity_relationships": {
                "base": "make",
                "third_person": "makes",
                "gerund": "making",
            },
            "outcome_vocab": {
                "condition": ["satisfied", "failed", "not_triggered"],
                "guard": ["violated"],
                "preference": ["chosen"],
            },
            "sections": [
                {
                    "section_id": "conditions",
                    "heading": "Conditions",
                    "combinator": "all",
                    "rules": [
                        {
                            "id": f"{id_prefix}-1",
                            "kind": "condition",
                            "reportable": True,
                            "report_when": "always",
                            "text": "the entity is theirs",
                            "children": [
                                {
                                    "id": f"{id_prefix}-1-note",
                                    "kind": "note",
                                    "reportable": False,
                                    "report_when": "never",
                                    "text": "judge by use, not mention",
                                }
                            ],
                        }
                    ],
                },
                {
                    "section_id": "guards",
                    "heading": "Guards",
                    "combinator": "any",
                    "rules": [
                        {
                            "id": f"{id_prefix}-G1",
                            "kind": "guard",
                            "reportable": True,
                            "report_when": "on_violation",
                            "text": "not the customer's process",
                        }
                    ],
                },
                {
                    "section_id": "matching",
                    "heading": "Matching",
                    "combinator": "ordered",
                    "rules": [
                        {
                            "id": f"{id_prefix}-M1",
                            "kind": "preference",
                            "reportable": True,
                            "report_when": "when_chosen",
                            "text": "exact label",
                        },
                        {
                            "id": f"{id_prefix}-M2",
                            "kind": "preference",
                            "reportable": True,
                            "report_when": "when_chosen",
                            "text": "generalize",
                        },
                    ],
                },
            ],
            "published": {},
        }
    )


_CATALOG = _synthetic_catalog()


def _row(rule_id: str, outcome: str = "satisfied") -> AppliedRule:
    return AppliedRule(rule_id=rule_id, outcome=outcome, explanation="because")


def _rule_index(sections):
    return {
        rule.rule_id: rule
        for section in sections
        for top in section.applied_rules
        for rule in [top, *top.sub_rules]
    }


# --- the primitive ---------------------------------------------------------


def test_reported_rows_hydrate_and_silence_materializes():
    sections = inflate_applied_rules(
        _CATALOG, [_row("SCR-1"), _row("SCR-M1", "chosen")]
    )
    rules = _rule_index(sections)

    assert rules["SCR-1"].reported and rules["SCR-1"].outcome == "satisfied"
    assert rules["SCR-M1"].reported and rules["SCR-M1"].outcome == "chosen"
    # Guard silence is a verdict; preference silence is context.
    assert not rules["SCR-G1"].reported
    assert rules["SCR-G1"].outcome == UNREPORTED_GUARD_OUTCOME
    assert not rules["SCR-M2"].reported and rules["SCR-M2"].outcome is None
    # The note child rides along as context under its parent.
    assert not rules["SCR-1-note"].reported and rules["SCR-1-note"].outcome is None


def test_sections_mirror_the_catalog():
    sections = inflate_applied_rules(_CATALOG, [_row("SCR-1")])
    assert [(s.section_id, s.combinator) for s in sections] == [
        ("conditions", "all"),
        ("guards", "any"),
        ("matching", "ordered"),
    ]


def test_zero_rows_yield_no_sections_unless_asked():
    assert inflate_applied_rules(_CATALOG, []) == []
    skeleton = inflate_applied_rules(_CATALOG, [], synthesize_all_on_empty=True)
    assert all(
        not rule.reported for rule in _rule_index(skeleton).values()
    ) and len(skeleton) == 3


def test_unknown_rule_id_is_refused():
    with pytest.raises(InflationError, match="does not exist at catalog_version"):
        inflate_applied_rules(_CATALOG, [_row("SCR-1"), _row("OTHER-9")])


def test_missing_always_rule_in_nonempty_report_is_refused():
    with pytest.raises(InflationError, match="always-reported rule 'SCR-1'"):
        inflate_applied_rules(_CATALOG, [_row("SCR-M1", "chosen")])


def test_outcome_outside_the_rules_vocabulary_is_refused():
    with pytest.raises(InflationError, match="not one of"):
        inflate_applied_rules(_CATALOG, [_row("SCR-1", "violated")])


def test_conflicting_duplicate_rows_are_refused_identical_ones_deduped():
    with pytest.raises(InflationError, match="refusing to pick a winner"):
        inflate_applied_rules(_CATALOG, [_row("SCR-1"), _row("SCR-1", "failed")])
    sections = inflate_applied_rules(_CATALOG, [_row("SCR-1"), _row("SCR-1")])
    assert _rule_index(sections)["SCR-1"].reported


# --- screening -------------------------------------------------------------


def test_no_candidate_verdict_inflates_the_full_silent_skeleton():
    copy = inflate_screening_llm_copy(
        _CATALOG,
        ScreeningVerdict(
            passed=False,
            identified_entity=None,
            applied_rules=[],
            no_candidate_explanation="nothing named",
        ),
    )
    assert not copy.passed and len(copy.sections) == 3
    assert all(not rule.reported for rule in _rule_index(copy.sections).values())


def test_verdict_whose_passed_disagrees_with_its_rules_cannot_inflate():
    with pytest.raises(ValidationError, match="cache of the fold"):
        inflate_screening_llm_copy(
            _CATALOG,
            ScreeningVerdict(
                passed=True,
                identified_entity="mill",
                applied_rules=[_row("SCR-1", "failed")],
            ),
        )


# --- grounding -------------------------------------------------------------


def test_tag_with_no_rules_is_corruption():
    with pytest.raises(InflationError, match="has no rules"):
        inflate_tag_groundings(_CATALOG, {"Milling": []})


def test_sentinel_keys_split_out_as_declination_not_corruption():
    """The escape hatch is stored exactly like a tag — a sentinel key with no
    rules — but it is a declination record, not a grounding link."""
    real, declined = split_sentinel_tag_rules(
        {"None of the above": [], "Milling": [_row("SCR-1")]}
    )
    assert declined is True
    assert list(real) == ["Milling"]


def test_sentinel_recognition_is_case_insensitive():
    real, declined = split_sentinel_tag_rules({"NONE OF THE ABOVE": []})
    assert declined is True and real == {}


def test_sentinel_with_rules_still_declines_and_warns(caplog):
    with caplog.at_level("WARNING"):
        real, declined = split_sentinel_tag_rules(
            {"Cannot categorize": [_row("SCR-1")]}
        )
    assert declined is True and real == {}
    assert any("declination" in record.message for record in caplog.records)


_RGR = _synthetic_catalog(
    "synthetic_recursive", "phrase_recursive_grounding", "RGR"
)
_IGR = _synthetic_catalog("synthetic_initial", "phrase_initial_grounding", "IGR")


def _descent_result():
    return {
        1: {
            IterativelyTaggedPhraseGroup(
                parent_group_id=None,
                group_id="Aerospace",
                direct_phrases_to_og_tag_w_rules={
                    # Mixed provenance on one node: an IGR row next to an RGR row.
                    "wing spars": {
                        "Aerospace": [_row("IGR-1"), _row("RGR-1")]
                    }
                },
                iterative_phrases_to_og_tag_w_rules={},
            )
        },
        2: {
            IterativelyTaggedPhraseGroup(
                parent_group_id="Aerospace",
                group_id="Aircraft Parts",
                stop_reason="sentinel",
                direct_phrases_to_og_tag_w_rules={},
                iterative_phrases_to_og_tag_w_rules={"wing spars": {}},
            )
        },
    }


def test_mixed_provenance_node_gets_sections_from_both_catalogs():
    by_phrase = inflate_in_vocab_grounding(
        _descent_result(), recursive_catalog=_RGR, initial_catalog=_IGR
    )
    node = by_phrase["wing spars"].levels[1][0]
    rules = _rule_index(node.sections)
    assert rules["RGR-1"].reported and rules["IGR-1"].reported
    # Each partition synthesizes its own catalog's silence.
    assert not rules["RGR-G1"].reported and not rules["IGR-G1"].reported


def test_sentinel_node_with_no_rules_has_no_sections():
    by_phrase = inflate_in_vocab_grounding(
        _descent_result(), recursive_catalog=_RGR, initial_catalog=_IGR
    )
    stopped = by_phrase["wing spars"].levels[2][0]
    assert stopped.stop_reason == "sentinel" and stopped.sections == []


def test_row_unknown_to_every_offered_catalog_is_refused():
    result = {
        1: {
            IterativelyTaggedPhraseGroup(
                parent_group_id=None,
                group_id="Aerospace",
                direct_phrases_to_og_tag_w_rules={"x": {"Aerospace": [_row("ZZZ-1")]}},
                iterative_phrases_to_og_tag_w_rules={},
            )
        }
    }
    with pytest.raises(InflationError, match="unknown to every catalog"):
        inflate_in_vocab_grounding(
            result, recursive_catalog=_RGR, initial_catalog=_IGR
        )


# --- chunk assembly --------------------------------------------------------


def _keyword_stats(**overrides) -> KeywordExtractionStats:
    fields: dict[str, Any] = dict(
        results={"cnc mill"},
        llm_phrase_search={0: set(), 1: {"cnc mill"}},
        llm_phrase_relationship={0: {}, 1: {"cnc mill": "a machine they run"}},
        llm_phrase_screening={
            0: {},
            1: {
                "cnc mill": ScreeningVerdict(
                    passed=True,
                    identified_entity="cnc mill",
                    applied_rules=[_row("SCR-1")],
                )
            },
        },
        llm_phrase_freehand_grounding={
            0: {},
            1: {"cnc mill": {"CNC Milling Machine": [_row("SCR-1")]}},
        },
    )
    fields.update(overrides)
    return KeywordExtractionStats(**fields)


def test_chunk_inflates_phrase_major_with_rounds():
    chunk = inflate_chunk(
        _keyword_stats(), screening_catalog=_CATALOG, oov_catalog=_CATALOG
    )
    phrase = chunk.extracted_phrases["cnc mill"]
    assert phrase.search_round == 1
    assert phrase.llm_screening is not None and phrase.oov_grounding is not None
    assert phrase.llm_relationship.llm_result == "a machine they run"
    assert phrase.llm_screening.llm_result.passed is True
    assert "CNC Milling Machine" in phrase.oov_grounding.tags
    assert phrase.in_vocab_grounding is None
    assert chunk.missed_phrases == []


def test_phrase_without_a_screening_verdict_gets_none_not_a_fake():
    stats = _keyword_stats(
        llm_phrase_screening={0: {}, 1: {}},
        llm_phrase_freehand_grounding={0: {}, 1: {}},
    )
    chunk = inflate_chunk(
        stats, screening_catalog=_CATALOG, oov_catalog=_CATALOG
    )
    phrase = chunk.extracted_phrases["cnc mill"]
    assert phrase.llm_screening is None and phrase.oov_grounding is None


def test_declined_grounding_inflates_a_flagged_empty_block():
    """The live shape that broke the first P3-gate template build: the model
    answered the sentinel, stored as a tag key with zero rules."""
    stats = _keyword_stats(
        llm_phrase_freehand_grounding={
            0: {},
            1: {"cnc mill": {"None of the above": []}},
        },
    )
    chunk = inflate_chunk(
        stats, screening_catalog=_CATALOG, oov_catalog=_CATALOG
    )
    block = chunk.extracted_phrases["cnc mill"].oov_grounding
    assert block is not None
    assert block.tags == {} and block.llm_declined is True


def test_sentinel_beside_a_real_tag_keeps_the_tag_and_declines():
    stats = _keyword_stats(
        llm_phrase_freehand_grounding={
            0: {},
            1: {
                "cnc mill": {
                    "CNC Milling Machine": [_row("SCR-1")],
                    "None of the above": [],
                }
            },
        },
    )
    chunk = inflate_chunk(
        stats, screening_catalog=_CATALOG, oov_catalog=_CATALOG
    )
    block = chunk.extracted_phrases["cnc mill"].oov_grounding
    assert block is not None and block.llm_declined is True
    assert list(block.tags) == ["CNC Milling Machine"]


def test_orphan_grounding_phrase_is_warned_and_skipped(caplog):
    stats = _keyword_stats(
        llm_phrase_freehand_grounding={
            0: {},
            1: {
                "cnc mill": {"CNC Milling Machine": [_row("SCR-1")]},
                "ghost": {"Ghost Tag": [_row("SCR-1")]},
            },
        },
    )
    with caplog.at_level("WARNING"):
        chunk = inflate_chunk(
            stats, screening_catalog=_CATALOG, oov_catalog=_CATALOG
        )
    assert "ghost" not in chunk.extracted_phrases
    assert any("never" in record.message for record in caplog.records)


# --- the real catalogs -----------------------------------------------------


def _judged_verdict(catalog: RuleCatalog) -> ScreeningVerdict:
    """A verdict shaped exactly as a validated wire response would store it."""
    rows = [
        AppliedRule(
            rule_id=rule.id,
            outcome=(
                "satisfied"
                if rule.kind == "condition"
                else catalog.outcome_vocab[rule.kind][0]
            ),
            explanation="cited from the relationship summary",
        )
        for rule in catalog.walk_rules()
        if rule.report_when == "always"
    ]
    return ScreeningVerdict(
        passed=passed_implied_by(catalog, rows),
        identified_entity="cnc mill",
        applied_rules=rows,
    )


@pytest.mark.parametrize(
    "name",
    [
        "equipment_phrase_relationship_screening",
        "industry_phrase_relationship_screening",
    ],
)
def test_real_screening_catalog_round_trips(name):
    catalog = _real_catalog(name)
    copy = inflate_screening_llm_copy(catalog, _judged_verdict(catalog))
    assert copy.passed is True  # all conditions satisfied, no guards fired
    dumped = copy.model_dump(mode="json")
    assert ScreeningLLMCopy.model_validate(dumped) == copy


@pytest.mark.parametrize(
    "name",
    ["equipment_phrase_freehand_grounding", "industry_phrase_initial_grounding"],
)
def test_real_grounding_catalog_inflates_a_stored_tag_map(name):
    catalog = _real_catalog(name)
    rows = _judged_verdict(catalog).applied_rules
    chosen = next(
        (r for r in catalog.walk_rules() if r.report_when == "when_chosen"), None
    )
    if chosen is not None:
        rows = [*rows, AppliedRule(rule_id=chosen.id, outcome="chosen", explanation="best fit")]
    inflated = inflate_tag_groundings(catalog, {"Some Tag": rows})
    sections = inflated["Some Tag"].llm_result
    assert sections is not None
    assert [s.section_id for s in sections] == [
        s.section_id for s in catalog.sections
    ]
    reported = [r for r in _rule_index(sections).values() if r.reported]
    assert len(reported) == len(rows)
