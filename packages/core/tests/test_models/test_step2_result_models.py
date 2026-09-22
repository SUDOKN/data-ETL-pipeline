"""Step 2 (2026-09-21): the additive result and metadata fields, and the
vocabulary-candidate document.

Three contracts, each of which a validator regeneration depends on:

1. A concept result stored BEFORE the fields existed loads unchanged — every
   new field is defaulted or Optional.
2. A result carrying the new blocks round-trips through the model, so the
   generated ``$jsonSchema`` (a superset) accepts both shapes.
3. ``VocabularyCandidate`` is a registered document with the collection name
   the leaf step writes to.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from core.db_models import DOCUMENT_MODELS
from core.db_models.vocabulary_candidates import CandidateEvidence, VocabularyCandidate
from core.models.extraction_results.extraction_node_metadata import (
    DescentNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    ConceptExtractionMetadata,
    ConceptExtractionStats,
    InitialGroundingStats,
)
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.grounding import RecordGroundingEntry
from core.models.extraction_schemas.screening import CandidateScreeningVerdict


def _old_shape_stats() -> dict:
    return {
        "llm_phrase_search": {0: []},
        "aggregation_fold": {"text_version_id": "t1", "normalizer_version": "n1", "verb_fold": False, "snippet_radius": 0, "collapse_compounds": False},
        "llm_phrase_screening": {},
        "results": {"in_vocab": [], "out_of_vocab": []},
        "brute_search": [],
        "llm_phrase_initial_grounding": {"in_vocab": {}, "out_of_vocab": {}},
        "llm_phrase_recursive_grounding": {},
    }


def test_pre_step2_concept_stats_load_with_defaults() -> None:
    stats = ConceptExtractionStats.model_validate(_old_shape_stats())
    assert stats.llm_phrase_grounding is None
    assert stats.llm_phrase_proposal == {}
    assert stats.llm_phrase_unit_screening == {}
    assert stats.llm_phrase_descent_screening == {}
    assert stats.llm_phrase_leaf_step == {}


def test_step2_blocks_round_trip() -> None:
    quote_rule = AppliedRule(rule_id="GR-E1", outcome="satisfied", explanation="the words quoted")
    entry = RecordGroundingEntry(tags={"Machining": [quote_rule]})
    verdict = CandidateScreeningVerdict(
        passed=True, applied_rules=[], evidence="named", failed_rule=None, quote="CNC machining"
    )
    stats = ConceptExtractionStats.model_validate(
        {
            **_old_shape_stats(),
            "llm_phrase_grounding": InitialGroundingStats(
                in_vocab={0: {"r1": entry}}, out_of_vocab={0: {}}
            ),
            "llm_phrase_proposal": {0: {"r2": RecordGroundingEntry(tags={}, explanation="no process named")}},
            "llm_phrase_unit_screening": {0: {"r1": {"Machining": verdict}}},
            "llm_phrase_descent_screening": {2: {"r1": {"CNC Machining": verdict}}},
            "llm_phrase_leaf_step": {"CNC Machining": {"r1": RecordGroundingEntry(tags={"5-axis CNC Machining": [quote_rule]})}},
        }
    )
    dumped = stats.model_dump(mode="json")
    again = ConceptExtractionStats.model_validate(dumped)
    assert again.llm_phrase_grounding is not None
    assert again.llm_phrase_grounding.in_vocab[0]["r1"].tags["Machining"][0].rule_id == "GR-E1"
    assert again.llm_phrase_unit_screening[0]["r1"]["Machining"].evidence == "named"
    assert again.llm_phrase_descent_screening[2]["r1"]["CNC Machining"].quote == "CNC machining"
    assert list(again.llm_phrase_leaf_step["CNC Machining"]["r1"].tags) == ["5-axis CNC Machining"]


def test_descent_metadata_carries_the_leaf_step_flag_outside_request_identity() -> None:
    def make(leaf_step: bool) -> DescentNodeMetadata:
        return DescentNodeMetadata.model_validate(
            {
                "llm_model": {"name": "gpt-4.1", "max_context_tokens": 1000},
                "model_params": {"temperature": 0, "top_p": 1, "presence_penalty": 0, "frequency_penalty": 0, "seed": 1, "max_completion_tokens": 10, "response_format": {"type": "json_object"}},
                "prompt_name": "process_cap_phrase_descent",
                "prompt_version_id": "v1",
                "created_at": "2026-09-21T00:00:00+00:00",
                "leaf_step": leaf_step,
            }
        )

    on, off = make(True), make(False)
    assert on.leaf_step and not off.leaf_step
    assert on.to_custom_id_segment() == off.to_custom_id_segment()
    assert on != off  # the prefill staleness rule sees the flag


def test_concept_metadata_new_stages_default_to_none() -> None:
    fields = ConceptExtractionMetadata.model_fields
    for name in ("llm_phrase_grounding", "llm_phrase_proposal", "llm_phrase_descent", "llm_phrase_unit_screening"):
        assert name in fields and fields[name].default is None, name


@pytest.fixture(autouse=True, scope="module")
def offline_candidate_settings():
    """Instantiating a Beanie Document needs only its ``_document_settings``
    (the synchronous half of ``init_beanie``) — the offline pattern the other
    document tests use."""
    from beanie.odm.settings.document import DocumentSettings

    settings_class = getattr(VocabularyCandidate, "Settings")  # noqa: B009
    settings_vars = {a: getattr(settings_class, a) for a in dir(settings_class) if not a.startswith("__")}
    VocabularyCandidate._document_settings = DocumentSettings(**settings_vars)


def test_vocabulary_candidate_is_a_registered_document() -> None:
    assert VocabularyCandidate in DOCUMENT_MODELS
    assert VocabularyCandidate.Settings.name == "vocabulary_candidates"
    doc = VocabularyCandidate(
        subject_unique_id="example.com",
        field_name="process_caps",
        run_timestamp=datetime(2026, 9, 21, tzinfo=timezone.utc),
        ontology_version_id="ov1",
        parent_label="CNC Machining",
        label="5-axis CNC Machining",
        records=[CandidateEvidence(record_id="r1", focal_form="5-axis machining", quote="5-axis machining of housings")],
        created_at=datetime(2026, 9, 21, tzinfo=timezone.utc),
    )
    assert doc.records[0].quote.startswith("5-axis")
