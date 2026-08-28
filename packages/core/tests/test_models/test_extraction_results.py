"""The stored stats and metadata shapes of the phrase pipeline: record-keyed
stage fields, uniform ConceptsFound, and the optional OOV grounding node.

Introduced as phase 1.3 of PIPELINE_V2_PLAN.md, which is why the notes below
still speak of v1 and v2 — the shapes were a departure FROM something, and that
is worth remembering. The ``V2`` suffix the classes wore is not: it went on
2026-08-27, once nothing on the other side of the migration survived."""

from core.models.extraction_results.concept_extraction_results import ConceptsFound
from core.models.extraction_results.llm_phrase_extraction_results import (
    ConceptExtractionStats,
    InitialGroundingStats,
    KeywordExtractionStats,
)
from core.models.extraction_schemas.relationship import (
    MaskedPhraseRelationshipRecord,
    PhraseMention,
    PhraseRelationshipRecord,
)
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.grounding import RecordGroundingEntry
from core.models.extraction_schemas.screening import CandidateScreeningVerdict
from core.models.extraction_schemas.stored_fold import (
    StoredBundle,
    StoredFold,
    StoredMention,
    StoredWindowFold,
)
from core.utils.record_id_util import record_id_for_phrase

_RID = record_id_for_phrase("aerospace")

# The persisted fold every chunk of stats now carries (2026-08-27). Minimal
# but not empty: a stats object whose fold has no mentions would not exercise
# the offsets, which are the part that has to survive a JSON round trip.
def _stored_fold() -> StoredFold:
    return StoredFold(
        text_version_id="v1",
        normalizer_version="n1",
        verb_fold=False,
        snippet_radius=0,
        collapse_compounds=False,
        bundles=[
            StoredBundle(
                group_id="g1",
                key="aerospace",
                forms=["Aerospace"],
                mentions=[
                    StoredMention(
                        window_index=0,
                        start=10,
                        end=19,
                        snippet_start=0,
                        snippet_end=40,
                        page="/",
                        sent_form="aerospace",
                        location="home page",
                        location_source="llm",
                    )
                ],
            )
        ],
        windows=[
            StoredWindowFold(
                window_index=0,
                window_id="0:40",
                base_offset=0,
                text_length=40,
                sent_forms=["aerospace"],
            )
        ],
    )


_MASKED_ROUND = {
    _RID: MaskedPhraseRelationshipRecord(
        phrase="aerospace",
        record=PhraseRelationshipRecord(
            mentions=[PhraseMention(form="Aerospace", page="/", account="a")],
            synthesis="s",
        ),
    )
}

_SCREENING_ROUND = {
    _RID: {
        "Aerospace Industry": CandidateScreeningVerdict(
            passed=True,
            applied_rules=[
                AppliedRule(rule_id="SCR-1", outcome="satisfied", explanation="e"),
                AppliedRule(rule_id="SCR-2", outcome="satisfied", explanation="e"),
            ],
        )
    }
}


def _concept_stats() -> ConceptExtractionStats:
    return ConceptExtractionStats(
        results=ConceptsFound(
            in_vocab={"Aerospace Industry"}, out_of_vocab={"Space Tourism"}
        ),
        brute_search=set(),
        aggregation_fold=_stored_fold(),
        llm_phrase_search={0: set(), 1: {"aerospace"}},
        llm_phrase_relationship={1: _MASKED_ROUND},
        llm_phrase_screening={1: _SCREENING_ROUND},
        llm_phrase_initial_grounding=InitialGroundingStats(
            in_vocab={1: {_RID: RecordGroundingEntry(tags={"Aerospace Industry": []})}},
            out_of_vocab={1: {_RID: RecordGroundingEntry(tags={"Space Tourism": []})}},
        ),
        llm_phrase_recursive_grounding={},
    )


def test_concept_stats_round_trip_through_json():
    stats = _concept_stats()
    reloaded = ConceptExtractionStats.model_validate_json(
        stats.model_dump_json()
    )
    assert reloaded == stats
    entry = reloaded.llm_phrase_relationship[1][_RID]
    assert entry.phrase == "aerospace"
    assert reloaded.llm_phrase_screening[1][_RID]["Aerospace Industry"].passed


def test_keyword_stats_take_the_uniform_shape_with_empty_in_vocab():
    stats = KeywordExtractionStats(
        results=ConceptsFound(in_vocab=set(), out_of_vocab={"CNC machining centers"}),
        aggregation_fold=_stored_fold(),
        llm_phrase_search={0: set(), 1: {"cnc machines"}},
        llm_phrase_relationship={1: {}},
        llm_phrase_screening={1: {}},
        llm_phrase_freehand_grounding={
            1: {_RID: RecordGroundingEntry(tags={"CNC machining centers": []})}
        },
    )
    reloaded = KeywordExtractionStats.model_validate_json(stats.model_dump_json())
    assert reloaded.results.in_vocab == set()
    assert reloaded.results.out_of_vocab == {"CNC machining centers"}


def test_oov_grounding_node_is_optional_in_concept_metadata():
    """None = the OOV pass is off for the run — run config as metadata identity
    (fork F6), never a StageToggle."""
    from core.models.extraction_results.llm_phrase_extraction_results import (
        ConceptExtractionMetadata,
    )

    fields = ConceptExtractionMetadata.model_fields
    assert fields["llm_phrase_oov_grounding"].is_required() is False
    assert fields["llm_phrase_initial_grounding"].is_required() is True


def test_search_round_values_accept_the_v1_search_results_shape():
    """The search stage is untouched by v2: its round map must keep loading
    whatever LLMSearchResults holds today."""
    stats = _concept_stats()
    assert set(stats.llm_phrase_search[1]) == {"aerospace"}
