from datetime import datetime

from llm_providers.models.llm_model import GPT_4o_mini
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
)
from core.models.extraction_results.keyword_extraction_results import (
    BatchedFreehandGroundingNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    BatchedMentionCollectionNodeMetadata,
    BatchedRelationshipNodeMetadata,
    BatchedSynthesisNodeMetadata,
    BatchedScreeningNodeMetadata,
    ExtractionNodeMetadata,
)

_COMMON = dict(
    llm_model=GPT_4o_mini,
    model_params=GPTModelParams.with_defaults(),
    prompt_name="some_prompt",
    prompt_version_id="s3-version-1",
    created_at=datetime(2026, 8, 11),
)


def test_unbatched_stage_carries_no_group_size():
    assert "|gs=" not in ExtractionNodeMetadata(**_COMMON).to_custom_id_segment()


def test_every_batched_stage_carries_its_group_size():
    relationship = BatchedRelationshipNodeMetadata(
        **_COMMON, max_phrases_per_request=50
    )
    screening = BatchedScreeningNodeMetadata(**_COMMON, max_pairs_per_request=15)
    grounding = BatchedInitialGroundingNodeMetadata(**_COMMON, max_pairs_per_request=15)
    freehand = BatchedFreehandGroundingNodeMetadata(
        **_COMMON, max_pairs_per_request=50
    )

    assert relationship.to_custom_id_segment().endswith("|gs=50")
    # v3: mentions (distinct snippets) per sub-window location request
    mention = BatchedMentionCollectionNodeMetadata(**_COMMON, max_mentions_per_request=50)
    assert mention.to_custom_id_segment().endswith("|gs=50")
    assert screening.to_custom_id_segment().endswith("|gs=15")
    assert grounding.to_custom_id_segment().endswith("|gs=15")
    assert freehand.to_custom_id_segment().endswith("|gs=50")


def test_changing_the_cap_changes_request_identity():
    """The point of carrying the cap: batch request docs are keyed by custom_id and
    outlive a deferred-document wipe, so without this a re-run at a new cap would
    find the old ids complete and replay answers to a differently sliced question."""
    fifty = BatchedRelationshipNodeMetadata(**_COMMON, max_phrases_per_request=50)
    twenty_five = BatchedRelationshipNodeMetadata(**_COMMON, max_phrases_per_request=25)
    assert fifty.to_custom_id_segment() != twenty_five.to_custom_id_segment()


def test_synthesis_carries_its_cap_and_its_location_arm():
    """v3 3.2: the soft entry cap AND the A/B arm are request identity — the two
    arms must coexist in Mongo, so `|loc=` is always present."""
    with_loc = BatchedSynthesisNodeMetadata(**_COMMON, max_entries_per_request=50, include_location=True)
    without = BatchedSynthesisNodeMetadata(**_COMMON, max_entries_per_request=50, include_location=False)
    assert with_loc.to_custom_id_segment().endswith("|gs=50|loc=1")
    assert without.to_custom_id_segment().endswith("|gs=50|loc=0")
    assert with_loc.to_custom_id_segment() != without.to_custom_id_segment()


def test_mention_snippet_radius_is_identity_only_when_set():
    """The radius knob was added after five runs: at 0 (the clip those runs
    used) the segment is unchanged so every stored id still matches; any other
    value is a different question."""
    legacy = BatchedMentionCollectionNodeMetadata(**_COMMON, max_mentions_per_request=50)
    zero = BatchedMentionCollectionNodeMetadata(**_COMMON, max_mentions_per_request=50, snippet_radius=0)
    two = BatchedMentionCollectionNodeMetadata(**_COMMON, max_mentions_per_request=50, snippet_radius=2)
    assert legacy.to_custom_id_segment() == zero.to_custom_id_segment()
    assert "|rad=" not in zero.to_custom_id_segment()
    assert two.to_custom_id_segment().endswith("|gs=50|rad=2")
