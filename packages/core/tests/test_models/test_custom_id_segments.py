from datetime import datetime

from llm_providers.models.llm_model import GPT_4o_mini
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    BatchedRelationshipNodeMetadata,
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

    assert relationship.to_custom_id_segment().endswith("|gs=50")
    assert screening.to_custom_id_segment().endswith("|gs=15")
    assert grounding.to_custom_id_segment().endswith("|gs=15")


def test_changing_the_cap_changes_request_identity():
    """The point of carrying the cap: batch request docs are keyed by custom_id and
    outlive a deferred-document wipe, so without this a re-run at a new cap would
    find the old ids complete and replay answers to a differently sliced question."""
    fifty = BatchedRelationshipNodeMetadata(**_COMMON, max_phrases_per_request=50)
    twenty_five = BatchedRelationshipNodeMetadata(**_COMMON, max_phrases_per_request=25)
    assert fifty.to_custom_id_segment() != twenty_five.to_custom_id_segment()
