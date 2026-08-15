"""The resume guard: a deferred field may only be resumed under the metadata it
was built with.

Extraction always runs on the latest resources — prompt bytes come from whatever
``PromptService`` pinned at startup, vocabulary from the latest ontology — while
request custom_ids are stamped from the metadata STORED on the deferred field.
These tests pin the rule that keeps those two from drifting apart.
"""

from datetime import datetime, timedelta

import pytest
from llm_providers.models.llm_model import GPT_4o_mini, GPT_4_1_mini
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

from core.models.chunking_strat import CONFORMITY_ATTESTATION_CHUNKING_STRAT
from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
    ConceptExtractionMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    BatchedRelationshipNodeMetadata,
    BatchedScreeningNodeMetadata,
    ExtractionNodeMetadata,
    RecursiveSearchNodeMetadata,
)
from core.models.pipeline_nodes.base.base_prefill_node import (
    PrefillNode,
    StaleExtractionMetadataError,
)

_PROCESS_START = datetime(2026, 8, 13, 19, 33, 13)
# A later run in a fresh process: `metadata_init_at = datetime.now(UTC)` moves,
# and it is stamped onto the envelope AND every per-stage metadata object.
_NEXT_PROCESS_START = _PROCESS_START + timedelta(hours=4)


class _FieldType:
    name = "conformity_attestation"

    def __hash__(self) -> int:
        return hash(self.name)


class _Prefill(PrefillNode):
    """Only ``field_type`` matters to the guard, so skip the node wiring."""

    def __init__(self) -> None:
        self.field_type = _FieldType()

    async def execute(self, *args, **kwargs) -> None: ...


def _metadata(
    *,
    metadata_init_at: datetime = _PROCESS_START,
    prompt_version_id: str = "J1U57U5GZLH1bJ.JnWV8ZnoZM9fmCGis",
    catalog_version: str | None = None,
    llm_model=GPT_4o_mini,
    ontology_version_id: str = "ontology-v1",
    screening_group_size: int = 15,
) -> dict:
    stage = dict(
        llm_model=llm_model,
        model_params=GPTModelParams.with_defaults(),
        prompt_name="conformity_attestation_phrase_search",
        prompt_version_id=prompt_version_id,
        catalog_version=catalog_version,
        created_at=metadata_init_at,
    )
    return ConceptExtractionMetadata(
        created_at=metadata_init_at,
        chunk_strat=CONFORMITY_ATTESTATION_CHUNKING_STRAT,
        ontology_version_id=ontology_version_id,
        llm_phrase_search=ExtractionNodeMetadata(**stage),
        llm_phrase_recursive_search=RecursiveSearchNodeMetadata(**stage, max_rounds=1),
        llm_phrase_relationship=BatchedRelationshipNodeMetadata(
            **stage, max_phrases_per_request=50
        ),
        llm_phrase_relationship_screening=BatchedScreeningNodeMetadata(
            **stage, max_pairs_per_request=screening_group_size
        ),
        llm_phrase_initial_grounding=BatchedInitialGroundingNodeMetadata(
            **stage, max_pairs_per_request=15
        ),
        llm_phrase_recursive_grounding=ExtractionNodeMetadata(**stage),
    ).model_dump()


def _check(stored: dict, latest: dict) -> None:
    _Prefill().raise_if_metadata_is_stale(
        subject_unique_id="steelcraft.com",
        stored_metadata_dump=stored,
        latest_metadata_dump=latest,
    )


def test_resuming_in_a_fresh_process_is_not_drift():
    """The whole config is unchanged; only the timestamps taken at process start
    moved. Before the depth-aware exclusion this raised on six per-stage
    ``created_at`` diffs, which would have made the rule reject every resumed
    subject on any restart."""
    _check(_metadata(), _metadata(metadata_init_at=_NEXT_PROCESS_START))


def test_republished_prompt_stops_the_resume():
    """The case the narrowed concept guard could not see: a new prompt version
    would otherwise be ignored entirely, because custom_ids are built from the
    stored pin and the old completed requests are replayed."""
    with pytest.raises(StaleExtractionMetadataError) as error:
        _check(_metadata(), _metadata(prompt_version_id="NEW-VERSION-ID"))

    message = str(error.value)
    assert "llm_phrase_search.prompt_version_id" in message
    assert "steelcraft.com.conformity_attestation" in message
    # The remedy has to be in the error: nulling the field is the only fix, and
    # it is done by hand outside the pipeline.
    assert "set deferred.conformity_attestation to null" in message


def test_changed_catalog_stops_the_resume():
    with pytest.raises(StaleExtractionMetadataError):
        _check(_metadata(catalog_version="v1"), _metadata(catalog_version="v2"))


def test_changed_model_stops_the_resume():
    with pytest.raises(StaleExtractionMetadataError):
        _check(_metadata(), _metadata(llm_model=GPT_4_1_mini))


def test_changed_ontology_version_stops_the_resume():
    with pytest.raises(StaleExtractionMetadataError):
        _check(_metadata(), _metadata(ontology_version_id="ontology-v2"))


def test_changed_group_size_stops_the_resume():
    """Group size decides which phrases share a request, so the same group index
    at a new cap is a different question — and it is in the custom_id."""
    with pytest.raises(StaleExtractionMetadataError):
        _check(_metadata(), _metadata(screening_group_size=25))
