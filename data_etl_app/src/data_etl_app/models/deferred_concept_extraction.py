from pydantic import BaseModel

from core.models.field_types import OntologyVersionIDType, S3FileVersionIDType
from open_ai_key_app.models.gpt_batch_request import GPTBatchRequest


class ConceptSearchBatchRequest(BaseModel):
    brute: set[str]
    llm: GPTBatchRequest
    mapping: (
        GPTBatchRequest | None
    )  # because this will be executed in a different batch


class DeferredConceptExtractionStats(BaseModel):
    extract_prompt_version_id: S3FileVersionIDType
    map_prompt_version_id: S3FileVersionIDType
    ontology_version_id: OntologyVersionIDType
    chunked_stats_batch_request_map: dict[str, ConceptSearchBatchRequest]


class DeferredConceptExtraction(BaseModel):
    deferred_stats: DeferredConceptExtractionStats
