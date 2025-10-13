from pydantic import BaseModel

from core.models.field_types import OntologyVersionIDType, S3FileVersionIDType
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID


class ConceptSearchBatchRequestBundle(BaseModel):
    brute: set[str]
    llm_batch_request_id: GPTBatchRequestCustomID
    mapping_batch_request_id: (
        GPTBatchRequestCustomID | None
    )  # because this will be executed in a different batch


class DeferredConceptExtractionStats(BaseModel):
    extract_prompt_version_id: S3FileVersionIDType
    map_prompt_version_id: S3FileVersionIDType
    ontology_version_id: OntologyVersionIDType
    chunked_stats_batch_request_map: dict[str, ConceptSearchBatchRequestBundle]


class DeferredConceptExtraction(BaseModel):
    deferred_stats: DeferredConceptExtractionStats
