from pydantic import BaseModel

from core.models.field_types import (
    OntologyVersionIDType,
    S3FileVersionIDType,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID


class ConceptExtractionBundle(BaseModel):
    brute: set[str]
    llm_search_request_id: GPTBatchRequestCustomID


class DeferredConceptExtraction(BaseModel):
    extract_prompt_version_id: S3FileVersionIDType
    map_prompt_version_id: S3FileVersionIDType
    ontology_version_id: OntologyVersionIDType
    chunk_request_bundle_map: dict[str, ConceptExtractionBundle]
    llm_mapping_request_id: (
        GPTBatchRequestCustomID | None
    )  # because this will be executed in a different batch
