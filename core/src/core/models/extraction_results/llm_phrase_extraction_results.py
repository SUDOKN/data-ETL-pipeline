from pydantic import BaseModel
from datetime import datetime


from core.models.field_types import (
    OntologyVersionIDType,
    S3FileVersionIDType,
    LLMSearchResults,
    LLMPhraseRelationshipResults,
    LLMScreeningResults,
)
from core.models.llm_model import LLM_Model
from data_etl_app.models.chunking_strat import ChunkingStrategy
from open_ai_key_app.models.gpt_model_params import GPTModelParams


class BaseExtractionMetadata(BaseModel):
    created_at: datetime
    chunk_strat: ChunkingStrategy
    ontology_version_id: OntologyVersionIDType


class ExtractionNodeMetadata(BaseModel):
    llm_model: LLM_Model
    model_params: GPTModelParams
    prompt_name: str
    prompt_version_id: S3FileVersionIDType
    created_at: datetime


class RecursiveSearchNodeMetadata(ExtractionNodeMetadata):
    # Hard cap on the number of recursive search rounds (beyond the first search).
    # Recursion also stops early when a round yields no new phrases.
    max_rounds: int


class LLMPhraseExtractionMetadata(BaseExtractionMetadata):
    llm_phrase_search: ExtractionNodeMetadata
    llm_phrase_recursive_search: RecursiveSearchNodeMetadata
    llm_phrase_relationship: ExtractionNodeMetadata
    llm_phrase_relationship_screening: ExtractionNodeMetadata


class LLMPhraseExtractionStats(BaseModel):
    llm_phrase_search: LLMSearchResults
    llm_phrase_relationship: LLMPhraseRelationshipResults
    llm_phrase_screening: LLMScreeningResults
