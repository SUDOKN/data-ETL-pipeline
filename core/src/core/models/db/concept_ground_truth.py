from beanie import Document
from datetime import datetime
from data_etl_app.models.chunking_strat import ChunkingStrategy
from pydantic import BaseModel, Field

from core.models.field_types import (
    HumanEvidenceResults,
    LLMMappingType,
    MfgETLDType,
    S3FileVersionIDType,
)
from core.models.concept_extraction_results import (
    ConceptExtractionMetadata,
    ConceptExtractionStats,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum, GroundTruthSource

from core.utils.time_util import get_current_time


class EvidenceResultCorrection(BaseModel):
    upsert: HumanEvidenceResults
    reject: list[str]


class MappingResultCorrection(BaseModel):
    upsert: LLMMappingType
    remove: list[str]


class HumanConceptCorrection(BaseModel):
    author_email: str
    source: GroundTruthSource
    llm_evidence_correction: EvidenceResultCorrection
    llm_mapping_correction: MappingResultCorrection


class ConceptCorrectionLog(BaseModel):
    created_at: datetime  # must be set beforehand, no default provided on purpose
    human_correction: HumanConceptCorrection


class ConceptGroundTruth(Document):
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())

    mfg_etld1: MfgETLDType
    scraped_text_file_version_id: S3FileVersionIDType
    concept_type: ConceptTypeEnum

    # context ids
    chunk_bounds: str
    chunk_no: int  # used as navigation parameter
    last_chunk_no: int  # informational for the end user
    chunk_text: str
    chunk_strat: ChunkingStrategy

    metadata: ConceptExtractionMetadata
    extraction_stats: ConceptExtractionStats

    corrections: list[ConceptCorrectionLog]

    class Settings:
        name = "concept_ground_truths"


"""
Indexes in MongoDB for ConceptGroundTruth:

db.concept_ground_truths.createIndex(
  {
    mfg_etld1: 1,
    concept_type: 1,
    scraped_text_file_version_id: 1,
    chunk_bounds: 1,
  },
  { 
    name: "concept_gt_unique_idx",
    unique: true 
  }
)
"""
