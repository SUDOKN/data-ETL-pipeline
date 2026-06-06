from beanie import Document
from datetime import datetime
from pydantic import BaseModel, Field

from core.models.field_types import (
    HumanEvidenceResults,
    RawLLMMappingResult,
    LLMSearchResults,
    MfgETLDType,
    S3FileVersionIDType,
)
from core.models.concept_extraction_results import (
    ConceptExtractionMetadata,
    ConceptExtractionStats,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum, GroundTruthSource

from core.utils.time_util import get_current_time

YES_PREFIX = "Yes, "
NO_PREFIX = "No, "
CORRECT_PREFIX = "Correct, "
INCORRECT_PREFIX = "Incorrect, "


class EvidenceResultCorrection(BaseModel):
    upsert: HumanEvidenceResults


class LLMSearchResultsCorrection(BaseModel):
    upsert: LLMSearchResults


class MappingResultCorrection(BaseModel):
    upsert: RawLLMMappingResult

    # constructor from original LLM mapping result
    @classmethod
    def from_raw_llm_mapping_result(
        cls, original_mapping_result: RawLLMMappingResult
    ) -> "MappingResultCorrection":
        prefixed_mapping_result: RawLLMMappingResult = original_mapping_result.copy()
        for _mu, mk_dict in prefixed_mapping_result.items():
            for mk in mk_dict:
                mk_dict[mk] = f"{CORRECT_PREFIX}{mk_dict[mk]}"
        return cls(upsert=prefixed_mapping_result)


class HumanConceptCorrection(BaseModel):
    author_email: str
    source: GroundTruthSource
    llm_search_correction: LLMSearchResultsCorrection
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

    # chunk identifiers
    chunk_bounds: str
    chunk_text: str
    chunk_no: int  # used as navigation parameter
    last_chunk_no: int  # informational for the end user

    # following is a copy of what was extracted at the time of creating this ground truth
    # stored originally in the linked manufacturer
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
