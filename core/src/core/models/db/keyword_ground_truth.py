from beanie import Document
from datetime import datetime
from pydantic import BaseModel, ValidationInfo, computed_field, Field, field_validator

from core.utils.time_util import get_current_time
from core.models.field_types import MfgETLDType, S3FileVersionIDType
from core.models.search_stage_results import SearchStageMetadata
from core.models.keyword_extraction_results import KeywordExtractionStats
from data_etl_app.models.types_and_enums import GroundTruthSource, KeywordTypeEnum


class KeywordResultCorrection(BaseModel):
    add: list[str]
    remove: list[str]


class HumanKeywordCorrection(BaseModel):
    author_email: str
    source: GroundTruthSource
    llm_search: KeywordResultCorrection


class KeywordCorrectionLog(BaseModel):
    created_at: datetime  # must be set beforehand, no default provided on purpose
    human_correction: HumanKeywordCorrection


class KeywordGroundTruth(Document):
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())

    mfg_etld1: MfgETLDType
    scraped_text_file_version_id: S3FileVersionIDType
    keyword_type: KeywordTypeEnum

    # context ids
    chunk_bounds: str
    chunk_no: int
    last_chunk_no: int

    chunk_text: str
    metadata: SearchStageMetadata
    extraction_stats: KeywordExtractionStats

    corrections: list[KeywordCorrectionLog]

    @field_validator("chunk_no")
    def check_chunk_no(cls, v, values: ValidationInfo):
        last_chunk_no = values.data["last_chunk_no"]
        if last_chunk_no is None:
            raise ValueError("last_chunk_no must be set before validating chunk_no.")
        if v < 1 or v > last_chunk_no:
            raise ValueError("chunk_no must be between 1 and last_chunk_no.")
        return v

    @computed_field
    @property
    def final_results(self) -> list[str] | None:
        from data_etl_app.utils.ground_truth_helper_util import (
            calculate_final_keyword_results,
        )

        return calculate_final_keyword_results(self)

    class Settings:
        name = "keyword_ground_truths"


"""
Indexes for KeywordGroundTruth

db.keyword_ground_truths.createIndex(
  {
    mfg_etld1: 1,
    scraped_text_file_version_id: 1,
    extract_prompt_version_id: 1,
    keyword_type: 1,
    chunk_no: 1
  },
  {
    name: "keyword_gt_unique_idx",
    unique: true,
  }
);

"""
