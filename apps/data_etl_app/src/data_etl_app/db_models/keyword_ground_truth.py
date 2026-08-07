from beanie import Document
from datetime import datetime
from core.models.chunking_strat import ChunkingStrategy
from pydantic import BaseModel, ValidationInfo, computed_field, Field, field_validator

from pure_utils.time_util import get_current_time
from infra.field_types import (
    S3FileVersionIDType,
)
from core.field_types import (
    SubjectUniqueIDType,
)
from core.models.extraction_results.keyword_extraction_results import (
    KeywordExtractionMetadata,
    KeywordExtractionStats,
)
from data_etl_app.models.types_and_enums import (
    GroundTruthSource,
    KeywordTypeEnum,
)


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

    subject_unique_id: SubjectUniqueIDType
    scraped_text_file_version_id: S3FileVersionIDType
    keyword_type: KeywordTypeEnum

    # context ids
    chunk_bounds: str
    chunk_no: int
    last_chunk_no: int
    chunk_text: str

    metadata: KeywordExtractionMetadata
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
    subject_unique_id: 1,
    scraped_text_file_version_id: 1,
    "metadata.search_prompt_version_id": 1,
    keyword_type: 1,
    chunk_bounds: 1
  },
  {
    name: "keyword_gt_unique_idx",
    unique: true,
  }
);

"""
