from beanie import Document
from datetime import datetime
from pydantic import BaseModel, ValidationInfo, computed_field, Field, field_validator

from shared.utils.time_util import get_current_time
from shared.models.field_types import (
    LLMMappingType,
    MfgETLDType,
    OntologyVersionIDType,
    S3FileVersionIDType,
)

from data_etl_app.models.concept_extraction_results import ConceptSearchChunkStats
from data_etl_app.models.types_and_enums import ConceptTypeEnum, GroundTruthSource


class ConceptResultCorrection(BaseModel):
    author_email: str
    add: LLMMappingType
    remove: list[str]
    source: GroundTruthSource


class ConceptResultCorrectionLog(BaseModel):
    """
    ResultCorrectionLog stores the history of corrections made to the concept extraction results.

    Attributes:
        created_at: When the correction was made.
        last_author_email: Email of the user who made the correction.
        result_correction: The correction details, including additions and removals.
    """

    created_at: datetime  # must be set beforehand, no default provided on purpose
    result_correction: ConceptResultCorrection


class ConceptGroundTruth(Document):
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())
    mfg_etld1: MfgETLDType

    concept_type: ConceptTypeEnum

    # ---- DECONSTRUCTED EXTRACTION STATS ---- #
    # knowledge identifiers
    scraped_text_file_version_id: str
    extract_prompt_version_id: S3FileVersionIDType
    map_prompt_version_id: S3FileVersionIDType
    ontology_version_id: OntologyVersionIDType

    # chunk identifiers, all need to match
    chunk_bounds: str
    last_chunk_no: int
    chunk_no: int
    chunk_extracted_at: datetime

    chunk_text: str
    chunk_search_stats: ConceptSearchChunkStats
    # ---------------------------------------- #

    correction_logs: list[ConceptResultCorrectionLog]

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
            calculate_final_concept_results,
        )

        return calculate_final_concept_results(self)

    class Settings:
        name = "concept_ground_truths"


"""
Indexes for ConceptGroundTruth

db.concept_ground_truths.createIndex(
  {
    mfg_etld1: 1,
    scraped_text_file_version_id: 1,
    extract_prompt_version_id: 1,
    map_prompt_version_id: 1,
    ontology_version_id: 1, # ADD More
    concept_type: 1,
    chunk_no: 1
  },
  {
    name: "concept_gt_unique_idx",
    unique: true,
  }
);

"""
