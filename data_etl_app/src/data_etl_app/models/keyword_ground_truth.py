from beanie import Document
from datetime import datetime
from pydantic import BaseModel, ValidationInfo, computed_field, Field, field_validator

from shared.utils.time_util import get_current_time
from shared.models.types import LLMMappingType, MfgETLDType, OntologyVersionIDType
from shared.models.db.extraction_results import ChunkSearchStats

from data_etl_app.models.types import ConceptTypeEnum


class HumanCorrection(BaseModel):
    author_email: str
    add: LLMMappingType
    remove: list[str]


class HumanCorrectionLog(BaseModel):
    """
    ResultCorrectionLog stores the history of corrections made to the keyword extraction results.

    Attributes:
        created_at: When the correction was made.
        last_author_email: Email of the user who made the correction.
        result_correction: The correction details, including additions and removals.
    """

    created_at: datetime  # must be set beforehand, no default provided on purpose
    result_correction: HumanCorrection


class KeywordGroundTruth(Document):
    """
    KeywordGroundTruth represents the ground truth for keyword extraction from manufacturer (mfg) scraped text chunks.

    This document stores expert corrections and metadata to ensure provenance and consistency, even if the source data changes.

    Attributes:
        created_at: Timestamp when the entry was created.
        updated_at: Timestamp when the entry was last updated.
        scraped_text_file_version_id: Identifier for the scraped text file version.
        ontology_version_id: Ontology version identifier at the time of ground truth creation.
        mfg_url: Reference to the manufacturer.
        concept_type: The type of concept for this ground truth.
        chunk_bounds: Reference to the chunk in manufacturer data.
        chunk_text: The text of the chunk.
        last_chunk_no: The last chunk number in the manufacturer data.
        chunk_no: The chunk number for this ground truth (must be between 1 and last_chunk_no).
        chunk_extracted_at: When the chunk was extracted.
        chunk_search_stats: Search statistics and initial keyword extraction results.
        human_correction_logs: List of human corrections to be applied to the keyword extraction results.

    Computed Properties:
        final_results: Final set of results after applying corrections, or None if no corrections.

    Notes:
        - Ensures chunk_no is within a valid range.
        - Designed for robust API interaction and data consistency.
    """

    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())
    mfg_etld1: MfgETLDType

    # knowledge base identifiers
    scraped_text_file_version_id: str
    ontology_version_id: OntologyVersionIDType

    concept_type: ConceptTypeEnum

    # chunk identifiers, all need to match
    chunk_bounds: str
    last_chunk_no: int
    chunk_no: int
    chunk_extracted_at: datetime

    chunk_text: str
    chunk_search_stats: ChunkSearchStats

    # human_correction: Optional[HumanCorrection] = None
    human_correction_logs: list[HumanCorrectionLog]

    # validator to check chunk_no ge 1 and le last_chunk_no
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
        from data_etl_app.utils.keyword_ground_truth_helper_util import (
            calculate_final_results,
        )

        return calculate_final_results(self)

    class Settings:
        name = "keyword_ground_truths"


"""
Indexes for KeywordGroundTruth

db.keyword_ground_truths.createIndex(
  {
    mfg_url: 1,
    scraped_text_file_version_id: 1,
    ontology_version_id: 1,
    concept_type: 1,
    chunk_no: 1
  },
  {
    name: "uniq_mfg_ver_ont_type_chunk",
    unique: true,
    background: true   // build in the background so it doesn’t lock writes
  }
);

"""
