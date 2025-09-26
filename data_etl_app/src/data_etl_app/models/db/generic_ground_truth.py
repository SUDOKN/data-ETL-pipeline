from typing import Optional
from beanie import Document
from datetime import datetime
from pydantic import BaseModel, computed_field, Field

from shared.utils.time_util import get_current_time
from shared.models.field_types import (
    MfgETLDType,
    OntologyVersionIDType,
    S3FileVersionIDType,
)

from data_etl_app.models.types_and_enums import GenericFieldTypeEnum, GroundTruthSource


class GenericResultCorrection(BaseModel):
    author_email: str
    add: list[str]  # because the user can add unknown concepts directly
    remove: list[str]
    source: GroundTruthSource


class GenericResultCorrectionLog(BaseModel):
    """
    KeywordCorrectionLog stores the history of corrections made to the keyword extraction results.

    Attributes:
        created_at: When the correction was made.
        last_author_email: Email of the user who made the correction.
        result_correction: The correction details, including additions and removals.
    """

    created_at: datetime  # must be set beforehand, no default provided on purpose
    result_correction: GenericResultCorrection


# FULL TEXT LEVEL, NOT CHUNK LEVEL
class GenericGroundTruth(Document):
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())
    mfg_etld1: MfgETLDType

    field_type: GenericFieldTypeEnum

    # knowledge base identifiers
    scraped_text_file_version_id: str
    ontology_version_id: Optional[OntologyVersionIDType] = None
    extract_prompt_version_id: S3FileVersionIDType
    map_prompt_version_id: Optional[S3FileVersionIDType] = None

    # following is a copy of what was extracted at the time of creating this ground truth
    # stored originally in the linked manufacturer[field], e.g. manufacturer['products'] or any other field in GenericFieldTypeEnum
    results: set[str]
    results_extracted_at: datetime

    correction_logs: list[GenericResultCorrectionLog]

    @computed_field
    @property
    def final_results(self) -> list[str] | None:
        from data_etl_app.utils.ground_truth_helper_util import (
            calculate_final_results,
        )

        return calculate_final_results(self)

    class Settings:
        name = "generic_ground_truths"
