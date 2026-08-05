from beanie import Document
from datetime import datetime
from pydantic import BaseModel, Field, computed_field

from packages.infra.src.infra.field_types import (
    S3FileVersionIDType,
)
from packages.core.src.core.field_types import (
    SubjectUniqueIDType,
)
from packages.core.src.core.models.extraction_results.binary_classification_result import (
    BaseClassificationDecision,
    BinaryClassificationStats,
)
from packages.core.src.core.models.extraction_results.single_stage_extraction_results import (
    LLMSingleStageExtractionMetadata,
)
from packages.core.src.core.models.types_and_enums import (
    GroundTruthSource,
    BinaryClassificationTypeEnum,
)

from packages.pure_utils.src.pure_utils.time_util import get_current_time


class HumanBinaryDecision(BaseClassificationDecision):
    """
    HumanBinaryDecision represents a binary decision made by a human expert.

    Attributes:
        decision: The binary decision made by the human expert.
        reason: The reason for the decision.
    """

    author_email: str
    source: GroundTruthSource
    # answer: bool
    # reason: str | None


class HumanDecisionLog(BaseModel):
    """
    HumanDecisionLog stores the history of human decisions made on binary ground truths.

    Attributes:
        created_at: When the decision was made.
        author_email: Email of the sudokn user who made the decision.
        human_decision: The human decision details, including answer and reason.
    """

    created_at: datetime  # must be set beforehand, no default provided on purpose
    human_decision: HumanBinaryDecision


class BinaryGroundTruth(Document):
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())

    mfg_etld1: SubjectUniqueIDType
    scraped_text_file_version_id: S3FileVersionIDType
    classification_type: BinaryClassificationTypeEnum

    # chunk identifiers
    chunk_bounds: str
    chunk_text: str

    # following is a copy of what was extracted at the time of creating this ground truth
    # stored originally in the linked manufacturer
    metadata: LLMSingleStageExtractionMetadata
    extraction_stats: BinaryClassificationStats

    corrections: list[HumanDecisionLog]

    @computed_field
    @property
    def final_decision(self) -> BaseClassificationDecision:
        for log in reversed(self.corrections):
            if log.human_decision.source == GroundTruthSource.API_SURVEY:
                return log.human_decision
        return self.extraction_stats.result

    class Settings:
        name = "binary_ground_truths"


"""
Indexes in MongoDB for BinaryGroundTruth:

db.binary_ground_truths.createIndex(
  {
    mfg_etld1: 1,
    scraped_text_file_version_id: 1,
    "metadata.prompt_version_id": 1,
    classification_type: 1
  },
  { 
    name: "binary_gt_unique_idx",
    unique: true 
  }
)

"""
