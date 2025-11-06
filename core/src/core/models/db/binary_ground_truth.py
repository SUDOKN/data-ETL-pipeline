from beanie import Document
from datetime import datetime
from pydantic import BaseModel, Field, computed_field

from core.models.field_types import MfgETLDType
from core.utils.time_util import get_current_time

from core.models.binary_classification_result import BinaryClassificationResult
from data_etl_app.models.types_and_enums import (
    GroundTruthSource,
    BinaryClassificationTypeEnum,
)


class HumanBinaryDecision(BaseModel):
    """
    HumanBinaryDecision represents a binary decision made by a human expert.

    Attributes:
        decision: The binary decision made by the human expert.
        reason: The reason for the decision.
    """

    author_email: str
    source: GroundTruthSource
    answer: (
        bool | None
    )  # CAUTION: this field is optional just for convenience of gt template, not optional in db schema
    reason: str | None


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
    mfg_etld1: MfgETLDType

    # knowledge base identifiers
    scraped_text_file_version_id: str

    classification_type: BinaryClassificationTypeEnum

    # following is a copy of what was extracted at the time of creating this ground truth
    # stored originally in the linked manufacturer
    llm_decision: BinaryClassificationResult

    human_decision_logs: list[HumanDecisionLog]

    @computed_field
    @property
    def final_decision(self) -> HumanBinaryDecision | None:
        if self.human_decision_logs:
            for log in reversed(self.human_decision_logs):
                if log.human_decision.source == GroundTruthSource.API_SURVEY:
                    return log.human_decision
        return None

    class Settings:
        name = "binary_ground_truths"


"""
Indexes in MongoDB for BinaryGroundTruth:

db.binary_ground_truths.createIndex(
  {
    mfg_etld1: 1,
    scraped_text_file_version_id: 1,
    classification_type: 1
  },
  { 
    name: "binary_gt_unique_idx",
    unique: true 
  }
)
"""
