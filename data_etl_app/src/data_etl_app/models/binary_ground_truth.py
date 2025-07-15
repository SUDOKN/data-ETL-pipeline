from beanie import Document
from datetime import datetime
from pydantic import BaseModel, Field, computed_field

from shared.models.db.manufacturer import BinaryClassifierResult
from shared.models.types import MfgURLType

from shared.utils.time_util import get_current_time

from data_etl_app.models.types import BinaryClassificationTypeEnum


class HumanBinaryDecision(BaseModel):
    """
    HumanBinaryDecision represents a binary decision made by a human expert.

    Attributes:
        decision: The binary decision made by the human expert.
        reason: The reason for the decision.
    """

    author_email: str
    answer: bool | None
    reason: str | None


class HumanDecisionLog(BaseModel):
    """
    HumanDecisionLog stores the history of human decisions made on binary ground truths.

    Attributes:
        created_at: When the decision was made.
        author_email: Email of the user who made the decision.
        human_decision: The human decision details, including answer and reason.
    """

    created_at: datetime  # must be set beforehand, no default provided on purpose
    human_decision: HumanBinaryDecision


class BinaryGroundTruth(Document):
    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())
    mfg_url: MfgURLType

    # knowledge base identifiers
    scraped_text_file_version_id: str

    classification_type: BinaryClassificationTypeEnum

    llm_decision: BinaryClassifierResult
    human_decision_logs: list[HumanDecisionLog]

    @computed_field
    @property
    def latest_human_decision(self) -> HumanBinaryDecision | None:
        if self.human_decision_logs:
            return self.human_decision_logs[-1].human_decision
        return None

    class Settings:
        name = "binary_ground_truths"


"""
Indexes in MongoDB for BinaryGroundTruth:

db.binary_ground_truths.createIndex(
  {
    mfg_url: 1,
    scraped_text_file_version_id: 1,
    classification_type: 1
  },
  { unique: true }
)
"""
