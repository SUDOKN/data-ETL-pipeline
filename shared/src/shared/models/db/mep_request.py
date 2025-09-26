from beanie import Document
from datetime import datetime
from enum import Enum
from pydantic import Field

from shared.utils.time_util import get_current_time


class MEPRequestStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class MEPRequest(Document):
    userEmail: str
    status: MEPRequestStatus = MEPRequestStatus.PENDING
    reason: str | None = None  # Reason for rejection, if applicable
    createdAt: datetime = Field(default_factory=lambda: get_current_time())
    updatedAt: datetime = Field(default_factory=lambda: get_current_time())

    class Settings:
        name = "mep_requests"
