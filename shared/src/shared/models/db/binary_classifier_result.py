from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional


class BinaryClassifierResult(BaseModel):
    evaluated_at: datetime
    name: Optional[str] = Field(default=None)
    answer: bool
    reason: str
