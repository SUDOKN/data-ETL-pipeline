from pydantic import BaseModel, Field
from typing import Optional, TypedDict


class BinaryClassifierResult(TypedDict):
    name: str
    answer: bool
    explanation: str


class BinaryClassifierResult_DBModel(BaseModel):
    name: Optional[str] = Field(default=None)
    answer: bool
    explanation: str

    @classmethod
    def from_dict(cls, binary_classifier_result: BinaryClassifierResult):
        return cls(
            name=binary_classifier_result.get("name"),
            answer=binary_classifier_result.get("answer"),
            explanation=binary_classifier_result.get("explanation"),
        )
