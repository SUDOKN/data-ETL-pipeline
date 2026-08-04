import logging
from typing import Optional

from pydantic import ValidationError

from packages.core.src.core.models.extraction_results.binary_classification_result import (
    LLMBinaryClassification,
)
from packages.core.src.core.models.extraction_schemas.basic_fields import (
    BinaryClassificationResponse,
)

logger = logging.getLogger(__name__)


def parse_binary_classification_result_from_gpt_response(
    gpt_response: Optional[str],
) -> LLMBinaryClassification:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_binary_classification_result_from_gpt_response: Empty or invalid response from GPT"
        )

    try:
        parsed = BinaryClassificationResponse.model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_binary_classification_result_from_gpt_response: Invalid response from GPT:{gpt_response}"
        ) from e

    return LLMBinaryClassification(**parsed.model_dump())
