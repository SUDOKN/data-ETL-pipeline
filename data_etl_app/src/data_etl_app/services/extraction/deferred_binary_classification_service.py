import json
import logging
from typing import Optional

from core.models.binary_classification_result import BinaryClassification

logger = logging.getLogger(__name__)


def parse_binary_classification_result_from_gpt_response(
    gpt_response: Optional[str],
) -> BinaryClassification:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_binary_classification_result_from_gpt_response: Empty or invalid response from GPT"
        )

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        json_response = json.loads(gpt_response)
    except:
        raise ValueError(
            f"parse_binary_classification_result_from_gpt_response: Invalid response from GPT:{gpt_response}"
        )

    return BinaryClassification(**json_response)
