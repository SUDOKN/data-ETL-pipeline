import json
import logging
from typing import Optional

from core.models.business_description_extraction_result import BusinessDescription

logger = logging.getLogger(__name__)


def parse_business_desc_from_gpt_response(
    gpt_response: Optional[str],
) -> BusinessDescription:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_business_desc_from_gpt_response: Empty or invalid response from GPT"
        )

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        json_response = json.loads(gpt_response)
        business_name = json_response.get("name")
        business_desc = json_response.get("description")
    except:
        raise ValueError(
            f"parse_business_desc_from_gpt_response: Invalid response from GPT:{gpt_response}"
        )

    logger.debug(
        f"parse_business_desc_from_gpt_response:`{business_name}`\n`{business_desc}`"
    )

    return BusinessDescription(name=business_name, description=business_desc)
