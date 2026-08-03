import logging
from typing import Optional

from pydantic import ValidationError

from data_etl_app.models.extraction_results.business_description_extraction_result import (
    BusinessDescription,
)
from core.models.extraction_schemas.basic_fields import BusinessDescResponse

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
        parsed = BusinessDescResponse.model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_business_desc_from_gpt_response: Invalid response from GPT:{gpt_response}"
        ) from e

    logger.debug(
        f"parse_business_desc_from_gpt_response:`{parsed.name}`\n`{parsed.description}`"
    )

    return BusinessDescription(name=parsed.name, description=parsed.description)
