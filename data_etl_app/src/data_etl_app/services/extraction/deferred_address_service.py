import json
import logging
from typing import Optional

from core.models.address_extraction_result import (
    Address,
)
from core.utils.str_util import make_json_array_parse_safe

logger = logging.getLogger(__name__)


def parse_address_list_from_gpt_response(
    gpt_response: Optional[str],
) -> list[Address]:
    addresses = []
    if not gpt_response:
        logger.error(
            f"parse_address_list_from_gpt_response: Invalid gpt_response:{gpt_response}, returning empty list"
        )
        return []

    try:
        cleaned_response = make_json_array_parse_safe(gpt_response)
    except Exception as e:
        logger.error(
            (
                f"parse_address_list_from_gpt_response: Failed to make_json_parse_safe GPT response: {e}\n",
                f"cleaned_response={gpt_response}, returning empty list",
            ),
            exc_info=True,
        )
        return []

    try:
        json_response = json.loads(cleaned_response)
    except Exception as e:
        logger.error(
            (
                f"parse_address_list_from_gpt_response: Failed to json.loads(cleaned_response): {e}\n",
                f"gpt_response={gpt_response}\n" f"cleaned_response={cleaned_response}",
            ),
            exc_info=True,
        )
        return []

    if isinstance(json_response, list):
        for addr in json_response:
            try:
                country = addr.get("country")
                if not country:
                    addr["country"] = "US"
                else:
                    addr["country"] = country.upper()
                addresses.append(Address(**addr))
            except Exception as e:
                logger.error(
                    f"parse_address_list_from_gpt_response: Skipping failed parsed address from GPT response addr:{addr}\n"
                    f"error={e}",
                    exc_info=True,
                )
    else:
        logger.info(
            f"parse_address_list_from_gpt_response: extracted non-list {json_response}, returning empty list"
        )
        return []

    # dedupe_addresses(addresses=addresses)  # modifies in place, commented out to keep integrity of what was exactly extracted

    return addresses
