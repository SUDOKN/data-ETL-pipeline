import logging
from typing import Optional

from pydantic import ValidationError

from apps.data_etl_app.src.data_etl_app.models.extraction_results.address_extraction_result import (
    Address,
)
from apps.data_etl_app.src.data_etl_app.models.basic_fields import (
    AddressExtractionResponse,
)

logger = logging.getLogger(__name__)


def parse_address_list_from_gpt_response(
    gpt_response: Optional[str],
) -> list[Address]:
    if not gpt_response:
        logger.error(
            f"parse_address_list_from_gpt_response: Invalid gpt_response:{gpt_response}, returning empty list"
        )
        return []

    try:
        parsed = AddressExtractionResponse.model_validate_json(gpt_response)
    except ValidationError as e:
        logger.error(
            f"parse_address_list_from_gpt_response: Failed to validate gpt_response: {e}\n"
            f"gpt_response={gpt_response}",
            exc_info=True,
        )
        return []

    addresses: list[Address] = []
    for wire_addr in parsed.addresses:
        try:
            addr = wire_addr.model_dump()
            country = addr.get("country")
            addr["country"] = country.upper() if country else "US"
            addresses.append(Address(**addr))
        except Exception as e:
            logger.error(
                f"parse_address_list_from_gpt_response: Skipping failed parsed address from GPT response addr:{addr}\n"
                f"error={e}",
                exc_info=True,
            )

    # dedupe_addresses(addresses=addresses)  # modifies in place, commented out to keep integrity of what was exactly extracted

    return addresses
