from datetime import datetime
import logging

from shared.models.db.manufacturer import IsManufacturerResult, Manufacturer

from data_etl_app.services.prompt_service import prompt_service
from data_etl_app.services.binary_classifier_service import (
    is_manufacturer,
)
from shared.models.types import MfgURLType

logger = logging.getLogger(__name__)


def reset_llm_aided_fields(manufacturer: Manufacturer):
    """
    Resets the classification and extracted fields of a manufacturer to None.
    This is useful when re-evaluating a manufacturer.
    """
    manufacturer.is_manufacturer = None
    manufacturer.is_contract_manufacturer = None
    manufacturer.is_product_manufacturer = None

    manufacturer.certificates = None
    manufacturer.industries = None
    manufacturer.material_caps = None
    manufacturer.process_caps = None
    manufacturer.products = None


async def update_manufacturer(updated_at: datetime, manufacturer: Manufacturer):
    manufacturer.updated_at = updated_at
    manufacturer = Manufacturer.model_validate(manufacturer.model_dump())

    logger.debug(f"Saving manufacturer {manufacturer} to the database.")
    await manufacturer.save()


async def is_company_a_manufacturer(
    timestamp: datetime, manufacturer_url: str, text: str
) -> IsManufacturerResult:

    logger.debug(f"Checking if {manufacturer_url} is a manufacturer...")

    name, binary_classifier_result = await is_manufacturer(
        timestamp,
        "is_manufacturer",
        manufacturer_url,
        text,
        prompt_service.is_manufacturer_prompt,
    )
    return IsManufacturerResult(
        name=name,
        **binary_classifier_result.model_dump(),
    )


async def find_random_manufacturer_url() -> MfgURLType | None:
    """
    Fetch a random manufacturer URL from the database.

    Returns:
        str: A random manufacturer URL.
    """
    agg_cursor = await Manufacturer.aggregate(
        [
            {"$match": {"is_manufacturer.answer": True}},
            {"$sample": {"size": 1}},
            {"$project": {"url": 1}},
        ]
    ).to_list(length=1)
    mfg_url = str(agg_cursor[0]["url"]) if agg_cursor else None
    return mfg_url


async def find_manufacturer_by_url(
    mfg_url: MfgURLType,
) -> Manufacturer | None:
    """
    Find a manufacturer by its URL.

    Args:
        mfg_url (MfgURLType): The URL of the manufacturer to find.

    Returns:
        Manufacturer | None: The manufacturer object if found, otherwise None.
    """
    return await Manufacturer.find_one({"url": mfg_url})


async def find_prevalidated_manufacturer_by_url(
    mfg_url: MfgURLType,
) -> Manufacturer:
    """
    Find a valid manufacturer by its URL.

    Args:
        mfg_url (MfgURLType): The URL of the manufacturer to find.

    Returns:
        Manufacturer | None: The manufacturer object if found and is a valid manufacturer, otherwise None.
    """
    manufacturer = await Manufacturer.find_one(
        {"url": mfg_url, "is_manufacturer.answer": True}
    )
    if not manufacturer:
        raise ValueError(
            f"Manufacturer with URL '{mfg_url}' does not exist or is not a valid manufacturer."
        )
    return manufacturer


async def find_manufacturer_by_url_and_scraped_file_version(
    mfg_url: MfgURLType,
    scraped_text_file_version_id: str,
) -> Manufacturer | None:
    """
    Find a manufacturer by its URL and scraped text file version ID.

    Args:
        mfg_url (MfgURLType): The URL of the manufacturer to find.
        scraped_text_file_version_id (str): The version ID of the scraped text file.

    Returns:
        Manufacturer | None: The manufacturer object if found, otherwise None.
    """
    return await Manufacturer.find_one(
        {
            "url": mfg_url,
            "scraped_text_file_version_id": scraped_text_file_version_id,
        }
    )
