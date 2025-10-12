from datetime import datetime
import logging

from core.models.db.manufacturer import Manufacturer
from core.models.field_types import MfgURLType, MfgETLDType
from core.utils.url_util import get_etld1_from_host


logger = logging.getLogger(__name__)


def reset_llm_extracted_fields(manufacturer: Manufacturer):
    """
    Resets the classification and extracted fields of a manufacturer to None.
    This is useful when re-evaluating a manufacturer.
    """
    manufacturer.is_manufacturer = None
    manufacturer.is_contract_manufacturer = None
    manufacturer.is_product_manufacturer = None

    manufacturer.business_desc = None
    manufacturer.products = None
    manufacturer.certificates = None
    manufacturer.industries = None
    manufacturer.material_caps = None
    manufacturer.process_caps = None
    manufacturer.products = None
    logger.info(
        f"Reset LLM extracted fields for manufacturer with etld1: {manufacturer.etld1}"
    )


async def update_manufacturer(updated_at: datetime, manufacturer: Manufacturer):
    manufacturer.updated_at = updated_at
    logger.info(f"Saving manufacturer {manufacturer.etld1} to the database.")
    await manufacturer.save()


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
            {"$project": {"url_accessible_at": 1}},
        ]
    ).to_list(length=1)
    mfg_url = str(agg_cursor[0]["url_accessible_at"]) if agg_cursor else None
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
    mfg_etld1 = get_etld1_from_host(mfg_url)
    logger.debug(f"Finding manufacturer with mfg_etld1: {mfg_etld1} and url: {mfg_url}")
    return await find_manufacturer_by_etld1(mfg_etld1)


async def find_manufacturer_by_etld1(
    mfg_etld1: MfgETLDType,
) -> Manufacturer | None:
    """
    Find a manufacturer by its URL.

    Args:
        mfg_url (MfgURLType): The URL of the manufacturer to find.

    Returns:
        Manufacturer | None: The manufacturer object if found, otherwise None.
    """

    logger.debug(f"Finding manufacturer with just mfg_etld1: {mfg_etld1}")
    return await Manufacturer.find_one({"etld1": mfg_etld1})


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
    mfg_etld1 = get_etld1_from_host(mfg_url)
    return await find_prevalidated_manufacturer_by_etld1(mfg_etld1)


async def find_prevalidated_manufacturer_by_etld1(
    mfg_etld1: MfgETLDType,
) -> Manufacturer:
    """
    Find a valid manufacturer by its URL.

    Args:
        mfg_url (MfgURLType): The URL of the manufacturer to find.

    Returns:
        Manufacturer | None: The manufacturer object if found and is a valid manufacturer, otherwise None.
    """

    manufacturer = await Manufacturer.find_one(
        {"etld1": mfg_etld1, "is_manufacturer.answer": True}
    )
    if not manufacturer:
        raise ValueError(
            f"Manufacturer with URL '{mfg_etld1}' does not exist or is not a valid manufacturer."
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
    mfg_etld1 = get_etld1_from_host(mfg_url)
    return await find_manufacturer_by_etld1_and_scraped_file_version(
        mfg_etld1, scraped_text_file_version_id
    )


async def find_manufacturer_by_etld1_and_scraped_file_version(
    mfg_etld1: MfgETLDType,
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
            "etld1": mfg_etld1,
            "scraped_text_file_version_id": scraped_text_file_version_id,
        }
    )
