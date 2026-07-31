from datetime import datetime
import logging

from beanie.operators import In
from core.models.db.manufacturer import Batch, Manufacturer
from core.models.field_types import MfgURLType, MfgETLDType
from core.utils.url_util import get_etld1_from_host
from scraper_app.models.scraped_text_file import ScrapedTextFile

logger = logging.getLogger(__name__)


def reset_llm_extracted_fields(manufacturer: Manufacturer):
    """
    Resets the classification and extracted fields of a manufacturer to None.
    This is useful when re-evaluating a manufacturer.
    """
    # manufacturer.addresses = None
    # manufacturer.business_desc = None
    # manufacturer.is_manufacturer = None
    # manufacturer.is_contract_manufacturer = None
    # manufacturer.is_product_manufacturer = None

    manufacturer.products = None
    manufacturer.contract_products = None
    manufacturer.equipments = None

    manufacturer.certificates = None
    manufacturer.industries = None
    manufacturer.material_caps = None
    manufacturer.process_caps = None

    logger.info(
        f"Reset LLM extracted fields for manufacturer with etld1: {manufacturer.etld1}"
    )


async def create_new_manufacturer(
    created_at: datetime,
    mfg_etld1: MfgETLDType,
    scraped_file: ScrapedTextFile,
    batch: Batch,
) -> Manufacturer:
    return Manufacturer(
        created_at=created_at,
        etld1=mfg_etld1,
        etld1_accessible_at=scraped_file.etld1_accessible_at,
        scraped_text_file_num_tokens=scraped_file.num_tokens,
        scraped_text_file_version_id=scraped_file.s3_version_id,
        batches=[batch],
        # Following fields will be set later during extraction
        name=None,
        is_manufacturer=None,
        is_contract_manufacturer=None,
        is_product_manufacturer=None,
        founded_in=None,
        email_addresses=None,
        num_employees=None,
        business_desc=None,
        business_statuses=None,
        primary_naics=None,
        secondary_naics=None,
        addresses=None,
        products=None,
        contract_products=None,
        equipments=None,
        certificates=None,
        industries=None,
        process_caps=None,
        material_caps=None,
    )


async def update_manufacturer(updated_at: datetime, manufacturer: Manufacturer):
    manufacturer.updated_at = updated_at
    logger.info(f"Saving manufacturer {manufacturer.etld1} to the database.")
    await manufacturer.save()


# unused
def is_llm_extraction_complete(manufacturer: Manufacturer) -> bool:
    """
    Check if the LLM extraction for the manufacturer is complete.

    Args:
        manufacturer (Manufacturer): The manufacturer object to check.
    Returns:
        bool: True if extraction is complete, False otherwise.
    """
    required_fields = [
        manufacturer.is_manufacturer,
        manufacturer.is_contract_manufacturer,
        manufacturer.is_product_manufacturer,
        manufacturer.business_desc,
        manufacturer.products,
        manufacturer.certificates,
        manufacturer.industries,
        manufacturer.material_caps,
        manufacturer.process_caps,
    ]

    extraction_complete = all(field is not None for field in required_fields)
    logger.debug(
        f"Extraction complete for manufacturer {manufacturer.etld1}: {extraction_complete}"
    )
    return extraction_complete


async def find_random_manufacturer_url() -> MfgURLType | None:
    """
    Fetch a random manufacturer starting eTLD+1 from the database.

    Returns:
        str: A random manufacturer starting eTLD+1.
    """
    agg_cursor = await Manufacturer.aggregate(
        [
            {"$match": {"is_manufacturer.result.answer": True}},
            {"$sample": {"size": 1}},
            {"$project": {"etld1": 1}},
        ]
    ).to_list(length=1)
    mfg_url = str(agg_cursor[0]["etld1"]) if agg_cursor else None
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


async def find_manufacturers_by_etld1s(
    mfg_etld1s: list[MfgETLDType],
) -> list[Manufacturer]:
    """
    Find a manufacturer by its URL.

    Args:
        mfg_url (MfgURLType): The URL of the manufacturer to find.

    Returns:
        Manufacturer | None: The manufacturer object if found, otherwise None.
    """

    logger.debug(f"Finding manufacturer with {len(mfg_etld1s)} mfg_etld1s")
    return await Manufacturer.find(In(Manufacturer.etld1, mfg_etld1s)).to_list()


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
    return await Manufacturer.find_one(Manufacturer.etld1 == mfg_etld1)


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
        Manufacturer.etld1 == mfg_etld1,
        Manufacturer.is_manufacturer.result.answer == True,  # type: ignore[union-attr]
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
        Manufacturer.etld1 == mfg_etld1,
        Manufacturer.scraped_text_file_version_id == scraped_text_file_version_id,
    )
