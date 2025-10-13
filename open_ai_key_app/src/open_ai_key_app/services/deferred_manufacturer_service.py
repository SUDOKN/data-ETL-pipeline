import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer

from open_ai_key_app.models.db.deferred_manufacturer import DeferredManufacturer
from scraper_app.models.scraped_text_file import ScrapedTextFile

from data_etl_app.services.llm_powered.extraction.extract_address_deferred_service import (
    extract_address_from_n_chunks_deferred,
)
from data_etl_app.services.llm_powered.search.llm_search_service import (
    find_business_desc_using_only_first_chunk_deferred,
)
from data_etl_app.services.llm_powered.extraction.extract_keyword_deferred import (
    extract_products_deferred,
)
from data_etl_app.services.llm_powered.extraction.extract_concept_deferred_service import (
    add_certificate_mapping_requests_to_deferred_stats,
    add_industry_mapping_requests_to_deferred_stats,
    add_process_mapping_requests_to_deferred_stats,
    add_material_mapping_requests_to_deferred_stats,
    extract_certificates_deferred,
    extract_industries_deferred,
    extract_processes_deferred,
    extract_materials_deferred,
)
from data_etl_app.services.llm_powered.classification.deferred_binary_classifier import (
    is_company_a_manufacturer_deferred,
    is_contract_manufacturer_deferred,
    is_product_manufacturer_deferred,
)

logger = logging.getLogger(__name__)


async def get_deferred_manufacturer_by_etld1_scraped_file_version(
    mfg_etld1: str,
    scraped_text_file_version_id: str,
) -> Optional[DeferredManufacturer]:
    return await DeferredManufacturer.find_one(
        {
            "mfg_etld1": mfg_etld1,
            "scraped_text_file_version_id": scraped_text_file_version_id,
        }
    )


async def update_deferred_manufacturer(
    updated_at: datetime, deferred_manufacturer: DeferredManufacturer
):
    deferred_manufacturer.updated_at = updated_at
    logger.info(
        f"Saving deferred manufacturer {deferred_manufacturer} to the database."
    )
    await deferred_manufacturer.save()


async def upsert_deferred_manufacturer(
    timestamp: datetime,
    manufacturer: Manufacturer,
) -> tuple[DeferredManufacturer, bool]:
    existing_scraped_file, exception = (
        await ScrapedTextFile.download_from_s3_and_create(
            manufacturer.etld1,
            manufacturer.scraped_text_file_version_id,
        )
    )
    if exception:
        raise exception

    assert existing_scraped_file is not None

    deferred_manufacturer = (
        await get_deferred_manufacturer_by_etld1_scraped_file_version(
            mfg_etld1=manufacturer.etld1,
            scraped_text_file_version_id=manufacturer.scraped_text_file_version_id,
        )
    )
    if not deferred_manufacturer:
        deferred_manufacturer = DeferredManufacturer(
            created_at=timestamp,
            mfg_etld1=manufacturer.etld1,
            scraped_text_file_version_id=manufacturer.scraped_text_file_version_id,
            is_manufacturer=None,
            is_contract_manufacturer=None,
            is_product_manufacturer=None,
            addresses=None,
            business_desc=None,
            products=None,
            certificates=None,
            industries=None,
            process_caps=None,
            material_caps=None,
        )

    updated = False
    if not manufacturer.is_manufacturer:
        if deferred_manufacturer.is_manufacturer:
            logger.info(
                f"Skipping is_manufacturer deferred classification for {manufacturer.etld1} because"
                f" it has already been deferred even though it may not have been completed yet."
            )
        else:
            deferred_manufacturer.is_manufacturer = (
                await is_company_a_manufacturer_deferred(
                    deferred_at=timestamp,
                    manufacturer_etld=manufacturer.etld1,
                    mfg_txt=existing_scraped_file.text,
                )
            )
            updated = True

    if not manufacturer.is_contract_manufacturer:
        if deferred_manufacturer.is_contract_manufacturer:
            logger.info(
                f"Skipping is_contract_manufacturer deferred classification for {manufacturer.etld1}"
                f" because it has already been deferred even though it may not have been completed yet."
            )
        else:
            deferred_manufacturer.is_contract_manufacturer = (
                await is_contract_manufacturer_deferred(
                    deferred_at=timestamp,
                    manufacturer_etld=manufacturer.etld1,
                    mfg_txt=existing_scraped_file.text,
                )
            )
            updated = True

    if not manufacturer.is_product_manufacturer:
        if deferred_manufacturer.is_product_manufacturer:
            logger.info(
                f"Skipping is_product_manufacturer deferred classification for {manufacturer.etld1} because"
                f" it has already been deferred even though it may not have been completed yet."
            )
        else:
            deferred_manufacturer.is_product_manufacturer = (
                await is_product_manufacturer_deferred(
                    deferred_at=timestamp,
                    manufacturer_etld=manufacturer.etld1,
                    mfg_txt=existing_scraped_file.text,
                )
            )
            updated = True

    if not manufacturer.addresses:
        # find any incomplete deferred addresses
        if deferred_manufacturer.addresses is not None:
            logger.info(
                f"Skipping addresses deferred extraction for {manufacturer.etld1} because"
                f" it has already been deferred even though it may not have been completed yet."
            )
        else:
            deferred_manufacturer.addresses = (
                await extract_address_from_n_chunks_deferred(
                    deferred_at=timestamp,
                    keyword_label="addresses",
                    mfg_etld1=manufacturer.etld1,
                    mfg_text=existing_scraped_file.text,
                )
            )
            updated = True

    if not manufacturer.business_desc:
        if deferred_manufacturer.business_desc:
            logger.info(
                f"Skipping business_desc deferred extraction for {manufacturer.etld1} because"
                f" it has already been deferred even though it may not have been completed yet."
            )
        else:
            deferred_manufacturer.business_desc = (
                await find_business_desc_using_only_first_chunk_deferred(
                    deferred_at=timestamp,
                    mfg_etld1=manufacturer.etld1,
                    mfg_text=existing_scraped_file.text,
                )
            )
            updated = True

    if not manufacturer.products:
        if deferred_manufacturer.products:
            logger.info(
                f"Skipping products deferred extraction for {manufacturer.etld1} because"
                f" it has already been deferred even though it may not have been completed yet."
            )
        else:
            deferred_manufacturer.products = await extract_products_deferred(
                deferred_at=timestamp,
                mfg_etld1=manufacturer.etld1,
                mfg_text=existing_scraped_file.text,
            )
            updated = True

    if not manufacturer.certificates:
        if not deferred_manufacturer.certificates:  # missing phase 1
            deferred_manufacturer.certificates = await extract_certificates_deferred(
                deferred_at=timestamp,
                mfg_etld1=manufacturer.etld1,
                mfg_text=existing_scraped_file.text,
            )
            updated = True
        else:  # phase 1 present, check to see if phase 2 can begin for any chunk
            updated = (
                await add_certificate_mapping_requests_to_deferred_stats(
                    deferred_at=timestamp,
                    deferred_stats=deferred_manufacturer.certificates.deferred_stats,
                    mfg_etld1=manufacturer.etld1,
                )
                or updated
            )

    if not manufacturer.industries:
        if not deferred_manufacturer.industries:  # missing phase 1
            deferred_manufacturer.industries = await extract_industries_deferred(
                deferred_at=timestamp,
                mfg_etld1=manufacturer.etld1,
                mfg_text=existing_scraped_file.text,
            )
            updated = True
        else:  # phase 1 present, check to see if phase 2 can begin for any chunk
            updated = (
                await add_industry_mapping_requests_to_deferred_stats(
                    deferred_at=timestamp,
                    deferred_stats=deferred_manufacturer.industries.deferred_stats,
                    mfg_etld1=manufacturer.etld1,
                )
                or updated
            )

    if not manufacturer.process_caps:
        if not deferred_manufacturer.process_caps:  # missing phase 1
            deferred_manufacturer.process_caps = await extract_processes_deferred(
                deferred_at=timestamp,
                mfg_etld1=manufacturer.etld1,
                mfg_text=existing_scraped_file.text,
            )
            updated = True
        else:  # phase 1 present, check to see if phase 2 can begin for any chunk
            updated = (
                await add_process_mapping_requests_to_deferred_stats(
                    deferred_at=timestamp,
                    deferred_stats=deferred_manufacturer.process_caps.deferred_stats,
                    mfg_etld1=manufacturer.etld1,
                )
                or updated
            )

    if not manufacturer.material_caps:
        if not deferred_manufacturer.material_caps:  # missing phase 1
            deferred_manufacturer.material_caps = await extract_materials_deferred(
                deferred_at=timestamp,
                mfg_etld1=manufacturer.etld1,
                mfg_text=existing_scraped_file.text,
            )
            updated = True
        else:  # phase 1 present, check to see if phase 2 can begin for any chunk
            updated = (
                await add_material_mapping_requests_to_deferred_stats(
                    deferred_at=timestamp,
                    deferred_stats=deferred_manufacturer.material_caps.deferred_stats,
                    mfg_etld1=manufacturer.etld1,
                )
                or updated
            )

    if updated:
        await update_deferred_manufacturer(timestamp, deferred_manufacturer)

    return deferred_manufacturer, updated
