import asyncio
from datetime import datetime
from typing import Optional
import logging
from pymongo.errors import BulkWriteError
from pymongo import ReplaceOne

from core.models.db.manufacturer import Manufacturer
from scraper_app.models.scraped_text_file import ScrapedTextFile

from data_etl_app.services.llm_powered.extraction.extract_address_deferred_service import (
    extract_address_from_n_chunks_deferred,
)
from data_etl_app.services.llm_powered.search.llm_search_service import (
    find_business_desc_using_only_first_chunk_deferred,
)
from data_etl_app.services.llm_powered.extraction.extract_keyword_deferred_service import (
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

from open_ai_key_app.models.db.gpt_batch_request import GPTBatchRequest
from open_ai_key_app.models.db.deferred_manufacturer import DeferredManufacturer
from open_ai_key_app.services.gpt_batch_request_service import (
    bulk_upsert_gpt_batch_requests,
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
        f"Saving deferred manufacturer {deferred_manufacturer.mfg_etld1} to the database."
    )
    await deferred_manufacturer.save()


async def upsert_deferred_manufacturer(
    timestamp: datetime,
    manufacturer: Manufacturer,
    existing_deferred_manufacturer: Optional[DeferredManufacturer],
    scraped_text_file: ScrapedTextFile,  # Now passed as parameter instead of downloaded here
) -> tuple[DeferredManufacturer, bool]:
    """
    Upsert deferred manufacturer with pre-downloaded scraped text file.

    Note: scraped_text_file should be downloaded before calling this function
    to avoid holding semaphore slots during S3 network I/O.
    """
    # Use provided deferred manufacturer if available (batch optimization)
    deferred_manufacturer = existing_deferred_manufacturer
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
    # batch_requests_to_save: list[GPTBatchRequest] = []
    if not manufacturer.is_manufacturer and not deferred_manufacturer.is_manufacturer:
        (
            deferred_manufacturer.is_manufacturer,
            batch_requests,
        ) = await is_company_a_manufacturer_deferred(
            deferred_at=timestamp,
            manufacturer_etld=manufacturer.etld1,
            mfg_txt=scraped_text_file.text,
        )
        # batch_requests_to_save.extend(batch_requests)
        await bulk_upsert_gpt_batch_requests(
            batch_requests=batch_requests,
            mfg_etld1=manufacturer.etld1,
            chunk_size=2500,
        )
        updated = True

    if (
        not manufacturer.is_contract_manufacturer
        and not deferred_manufacturer.is_contract_manufacturer
    ):
        (
            deferred_manufacturer.is_contract_manufacturer,
            batch_requests,
        ) = await is_contract_manufacturer_deferred(
            deferred_at=timestamp,
            manufacturer_etld=manufacturer.etld1,
            mfg_txt=scraped_text_file.text,
        )
        # batch_requests_to_save.extend(batch_requests)
        await bulk_upsert_gpt_batch_requests(
            batch_requests=batch_requests,
            mfg_etld1=manufacturer.etld1,
            chunk_size=2500,
        )
        updated = True

    if (
        not manufacturer.is_product_manufacturer
        and not deferred_manufacturer.is_product_manufacturer
    ):
        (
            deferred_manufacturer.is_product_manufacturer,
            batch_requests,
        ) = await is_product_manufacturer_deferred(
            deferred_at=timestamp,
            manufacturer_etld=manufacturer.etld1,
            mfg_txt=scraped_text_file.text,
        )
        # batch_requests_to_save.extend(batch_requests)
        await bulk_upsert_gpt_batch_requests(
            batch_requests=batch_requests,
            mfg_etld1=manufacturer.etld1,
            chunk_size=2500,
        )
        updated = True

    if not manufacturer.addresses and not deferred_manufacturer.addresses:
        (
            deferred_manufacturer.addresses,
            batch_requests,
        ) = await extract_address_from_n_chunks_deferred(
            deferred_at=timestamp,
            keyword_label="addresses",
            mfg_etld1=manufacturer.etld1,
            mfg_text=scraped_text_file.text,
        )
        # batch_requests_to_save.extend(batch_requests)
        await bulk_upsert_gpt_batch_requests(
            batch_requests=batch_requests,
            mfg_etld1=manufacturer.etld1,
            chunk_size=2500,
        )
        updated = True

    if not manufacturer.business_desc and not deferred_manufacturer.business_desc:
        (
            deferred_manufacturer.business_desc,
            batch_request,
        ) = await find_business_desc_using_only_first_chunk_deferred(
            deferred_at=timestamp,
            mfg_etld1=manufacturer.etld1,
            mfg_text=scraped_text_file.text,
        )
        # batch_requests_to_save.append(batch_requests)
        await bulk_upsert_gpt_batch_requests(
            batch_requests=[batch_request],
            mfg_etld1=manufacturer.etld1,
            chunk_size=2500,
        )
        updated = True

    if not manufacturer.products and not deferred_manufacturer.products:
        (
            deferred_manufacturer.products,
            batch_requests,
        ) = await extract_products_deferred(
            deferred_at=timestamp,
            mfg_etld1=manufacturer.etld1,
            mfg_text=scraped_text_file.text,
        )
        # batch_requests_to_save.extend(batch_requests)
        await bulk_upsert_gpt_batch_requests(
            batch_requests=batch_requests,
            mfg_etld1=manufacturer.etld1,
            chunk_size=2500,
        )
        updated = True

    if (
        not manufacturer.certificates and not deferred_manufacturer.certificates
    ):  # missing phase 1
        (
            deferred_manufacturer.certificates,
            batch_requests,
        ) = await extract_certificates_deferred(
            deferred_at=timestamp,
            mfg_etld1=manufacturer.etld1,
            mfg_text=scraped_text_file.text,
        )
        # batch_requests_to_save.extend(batch_requests)
        await bulk_upsert_gpt_batch_requests(
            batch_requests=batch_requests,
            mfg_etld1=manufacturer.etld1,
            chunk_size=2500,
        )
        updated = True

    if (
        not manufacturer.industries and not deferred_manufacturer.industries
    ):  # missing phase 1
        (
            deferred_manufacturer.industries,
            batch_requests,
        ) = await extract_industries_deferred(
            deferred_at=timestamp,
            mfg_etld1=manufacturer.etld1,
            mfg_text=scraped_text_file.text,
        )
        # batch_requests_to_save.extend(batch_requests)
        await bulk_upsert_gpt_batch_requests(
            batch_requests=batch_requests,
            mfg_etld1=manufacturer.etld1,
            chunk_size=2500,
        )
        updated = True

    if not manufacturer.process_caps and not deferred_manufacturer.process_caps:
        (
            deferred_manufacturer.process_caps,
            batch_requests,
        ) = await extract_processes_deferred(
            deferred_at=timestamp,
            mfg_etld1=manufacturer.etld1,
            mfg_text=scraped_text_file.text,
        )
        # batch_requests_to_save.extend(batch_requests)
        await bulk_upsert_gpt_batch_requests(
            batch_requests=batch_requests,
            mfg_etld1=manufacturer.etld1,
            chunk_size=2500,
        )
        updated = True

    if not manufacturer.material_caps and not deferred_manufacturer.material_caps:
        (
            deferred_manufacturer.material_caps,
            batch_requests,
        ) = await extract_materials_deferred(
            deferred_at=timestamp,
            mfg_etld1=manufacturer.etld1,
            mfg_text=scraped_text_file.text,
        )
        # batch_requests_to_save.extend(batch_requests)
        await bulk_upsert_gpt_batch_requests(
            batch_requests=batch_requests,
            mfg_etld1=manufacturer.etld1,
            chunk_size=2500,
        )
        updated = True

    if updated:
        # Bulk upsert all batch requests using dedicated service function
        await update_deferred_manufacturer(timestamp, deferred_manufacturer)

    return deferred_manufacturer, updated
