from datetime import datetime
from typing import Optional
import logging

from core.models.db.deferred_manufacturer import DeferredManufacturer
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
    BinaryClassificationTypeEnum,
    ConceptTypeEnum,
    KeywordTypeEnum,
)
from core.models.deferred_basic_extraction import DeferredBasicExtraction
from core.models.deferred_binary_classification import (
    DeferredBinaryClassification,
)
from core.models.deferred_keyword_extraction import (
    DeferredKeywordExtraction,
)
from core.models.deferred_concept_extraction import (
    DeferredConceptExtraction,
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


def is_deferred_manufacturer_empty(deferred_manufacturer: DeferredManufacturer) -> bool:
    # check if each optional field is None
    return all(
        getattr(deferred_manufacturer, field) is None
        for field in [
            BinaryClassificationTypeEnum.is_manufacturer.name,
            BinaryClassificationTypeEnum.is_contract_manufacturer.name,
            BinaryClassificationTypeEnum.is_product_manufacturer.name,
            BasicFieldTypeEnum.addresses.name,
            BasicFieldTypeEnum.business_desc.name,
            KeywordTypeEnum.products.name,
            ConceptTypeEnum.certificates.name,
            ConceptTypeEnum.industries.name,
            ConceptTypeEnum.process_caps.name,
            ConceptTypeEnum.material_caps.name,
        ]
    )


async def delete_deferred_manufacturer(
    deferred_manufacturer: DeferredManufacturer,
):
    logger.info(
        f"Deleting deferred manufacturer {deferred_manufacturer.mfg_etld1} from the database."
    )
    await deferred_manufacturer.delete()


async def delete_deferred_manufacturer_if_empty(
    deferred_manufacturer: DeferredManufacturer,
    # delete_batch_requests_too: bool,
):
    if is_deferred_manufacturer_empty(deferred_manufacturer):
        logger.info(
            f"✅✅✅ Deferred manufacturer {deferred_manufacturer.mfg_etld1} is empty. Deleting from the database."
        )
        await delete_deferred_manufacturer(deferred_manufacturer=deferred_manufacturer)


def get_bin_field_embedded_gpt_request_ids(
    bin_field: DeferredBinaryClassification,
) -> set[GPTBatchRequestCustomID]:
    custom_ids: set[GPTBatchRequestCustomID] = set()
    for (
        chunk_key,
        custom_id,
    ) in bin_field.chunk_request_id_map.items():
        custom_ids.add(custom_id)
    return custom_ids


def get_basic_field_embedded_gpt_request_id(
    basic_field: DeferredBasicExtraction,
) -> GPTBatchRequestCustomID:
    return basic_field.gpt_request_id


def get_keyword_field_embedded_gpt_request_ids(
    keyword_field: DeferredKeywordExtraction,
) -> set[GPTBatchRequestCustomID]:
    custom_ids: set[GPTBatchRequestCustomID] = set()
    for (
        chunk_key,
        custom_id,
    ) in keyword_field.chunk_request_id_map.items():
        custom_ids.add(custom_id)
    return custom_ids


def get_concept_field_embedded_gpt_request_ids(
    concept_field: DeferredConceptExtraction,
) -> set[GPTBatchRequestCustomID]:
    custom_ids: set[GPTBatchRequestCustomID] = set()
    for (
        chunk_key,
        bundle,
    ) in concept_field.chunk_request_bundle_map.items():
        custom_ids.add(bundle.llm_search_request_id)
    if concept_field.llm_mapping_request_id:
        custom_ids.add(concept_field.llm_mapping_request_id)
    return custom_ids


def get_embedded_gpt_request_ids(
    deferred_mfg: DeferredManufacturer,
) -> set[GPTBatchRequestCustomID]:
    custom_ids: set[GPTBatchRequestCustomID] = set()

    if deferred_mfg.is_manufacturer:
        custom_ids |= get_bin_field_embedded_gpt_request_ids(
            deferred_mfg.is_manufacturer
        )
    if deferred_mfg.is_contract_manufacturer:
        custom_ids |= get_bin_field_embedded_gpt_request_ids(
            deferred_mfg.is_contract_manufacturer
        )
    if deferred_mfg.is_product_manufacturer:
        custom_ids |= get_bin_field_embedded_gpt_request_ids(
            deferred_mfg.is_product_manufacturer
        )

    if deferred_mfg.addresses:
        custom_ids.add(get_basic_field_embedded_gpt_request_id(deferred_mfg.addresses))
    if deferred_mfg.business_desc:
        custom_ids.add(
            get_basic_field_embedded_gpt_request_id(deferred_mfg.business_desc)
        )

    if deferred_mfg.products:
        custom_ids |= get_keyword_field_embedded_gpt_request_ids(deferred_mfg.products)

    if deferred_mfg.certificates:
        custom_ids |= get_concept_field_embedded_gpt_request_ids(
            deferred_mfg.certificates
        )
    if deferred_mfg.industries:
        custom_ids |= get_concept_field_embedded_gpt_request_ids(
            deferred_mfg.industries
        )
    if deferred_mfg.process_caps:
        custom_ids |= get_concept_field_embedded_gpt_request_ids(
            deferred_mfg.process_caps
        )
    if deferred_mfg.material_caps:
        custom_ids |= get_concept_field_embedded_gpt_request_ids(
            deferred_mfg.material_caps
        )

    return custom_ids


'''
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
            mfg_etld1=manufacturer.etld1,
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
        ) = await find_addresses_from_first_chunk_deferred(
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
        ) = await get_missing_product_search_requests(
            deferred_at=timestamp,
            mfg_etld1=manufacturer.etld1,
            mfg_text=scraped_text_file.text,
            deferred_manufacturer=deferred_manufacturer,
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
        ) = await get_deferred_certificate_extraction_requests(
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
        ) = await get_deferred_industry_extraction_requests(
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
        ) = await get_deferred_process_extraction_requests(
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
        ) = await get_deferred_material_extraction_requests(
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
        await update_deferred_manufacturer(timestamp, deferred_manufacturer)

    return deferred_manufacturer, updated


# pass only those whose phase 1 is guranteed each chunk fully complete with a response blob
async def upsert_deferred_manufacturer_phase2(
    timestamp: datetime,
    deferred_manufacturer: DeferredManufacturer,
) -> tuple[DeferredManufacturer, bool]:
    if (
        not deferred_manufacturer.certificates
        or not deferred_manufacturer.certificates.deferred_stats
        or not deferred_manufacturer.certificates.deferred_stats.chunked_batch_request_bundle_map
    ):
        raise ValueError(
            f"{deferred_manufacturer.mfg_etld1}: deferred_manufacturer.certificates phase 1 incomplete"
        )
    else:
        llm_gpt_batch_requests = await _validate_and_get_phase1_completed_requests(
            mfg_etld1=deferred_manufacturer.mfg_etld1,
            deferred_stats=deferred_manufacturer.certificates.deferred_stats,
        )
        updated, new_batch_requests = await add_certificate_mapping_requests(
            deferred_at=timestamp,
            deferred_stats=deferred_manufacturer.certificates.deferred_stats,
            llm_gpt_batch_requests=llm_gpt_batch_requests,
            mfg_etld1=deferred_manufacturer.mfg_etld1,
        )
        await bulk_upsert_gpt_batch_requests(
            batch_requests=new_batch_requests,
            mfg_etld1=deferred_manufacturer.mfg_etld1,
            chunk_size=2500,
        )

    if (
        not deferred_manufacturer.industries
        or not deferred_manufacturer.industries.deferred_stats
        or not deferred_manufacturer.industries.deferred_stats.chunked_batch_request_bundle_map
    ):
        raise ValueError(
            f"{deferred_manufacturer.mfg_etld1}: deferred_manufacturer.industries phase 1 incomplete"
        )
    else:
        llm_gpt_batch_requests = await _validate_and_get_phase1_completed_requests(
            mfg_etld1=deferred_manufacturer.mfg_etld1,
            deferred_stats=deferred_manufacturer.industries.deferred_stats,
        )
        updated, new_batch_requests = await add_industry_mapping_requests(
            deferred_at=timestamp,
            deferred_stats=deferred_manufacturer.industries.deferred_stats,
            llm_gpt_batch_requests=llm_gpt_batch_requests,
            mfg_etld1=deferred_manufacturer.mfg_etld1,
        )
        await bulk_upsert_gpt_batch_requests(
            batch_requests=new_batch_requests,
            mfg_etld1=deferred_manufacturer.mfg_etld1,
            chunk_size=2500,
        )

    if (
        not deferred_manufacturer.process_caps
        or not deferred_manufacturer.process_caps.deferred_stats
        or not deferred_manufacturer.process_caps.deferred_stats.chunked_batch_request_bundle_map
    ):
        raise ValueError(
            f"{deferred_manufacturer.mfg_etld1}: deferred_manufacturer.process_caps phase 1 incomplete"
        )
    else:
        llm_gpt_batch_requests = await _validate_and_get_phase1_completed_requests(
            mfg_etld1=deferred_manufacturer.mfg_etld1,
            deferred_stats=deferred_manufacturer.process_caps.deferred_stats,
        )
        updated, new_batch_requests = await add_process_mapping_requests(
            deferred_at=timestamp,
            deferred_stats=deferred_manufacturer.process_caps.deferred_stats,
            llm_gpt_batch_requests=llm_gpt_batch_requests,
            mfg_etld1=deferred_manufacturer.mfg_etld1,
        )
        await bulk_upsert_gpt_batch_requests(
            batch_requests=new_batch_requests,
            mfg_etld1=deferred_manufacturer.mfg_etld1,
            chunk_size=2500,
        )

    if (
        not deferred_manufacturer.material_caps
        or not deferred_manufacturer.material_caps.deferred_stats
        or not deferred_manufacturer.material_caps.deferred_stats.chunked_batch_request_bundle_map
    ):
        raise ValueError(
            f"{deferred_manufacturer.mfg_etld1}: deferred_manufacturer.material_caps phase 1 incomplete"
        )
    else:
        llm_gpt_batch_requests = await _validate_and_get_phase1_completed_requests(
            mfg_etld1=deferred_manufacturer.mfg_etld1,
            deferred_stats=deferred_manufacturer.material_caps.deferred_stats,
        )
        updated, new_batch_requests = await add_material_mapping_requests(
            deferred_at=timestamp,
            deferred_stats=deferred_manufacturer.material_caps.deferred_stats,
            llm_gpt_batch_requests=llm_gpt_batch_requests,
            mfg_etld1=deferred_manufacturer.mfg_etld1,
        )
        await bulk_upsert_gpt_batch_requests(
            batch_requests=new_batch_requests,
            mfg_etld1=deferred_manufacturer.mfg_etld1,
            chunk_size=2500,
        )

    if updated:
        await update_deferred_manufacturer(timestamp, deferred_manufacturer)

    return deferred_manufacturer, updated


async def _validate_and_get_phase1_completed_requests(
    mfg_etld1: str,
    deferred_stats: DeferredConceptExtractionStats,
) -> list[GPTBatchRequest]:
    llm_batch_request_ids = []
    batch_request_map_items = deferred_stats.chunked_batch_request_bundle_map.items()
    for (
        _chunk_bounds,
        search_bundle,
    ) in batch_request_map_items:
        if not search_bundle.llm_batch_request_id:
            raise ValueError(
                f"{mfg_etld1}: deferred_manufacturer.certificates phase 1 partially incomplete"
            )
        else:
            llm_batch_request_ids.append(search_bundle.llm_batch_request_id)

    llm_gpt_batch_requests = await GPTBatchRequest.find(
        {
            "request.custom_id": {"$in": llm_batch_request_ids},
            "response_blob": {"$ne": None},  # to check completed
        }
    ).to_list()
    if len(llm_gpt_batch_requests) != len(batch_request_map_items):
        raise ValueError(
            f"{mfg_etld1}: deferred_manufacturer.certificates.chunk_request_id_map phase 1 incomplete, contains stale or in process GPTBatchRequestCustomIDs"
        )

    return llm_gpt_batch_requests
'''
