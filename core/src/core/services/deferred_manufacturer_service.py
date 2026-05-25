from datetime import datetime
from typing import Optional
import logging

from core.models.db.deferred_manufacturer import DeferredManufacturer
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
    BinaryClassificationTypeEnum,
    ConceptTypeEnum,
    KeywordTypeEnum,
)

logger = logging.getLogger(__name__)


async def get_deferred_manufacturer_by_etld1_scraped_file_version(
    mfg_etld1: str,
    scraped_text_file_version_id: str,
) -> Optional[DeferredManufacturer]:
    return await DeferredManufacturer.find_one(
        DeferredManufacturer.etld1 == mfg_etld1,
        DeferredManufacturer.scraped_text_file_version_id
        == scraped_text_file_version_id,
    )


async def update_deferred_manufacturer(
    updated_at: datetime, deferred_manufacturer: DeferredManufacturer
):
    deferred_manufacturer.updated_at = updated_at
    logger.info(
        f"Saving deferred manufacturer {deferred_manufacturer.etld1} to the database."
    )
    await deferred_manufacturer.save()


def is_deferred_manufacturer_empty(deferred_manufacturer: DeferredManufacturer) -> bool:
    # check if each optional field is None
    return all(
        getattr(deferred_manufacturer, field) is None
        for field in [
            BinaryClassificationTypeEnum.is_manufacturer.name,
            # BinaryClassificationTypeEnum.is_contract_manufacturer.name,
            # BinaryClassificationTypeEnum.is_product_manufacturer.name,
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
        f"Deleting deferred manufacturer {deferred_manufacturer.etld1} from the database."
    )
    await deferred_manufacturer.delete()


async def delete_deferred_manufacturer_if_empty(
    deferred_manufacturer: DeferredManufacturer,
    # delete_batch_requests_too: bool,
):
    if is_deferred_manufacturer_empty(deferred_manufacturer):
        logger.info(
            f"✅✅✅ Deferred manufacturer {deferred_manufacturer.etld1} is empty. Deleting from the database."
        )
        await delete_deferred_manufacturer(deferred_manufacturer=deferred_manufacturer)
