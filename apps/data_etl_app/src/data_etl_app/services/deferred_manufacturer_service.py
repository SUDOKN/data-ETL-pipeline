from datetime import datetime
from typing import Optional
import logging

from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    DeferredLLMPhraseExtractionRequests,
)
from llm_providers.field_types import BatchRequestIDType
from core.models.deferred_extraction.deferred_single_stage_extraction_requests import (
    DeferredSingleStageExtractionRequests,
)
from core.models.deferred_extraction.deferred_keyword_extraction import (
    DeferredKeywordExtractionRequests,
)
from core.models.deferred_extraction.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
)
from data_etl_app.models.types_and_enums import (
    BinaryClassificationTypeEnum,
    ConceptTypeEnum,
    KeywordTypeEnum,
)
from data_etl_app.models.types_and_enums import (
    BasicFieldTypeEnum,
)
from data_etl_app.db_models.deferred_manufacturer import (
    DeferredManufacturer,
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


def get_embedded_gpt_request_ids(
    deferred_mfg: DeferredManufacturer,
) -> set[BatchRequestIDType]:
    custom_ids: set[BatchRequestIDType] = set()

    def fill_single_stage_ids(
        bin_field: Optional[DeferredSingleStageExtractionRequests],
    ):
        if bin_field:
            for (
                chunk_key,
                bundle,
            ) in bin_field.chunked_request_map.items():
                if bundle.llm_request_id:
                    custom_ids.add(bundle.llm_request_id)

    fill_single_stage_ids(deferred_mfg.is_manufacturer)
    fill_single_stage_ids(deferred_mfg.is_contract_manufacturer)
    fill_single_stage_ids(deferred_mfg.is_product_manufacturer)
    fill_single_stage_ids(deferred_mfg.addresses)
    fill_single_stage_ids(deferred_mfg.business_desc)

    def fill_common_llm_extraction_ids(
        extraction_field: Optional[DeferredLLMPhraseExtractionRequests],
    ):
        if extraction_field:
            for (
                chunk_key,
                bundle,
            ) in extraction_field.chunked_request_map.items():
                if bundle.llm_phrase_search_req_id:
                    custom_ids.add(bundle.llm_phrase_search_req_id)
                for recursive_req_id in bundle.llm_phrase_recursive_search_req_ids:
                    custom_ids.add(recursive_req_id)
                for relationship_req_id in bundle.llm_phrase_relationship_req_ids:
                    custom_ids.add(relationship_req_id)
                for (
                    screening_req_id
                ) in bundle.llm_phrase_relationship_screening_req_ids:
                    custom_ids.add(screening_req_id)

    def fill_keyword_ids(
        keyword_field: Optional[DeferredKeywordExtractionRequests],
    ):
        if keyword_field:
            for (
                chunk_key,
                bundle,
            ) in keyword_field.chunked_request_map.items():
                fill_common_llm_extraction_ids(keyword_field)
                for grounding_req_id in bundle.llm_phrase_freehand_grounding_req_ids:
                    custom_ids.add(grounding_req_id)

    fill_keyword_ids(deferred_mfg.products)
    fill_keyword_ids(deferred_mfg.equipments)
    fill_keyword_ids(deferred_mfg.contract_products)

    def fill_concept_ids(
        concept_field: Optional[DeferredConceptExtractionRequests],
    ):
        if concept_field:
            for (
                chunk_key,
                bundle,
            ) in concept_field.chunked_request_map.items():
                fill_common_llm_extraction_ids(concept_field)
                for grounding_req_id in bundle.llm_phrase_initial_grounding_req_ids:
                    custom_ids.add(grounding_req_id)
                if bundle.llm_phrase_recursive_tagging_reqs:
                    for _lvl, itrs in bundle.llm_phrase_recursive_tagging_reqs.items():
                        for itr in itrs:
                            custom_ids.add(itr.descend_req_id)

    fill_concept_ids(deferred_mfg.certificates)
    fill_concept_ids(deferred_mfg.industries)
    fill_concept_ids(deferred_mfg.process_caps)
    fill_concept_ids(deferred_mfg.material_caps)

    return custom_ids
