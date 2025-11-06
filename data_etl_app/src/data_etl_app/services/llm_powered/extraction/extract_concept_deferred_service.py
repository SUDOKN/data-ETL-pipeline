from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time — they're stored as strings automatically.

import asyncio
import logging
from datetime import datetime
from typing import Optional

from core.models.prompt import Prompt
from core.models.field_types import (
    OntologyVersionIDType,
)

from data_etl_app.models.types_and_enums import ConceptTypeEnum
from core.models.db.gpt_batch_request import GPTBatchRequest
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from core.models.gpt_batch_request_blob import (
    GPTBatchRequestBlob,
    GPTBatchRequestBlobBody,
)
from core.models.gpt_batch_response_blob import (
    GPTBatchResponseBlob,
    GPTBatchResponseBody,
    GPTResponseBlobBody,
    GPTBatchResponseBlobUsage,
    GPTBatchResponseBlobChoice,
    GPTBatchResponseBlobChoiceMessage,
)

from core.models.deferred_concept_extraction import (
    DeferredConceptExtraction,
    ConceptExtractionBundle,
)
from data_etl_app.models.skos_concept import Concept
from data_etl_app.services.brute_search_service import brute_search
from data_etl_app.services.llm_powered.extraction.extract_concept_service import (
    get_matched_concepts_and_unmatched_keywords_by_concept_type,
)
from data_etl_app.services.llm_powered.map.map_known_to_unknown_deferred_service import (
    map_known_to_unknown_deferred,
)
from data_etl_app.services.llm_powered.search.llm_search_service import (
    parse_llm_search_response,
)
from data_etl_app.services.knowledge.ontology_service import get_ontology_service
from data_etl_app.services.knowledge.prompt_service import get_prompt_service
from data_etl_app.utils.chunk_util import (
    ChunkingStrat,
    get_chunks_respecting_line_boundaries,
)
from core.services.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    find_completed_gpt_batch_requests_by_custom_ids,
    find_gpt_batch_request_ids_only,
)

logger = logging.getLogger(__name__)

from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)


async def get_missing_concept_mapping_request(
    deferred_at: datetime,
    deferred_concept_extraction: DeferredConceptExtraction,
    mfg_etld1: str,
    concept_type: ConceptTypeEnum,
) -> tuple[DeferredConceptExtraction, GPTBatchRequest]:
    """Add mapping requests for the given concept type to the deferred concept extraction stats."""
    mapping_requests_functions = {
        ConceptTypeEnum.certificates: get_missing_certificate_mapping_request,
        ConceptTypeEnum.industries: get_missing_industry_mapping_request,
        ConceptTypeEnum.process_caps: get_missing_process_mapping_request,
        ConceptTypeEnum.material_caps: get_missing_material_mapping_request,
    }

    mapping_function = mapping_requests_functions.get(concept_type)
    if not mapping_function:
        raise ValueError(f"Unsupported concept type: {concept_type}")

    return await mapping_function(
        deferred_at=deferred_at,
        deferred_concept_extraction=deferred_concept_extraction,
        mfg_etld1=mfg_etld1,
    )


async def get_missing_certificate_mapping_request(
    deferred_at: datetime,
    deferred_concept_extraction: DeferredConceptExtraction,
    mfg_etld1: str,
) -> tuple[DeferredConceptExtraction, GPTBatchRequest]:
    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_certificates = ontology_service.certificates
    if deferred_concept_extraction.ontology_version_id != ontology_version_id:
        raise ValueError(
            f"add_certificate_mapping_requests_to_deferred_stats: Ontology version mismatch for {mfg_etld1}: deferred_concept_extraction.ontology_version_id={deferred_concept_extraction.ontology_version_id} != ontology_version_id={ontology_version_id}"
        )
    return await _get_missing_mapping_request(
        deferred_at=deferred_at,
        deferred_concept_extraction=deferred_concept_extraction,
        concept_type=ConceptTypeEnum.certificates,
        mfg_etld1=mfg_etld1,
        known_concepts=known_certificates,
        map_prompt=prompt_service.unknown_to_known_certificate_prompt,
    )


async def get_missing_industry_mapping_request(
    deferred_at: datetime,
    deferred_concept_extraction: DeferredConceptExtraction,
    mfg_etld1: str,
) -> tuple[DeferredConceptExtraction, GPTBatchRequest]:
    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_industries = ontology_service.industries
    if deferred_concept_extraction.ontology_version_id != ontology_version_id:
        raise ValueError(
            f"add_industry_mapping_requests_to_deferred_stats: Ontology version mismatch for {mfg_etld1}: deferred_concept_extraction.ontology_version_id={deferred_concept_extraction.ontology_version_id} != ontology_version_id={ontology_version_id}"
        )
    return await _get_missing_mapping_request(
        deferred_at=deferred_at,
        deferred_concept_extraction=deferred_concept_extraction,
        concept_type=ConceptTypeEnum.industries,
        mfg_etld1=mfg_etld1,
        known_concepts=known_industries,
        map_prompt=prompt_service.unknown_to_known_industry_prompt,
    )


async def get_missing_process_mapping_request(
    deferred_at: datetime,
    deferred_concept_extraction: DeferredConceptExtraction,
    mfg_etld1: str,
) -> tuple[DeferredConceptExtraction, GPTBatchRequest]:
    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_processes = ontology_service.process_caps
    if deferred_concept_extraction.ontology_version_id != ontology_version_id:
        raise ValueError(
            f"add_process_mapping_requests_to_deferred_stats: Ontology version mismatch for {mfg_etld1}: deferred_concept_extraction.ontology_version_id={deferred_concept_extraction.ontology_version_id} != ontology_version_id={ontology_version_id}"
        )
    return await _get_missing_mapping_request(
        deferred_at=deferred_at,
        deferred_concept_extraction=deferred_concept_extraction,
        concept_type=ConceptTypeEnum.process_caps,
        mfg_etld1=mfg_etld1,
        known_concepts=known_processes,
        map_prompt=prompt_service.unknown_to_known_process_cap_prompt,
    )


async def get_missing_material_mapping_request(
    deferred_at: datetime,
    deferred_concept_extraction: DeferredConceptExtraction,
    mfg_etld1: str,
) -> tuple[DeferredConceptExtraction, GPTBatchRequest]:
    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_materials = ontology_service.material_caps
    if deferred_concept_extraction.ontology_version_id != ontology_version_id:
        raise ValueError(
            f"add_material_mapping_requests_to_deferred_stats: Ontology version mismatch for {mfg_etld1}: deferred_concept_extraction.ontology_version_id={deferred_concept_extraction.ontology_version_id} != ontology_version_id={ontology_version_id}"
        )
    return await _get_missing_mapping_request(
        deferred_at=deferred_at,
        deferred_concept_extraction=deferred_concept_extraction,
        concept_type=ConceptTypeEnum.material_caps,
        mfg_etld1=mfg_etld1,
        known_concepts=known_materials,
        map_prompt=prompt_service.unknown_to_known_material_cap_prompt,
    )


async def _get_missing_mapping_request(
    deferred_at: datetime,
    deferred_concept_extraction: DeferredConceptExtraction,
    concept_type: ConceptTypeEnum,  # used for logging and debugging
    mfg_etld1: str,
    known_concepts: set[Concept],  # DO NOT MUTATE
    map_prompt: Prompt,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> tuple[DeferredConceptExtraction, GPTBatchRequest]:
    logger.info(
        f"_add_mapping_requests_to_deferred_stats: Generating GPTBatchRequest for {mfg_etld1}:{concept_type}"
    )
    if (
        not deferred_concept_extraction.map_prompt_version_id
        == map_prompt.s3_version_id
    ):
        raise ValueError(
            f"_extract_concept_data_deferred: Prompt version mismatch for {mfg_etld1}:{concept_type}, deferred_concept_extraction.map_prompt_version_id={deferred_concept_extraction.map_prompt_version_id} != map_prompt.s3_version_id={map_prompt.s3_version_id}"
        )

    new_batch_request: GPTBatchRequest | None = None

    if deferred_concept_extraction.llm_mapping_request_id is None:
        deferred_concept_extraction.llm_mapping_request_id = (
            f"{mfg_etld1}>{concept_type.name}>mapping"
        )

    # Create lookup map: custom_id -> GPTBatchRequest
    llm_search_gpt_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest] = (
        await find_completed_gpt_batch_requests_by_custom_ids(
            [
                extraction_bundle.llm_search_request_id
                for extraction_bundle in deferred_concept_extraction.chunk_request_bundle_map.values()
            ]
        )
    )

    if len(llm_search_gpt_request_map) != len(
        deferred_concept_extraction.chunk_request_bundle_map
    ):
        raise ValueError(
            f"_get_missing_mapping_request_to_deferred_stats: Inconsistent number of GPTBatchRequests for {mfg_etld1}:{concept_type}"
        )

    llm_search_results: set[str] = set()
    for llm_search_req in llm_search_gpt_request_map.values():
        assert (  # should be ensured by find_completed_gpt_batch_requests_by_custom_ids
            llm_search_req.response_blob is not None
        ), f"Missing response_blob for {llm_search_req.request.custom_id}"
        llm_search_results |= parse_llm_search_response(
            llm_search_req.response_blob.result
        )

    _matched_concepts, unmatched_keywords = (
        await get_matched_concepts_and_unmatched_keywords_by_concept_type(
            concept_type, llm_search_results
        )
    )

    if not unmatched_keywords:
        # add a dummy response blob with empty dict
        # Note: Use valid schema values to avoid MongoDB validation errors
        logger.info(
            f"All concepts matched via brute search for {mfg_etld1}:{concept_type}, creating dummy mapping request"
        )
        dummy_batch_request = _get_dummy_completed_batch_request(
            deferred_at=deferred_at,
            llm_mapping_request_id=deferred_concept_extraction.llm_mapping_request_id,
        )
        new_batch_request = dummy_batch_request
    else:
        mapping_batch_request = map_known_to_unknown_deferred(
            deferred_at=deferred_at,
            llm_mapping_req_id=deferred_concept_extraction.llm_mapping_request_id,
            known_concepts=known_concepts,
            unmatched_keywords=unmatched_keywords,
            mapping_prompt=map_prompt,
            gpt_model=gpt_model,
            model_params=model_params,
        )
        new_batch_request = mapping_batch_request

    return deferred_concept_extraction, new_batch_request


def _get_dummy_completed_batch_request(
    deferred_at: datetime, llm_mapping_request_id: GPTBatchRequestCustomID
) -> GPTBatchRequest:
    if llm_mapping_request_id is None:
        raise ValueError("_get_dummy_batch_request: llm_mapping_request_id is None")

    return GPTBatchRequest(
        created_at=deferred_at,
        request=GPTBatchRequestBlob(
            custom_id=llm_mapping_request_id,
            body=GPTBatchRequestBlobBody(
                model="basic_logic",
                messages=[
                    {
                        "role": "system",
                        "content": "No mapping needed - all concepts matched via brute search",
                    }
                ],
                input_tokens=1,
                max_tokens=1,
            ),
        ),
        batch_id="empty_unmapped_unknowns",
        response_blob=GPTBatchResponseBlob(
            batch_id="empty_unmapped_unknowns",
            request_custom_id="no-request",
            response=GPTBatchResponseBody(
                status_code=200,
                body=GPTResponseBlobBody(
                    created=deferred_at,
                    choices=[
                        GPTBatchResponseBlobChoice(
                            index=0,
                            message=GPTBatchResponseBlobChoiceMessage(
                                role="assistant", content="```json\n{}\n```"
                            ),
                        )
                    ],
                    usage=GPTBatchResponseBlobUsage(
                        prompt_tokens=1,
                        completion_tokens=1,
                        total_tokens=2,
                    ),
                ),
            ),
        ),
    )


async def get_missing_concept_search_requests(
    deferred_at: datetime,
    concept_type: ConceptTypeEnum,
    deferred_concept_extraction: Optional[DeferredConceptExtraction],
    mfg_etld1: str,
    mfg_text: str,
) -> tuple[DeferredConceptExtraction, list[GPTBatchRequest]]:
    """
    Dispatcher function that calls the appropriate extraction function based on concept type.
    """

    extraction_map = {
        ConceptTypeEnum.certificates: get_missing_certificate_search_requests,
        ConceptTypeEnum.industries: get_missing_industry_search_requests,
        ConceptTypeEnum.process_caps: get_missing_process_search_requests,
        ConceptTypeEnum.material_caps: get_missing_material_search_requests,
    }

    extractor = extraction_map.get(concept_type)
    if not extractor:
        raise ValueError(f"Unsupported concept type: {concept_type}")

    return await extractor(
        deferred_at=deferred_at,
        deferred_concept_extraction=deferred_concept_extraction,
        mfg_etld1=mfg_etld1,
        mfg_text=mfg_text,
    )


async def get_missing_certificate_search_requests(
    deferred_at: datetime,
    deferred_concept_extraction: Optional[DeferredConceptExtraction],
    mfg_etld1: str,
    mfg_text: str,
) -> tuple[DeferredConceptExtraction, list[GPTBatchRequest]]:
    """
    Extract certificates for a manufacturer's text.
    """

    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_certificates = ontology_service.certificates

    return await _get_missing_concept_search_requests(
        deferred_at,
        concept_type=ConceptTypeEnum.certificates,
        deferred_concept_extraction=deferred_concept_extraction,
        mfg_etld1=mfg_etld1,
        mfg_text=mfg_text,
        ontology_version_id=ontology_version_id,
        known_concepts=known_certificates,
        search_prompt=prompt_service.extract_any_certificate_prompt,
        map_prompt=prompt_service.unknown_to_known_certificate_prompt,
        chunk_strategy=ChunkingStrat(overlap=0.0, max_tokens=7500),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def get_missing_industry_search_requests(
    deferred_at: datetime,
    deferred_concept_extraction: Optional[DeferredConceptExtraction],
    mfg_etld1: str,
    mfg_text: str,
) -> tuple[DeferredConceptExtraction, list[GPTBatchRequest]]:
    """
    Extract industries for a manufacturer's text.
    """
    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_industries = ontology_service.industries

    return await _get_missing_concept_search_requests(
        deferred_at,
        concept_type=ConceptTypeEnum.industries,
        deferred_concept_extraction=deferred_concept_extraction,
        mfg_etld1=mfg_etld1,
        mfg_text=mfg_text,
        ontology_version_id=ontology_version_id,
        known_concepts=known_industries,
        search_prompt=prompt_service.extract_any_industry_prompt,
        map_prompt=prompt_service.unknown_to_known_industry_prompt,
        chunk_strategy=ChunkingStrat(overlap=0.15, max_tokens=5000),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def get_missing_process_search_requests(
    deferred_at: datetime,
    deferred_concept_extraction: Optional[DeferredConceptExtraction],
    mfg_etld1: str,
    mfg_text: str,
) -> tuple[DeferredConceptExtraction, list[GPTBatchRequest]]:
    """
    Extract processes for a manufacturer's text.
    """

    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_processes = ontology_service.process_caps

    return await _get_missing_concept_search_requests(
        deferred_at,
        concept_type=ConceptTypeEnum.process_caps,
        deferred_concept_extraction=deferred_concept_extraction,
        mfg_etld1=mfg_etld1,
        mfg_text=mfg_text,
        ontology_version_id=ontology_version_id,
        known_concepts=known_processes,
        search_prompt=prompt_service.extract_any_process_cap_prompt,
        map_prompt=prompt_service.unknown_to_known_process_cap_prompt,
        chunk_strategy=ChunkingStrat(overlap=0.15, max_tokens=2500),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def get_missing_material_search_requests(
    deferred_at: datetime,
    deferred_concept_extraction: Optional[DeferredConceptExtraction],
    mfg_etld1: str,
    mfg_text: str,
) -> tuple[DeferredConceptExtraction, list[GPTBatchRequest]]:
    """
    Extract materials for a manufacturer's text.
    """

    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_materials = ontology_service.material_caps

    return await _get_missing_concept_search_requests(
        deferred_at,
        concept_type=ConceptTypeEnum.material_caps,
        deferred_concept_extraction=deferred_concept_extraction,
        mfg_etld1=mfg_etld1,
        mfg_text=mfg_text,
        ontology_version_id=ontology_version_id,
        known_concepts=known_materials,
        search_prompt=prompt_service.extract_any_material_cap_prompt,
        map_prompt=prompt_service.unknown_to_known_material_cap_prompt,
        chunk_strategy=ChunkingStrat(overlap=0.15, max_tokens=5000),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def _get_missing_concept_search_requests(
    deferred_at: datetime,
    concept_type: ConceptTypeEnum,  # used for logging and debugging
    deferred_concept_extraction: Optional[DeferredConceptExtraction],
    mfg_etld1: str,
    mfg_text: str,
    known_concepts: set[Concept],
    ontology_version_id: OntologyVersionIDType,
    search_prompt: Prompt,
    map_prompt: Prompt,
    chunk_strategy: ChunkingStrat,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> tuple[DeferredConceptExtraction, list[GPTBatchRequest]]:

    logger.info(
        f"_get_missing_concept_search_requests: Generating GPTBatchRequest for {mfg_etld1}:{concept_type}"
    )

    batch_requests: list[GPTBatchRequest] = []

    if not deferred_concept_extraction:
        # if not, create fresh DeferredConceptExtraction
        deferred_concept_extraction = DeferredConceptExtraction(
            extract_prompt_version_id=search_prompt.s3_version_id,
            map_prompt_version_id=map_prompt.s3_version_id,
            ontology_version_id=ontology_version_id,
            llm_mapping_request_id=None,
            chunk_request_bundle_map={},
        )

        # expensive operation for large texts
        chunk_map = await get_chunks_respecting_line_boundaries(
            mfg_text, chunk_strategy.max_tokens, chunk_strategy.overlap
        )
        chunk_items = list(chunk_map.items())

    else:
        if not deferred_concept_extraction.ontology_version_id == ontology_version_id:
            raise ValueError(
                f"_extract_concept_data_deferred: Ontology version mismatch for {mfg_etld1}:{concept_type}, deferred_concept_extraction.ontology_version_id={deferred_concept_extraction.ontology_version_id} != ontology_version_id={ontology_version_id}"
            )
        if (
            not deferred_concept_extraction.extract_prompt_version_id
            == search_prompt.s3_version_id
        ):
            raise ValueError(
                f"_extract_concept_data_deferred: Prompt version mismatch for {mfg_etld1}:{concept_type}, deferred_concept_extraction.extract_prompt_version_id={deferred_concept_extraction.extract_prompt_version_id} != search_prompt.s3_version_id={search_prompt.s3_version_id}"
            )
        if (
            not deferred_concept_extraction.map_prompt_version_id
            == map_prompt.s3_version_id
        ):
            raise ValueError(
                f"_extract_concept_data_deferred: Prompt version mismatch for {mfg_etld1}:{concept_type}, deferred_concept_extraction.map_prompt_version_id={deferred_concept_extraction.map_prompt_version_id} != map_prompt.s3_version_id={map_prompt.s3_version_id}"
            )

        # if yes lookup all chunk batch requests IDs inside chunk_request_id_map
        llm_search_req_ids_to_lookup = set()

        # Check if all search request exist and have batch_id
        for (
            _chunk_bounds,
            extraction_bundle,
        ) in deferred_concept_extraction.chunk_request_bundle_map.items():
            llm_search_req_ids_to_lookup.add(extraction_bundle.llm_search_request_id)

        gpt_req_ids_missing = llm_search_req_ids_to_lookup - (
            await find_gpt_batch_request_ids_only(list(llm_search_req_ids_to_lookup))
        )

        # create chunk_items only for missing batch requests
        chunk_items = []
        for (
            chunk_bounds,
            extraction_bundle,
        ) in deferred_concept_extraction.chunk_request_bundle_map.items():
            if extraction_bundle.llm_search_request_id in gpt_req_ids_missing:
                start = chunk_bounds.split(":")[0]
                end = chunk_bounds.split(":")[1]
                chunk_items.append((chunk_bounds, mfg_text[int(start) : int(end)]))

    # Process chunks in batches to yield control periodically
    BATCH_SIZE = 100  # Process 100 chunks at a time

    for i in range(0, len(chunk_items), BATCH_SIZE):
        batch = chunk_items[i : i + BATCH_SIZE]

        # Process current batch
        for chunk_bounds, chunk_text in batch:
            llm_batch_request = create_base_gpt_batch_request(
                deferred_at=deferred_at,
                custom_id=f"{mfg_etld1}>{concept_type.name}>llm_search>chunk>{chunk_bounds}",
                context=chunk_text,
                prompt=search_prompt,
                gpt_model=gpt_model,
                model_params=model_params,
            )

            batch_requests.append(llm_batch_request)
            deferred_concept_extraction.chunk_request_bundle_map[chunk_bounds] = (
                ConceptExtractionBundle(
                    brute={b.name for b in brute_search(chunk_text, known_concepts)},
                    llm_search_request_id=llm_batch_request.request.custom_id,
                )
            )

        # Yield control to event loop after each batch
        await asyncio.sleep(0)

        if (i + BATCH_SIZE) % 500 == 0:
            logger.info(
                f"Created {min(i + BATCH_SIZE, len(chunk_items))}/{len(chunk_items)} "
                f"gpt request for {mfg_etld1}:{concept_type}"
            )

    return (
        deferred_concept_extraction,
        batch_requests,
    )
