from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time — they're stored as strings automatically.

import json
import logging
from datetime import datetime

from core.models.prompt import Prompt
from core.models.field_types import (
    OntologyVersionIDType,
)

from open_ai_key_app.services.gpt_batch_request_service import (
    find_gpt_batch_request_by_mongo_id,
)

from data_etl_app.models.deferred_concept_extraction import (
    DeferredConceptExtraction,
    DeferredConceptExtractionStats,
    ConceptSearchBatchRequestBundle,
)
from data_etl_app.models.skos_concept import Concept
from data_etl_app.services.brute_search_service import brute_search
from data_etl_app.services.llm_powered.search.llm_search_service import (
    llm_search_deferred,
)
from data_etl_app.services.llm_powered.map.map_known_to_unknown_deferred_service import (
    map_known_to_unknown_deferred,
)
from data_etl_app.services.knowledge.ontology_service import get_ontology_service
from data_etl_app.services.knowledge.prompt_service import get_prompt_service
from data_etl_app.utils.chunk_util import (
    ChunkingStrat,
    get_chunks_respecting_line_boundaries,
)

logger = logging.getLogger(__name__)

from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)


async def add_certificate_mapping_requests_to_deferred_stats(
    deferred_at: datetime,
    deferred_stats: DeferredConceptExtractionStats,
    mfg_etld1: str,
) -> bool:
    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_certificates = ontology_service.certificates
    if deferred_stats.ontology_version_id != ontology_version_id:
        raise ValueError(
            f"add_certificate_mapping_requests_to_deferred_stats: Ontology version mismatch for {mfg_etld1}: deferred_stats.ontology_version_id={deferred_stats.ontology_version_id} != ontology_version_id={ontology_version_id}"
        )
    return await _add_mapping_requests_to_deferred_stats(
        deferred_at=deferred_at,
        deferred_stats=deferred_stats,
        concept_type="certificates",
        mfg_etld1=mfg_etld1,
        known_concepts=known_certificates,
        map_prompt=prompt_service.unknown_to_known_certificate_prompt,
    )


async def add_industry_mapping_requests_to_deferred_stats(
    deferred_at: datetime,
    deferred_stats: DeferredConceptExtractionStats,
    mfg_etld1: str,
) -> bool:
    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_industries = ontology_service.industries
    if deferred_stats.ontology_version_id != ontology_version_id:
        raise ValueError(
            f"add_industry_mapping_requests_to_deferred_stats: Ontology version mismatch for {mfg_etld1}: deferred_stats.ontology_version_id={deferred_stats.ontology_version_id} != ontology_version_id={ontology_version_id}"
        )
    return await _add_mapping_requests_to_deferred_stats(
        deferred_at=deferred_at,
        deferred_stats=deferred_stats,
        concept_type="industries",
        mfg_etld1=mfg_etld1,
        known_concepts=known_industries,
        map_prompt=prompt_service.unknown_to_known_industry_prompt,
    )


async def add_process_mapping_requests_to_deferred_stats(
    deferred_at: datetime,
    deferred_stats: DeferredConceptExtractionStats,
    mfg_etld1: str,
) -> bool:
    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_processes = ontology_service.process_caps
    if deferred_stats.ontology_version_id != ontology_version_id:
        raise ValueError(
            f"add_process_mapping_requests_to_deferred_stats: Ontology version mismatch for {mfg_etld1}: deferred_stats.ontology_version_id={deferred_stats.ontology_version_id} != ontology_version_id={ontology_version_id}"
        )
    return await _add_mapping_requests_to_deferred_stats(
        deferred_at=deferred_at,
        deferred_stats=deferred_stats,
        concept_type="processes",
        mfg_etld1=mfg_etld1,
        known_concepts=known_processes,
        map_prompt=prompt_service.unknown_to_known_process_cap_prompt,
    )


async def add_material_mapping_requests_to_deferred_stats(
    deferred_at: datetime,
    deferred_stats: DeferredConceptExtractionStats,
    mfg_etld1: str,
) -> bool:
    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_materials = ontology_service.material_caps
    if deferred_stats.ontology_version_id != ontology_version_id:
        raise ValueError(
            f"add_material_mapping_requests_to_deferred_stats: Ontology version mismatch for {mfg_etld1}: deferred_stats.ontology_version_id={deferred_stats.ontology_version_id} != ontology_version_id={ontology_version_id}"
        )
    return await _add_mapping_requests_to_deferred_stats(
        deferred_at=deferred_at,
        deferred_stats=deferred_stats,
        concept_type="materials",
        mfg_etld1=mfg_etld1,
        known_concepts=known_materials,
        map_prompt=prompt_service.unknown_to_known_material_cap_prompt,
    )


async def _add_mapping_requests_to_deferred_stats(
    deferred_at: datetime,
    deferred_stats: DeferredConceptExtractionStats,
    concept_type: str,  # used for logging and debugging
    mfg_etld1: str,
    known_concepts: list[Concept],  # DO NOT MUTATE
    map_prompt: Prompt,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> bool:
    updated = False
    for (
        chunk_bounds,
        chunk_batch_request_bundle,
    ) in deferred_stats.chunked_stats_batch_request_map.items():
        custom_id = f"{mfg_etld1}>{concept_type}>mapping>chunk>{chunk_bounds}"
        llm_gpt_batch_request = await find_gpt_batch_request_by_mongo_id(
            chunk_batch_request_bundle.llm_batch_request_id
        )
        if (
            llm_gpt_batch_request.response_blob
            and not chunk_batch_request_bundle.mapping_batch_request_id
        ):
            logger.info(
                f"add_mapping_requests_to_deferred_stats: Adding mapping request for chunk {chunk_bounds}"
            )
            try:
                gpt_response = (
                    llm_gpt_batch_request.response_blob.response.result.replace(
                        "```", ""
                    ).replace("json", "")
                )
                chunk_llm_results: set[str] = set(json.loads(gpt_response))
                unmapped_unknowns = chunk_llm_results - chunk_batch_request_bundle.brute
                # if not unmapped_unknowns:
                #     logger.info(
                #         f"add_mapping_requests_to_deferred_stats: No unmapped unknowns for chunk {chunk_bounds}, skipping mapping request"
                #     )
                #     continue
                chunk_batch_request_bundle.mapping_batch_request_id = (
                    await map_known_to_unknown_deferred(
                        deferred_at=deferred_at,
                        custom_id=custom_id,
                        known_concepts=known_concepts,
                        unmapped_unknowns=unmapped_unknowns,
                        prompt=map_prompt,
                        gpt_model=gpt_model,
                        model_params=model_params,
                    )
                )
                updated = True
            except:
                raise ValueError(
                    f"llm_results: Invalid response from GPT:{gpt_response}"
                )
        else:
            logger.debug(
                f"add_mapping_requests_to_deferred_stats: Skipping mapping request for custom_id={custom_id}"
                f" because llm.response_blob is None or mapping already exists"
            )

    return updated


async def extract_certificates_deferred(
    deferred_at: datetime,
    mfg_etld1: str,
    mfg_text: str,
) -> DeferredConceptExtraction:
    """
    Extract certificates for a manufacturer's text.
    """
    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_certificates = ontology_service.certificates
    return await _extract_concept_data_deferred(
        deferred_at,
        concept_type="certificates",
        mfg_etld1=mfg_etld1,
        text=mfg_text,
        ontology_version_id=ontology_version_id,
        known_concepts=known_certificates,
        search_prompt=prompt_service.extract_any_certificate_prompt,
        map_prompt=prompt_service.unknown_to_known_certificate_prompt,
        chunk_strategy=ChunkingStrat(overlap=0.0, max_tokens=7500),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def extract_industries_deferred(
    deferred_at: datetime,
    mfg_etld1: str,
    mfg_text: str,
) -> DeferredConceptExtraction:
    """
    Extract industries for a manufacturer's text.
    """
    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_industries = ontology_service.industries
    return await _extract_concept_data_deferred(
        deferred_at,
        concept_type="industries",
        mfg_etld1=mfg_etld1,
        text=mfg_text,
        ontology_version_id=ontology_version_id,
        known_concepts=known_industries,
        search_prompt=prompt_service.extract_any_industry_prompt,
        map_prompt=prompt_service.unknown_to_known_industry_prompt,
        chunk_strategy=ChunkingStrat(overlap=0.15, max_tokens=5000),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def extract_processes_deferred(
    deferred_at: datetime,
    mfg_etld1: str,
    mfg_text: str,
) -> DeferredConceptExtraction:
    """
    Extract processes for a manufacturer's text.
    """
    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_processes = ontology_service.process_caps
    return await _extract_concept_data_deferred(
        deferred_at,
        concept_type="processes",
        mfg_etld1=mfg_etld1,
        text=mfg_text,
        ontology_version_id=ontology_version_id,
        known_concepts=known_processes,
        search_prompt=prompt_service.extract_any_process_cap_prompt,
        map_prompt=prompt_service.unknown_to_known_process_cap_prompt,
        chunk_strategy=ChunkingStrat(overlap=0.15, max_tokens=2500),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def extract_materials_deferred(
    deferred_at: datetime,
    mfg_etld1: str,
    mfg_text: str,
) -> DeferredConceptExtraction:
    """
    Extract materials for a manufacturer's text.
    """
    prompt_service = await get_prompt_service()
    ontology_service = await get_ontology_service()
    ontology_version_id, known_materials = ontology_service.material_caps
    return await _extract_concept_data_deferred(
        deferred_at,
        concept_type="materials",
        mfg_etld1=mfg_etld1,
        text=mfg_text,
        ontology_version_id=ontology_version_id,
        known_concepts=known_materials,
        search_prompt=prompt_service.extract_any_material_cap_prompt,
        map_prompt=prompt_service.unknown_to_known_material_cap_prompt,
        chunk_strategy=ChunkingStrat(overlap=0.15, max_tokens=5000),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def _extract_concept_data_deferred(
    deferred_at: datetime,
    concept_type: str,  # used for logging and debugging
    mfg_etld1: str,
    text: str,
    ontology_version_id: OntologyVersionIDType,
    known_concepts: list[Concept],
    search_prompt: Prompt,
    map_prompt: Prompt,
    chunk_strategy: ChunkingStrat,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> DeferredConceptExtraction:
    logger.info(
        f"_extract_concept_data: Generating GPTBatchRequest for {mfg_etld1}:{concept_type}"
    )

    deferred_stats = DeferredConceptExtractionStats(
        extract_prompt_version_id=search_prompt.s3_version_id,
        map_prompt_version_id=map_prompt.s3_version_id,
        ontology_version_id=ontology_version_id,
        chunked_stats_batch_request_map={},
    )

    chunk_map = get_chunks_respecting_line_boundaries(
        text, chunk_strategy.max_tokens, chunk_strategy.overlap
    )

    for chunk_bounds, chunk_text in chunk_map.items():
        deferred_stats.chunked_stats_batch_request_map[chunk_bounds] = (
            ConceptSearchBatchRequestBundle(
                brute={b.name for b in brute_search(chunk_text, known_concepts)},
                llm_batch_request_id=await llm_search_deferred(
                    deferred_at=deferred_at,
                    custom_id=f"{mfg_etld1}>{concept_type}>llm_search>chunk>{chunk_bounds}",
                    text=chunk_text,
                    prompt=search_prompt,
                    gpt_model=gpt_model,
                    model_params=model_params,
                ),
                mapping_batch_request_id=None,  # to be filled after LLM response is received
            )
        )

    return DeferredConceptExtraction(deferred_stats=deferred_stats)
