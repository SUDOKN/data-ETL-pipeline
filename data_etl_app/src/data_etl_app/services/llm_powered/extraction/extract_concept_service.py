from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time — they're stored as strings automatically.
import asyncio
import logging
from datetime import datetime

from core.models.prompt import Prompt
from core.models.field_types import (
    OntologyVersionIDType,
)

from data_etl_app.models.concept_extraction_results import (
    ConceptSearchChunkStats,
    ConceptExtractionStats,
    ConceptExtractionResults,
)
from data_etl_app.models.skos_concept import Concept
from data_etl_app.services.llm_powered.map.map_known_to_unknown_service import (
    mapKnownToUnknown,
)
from data_etl_app.services.brute_search_service import brute_search
from data_etl_app.services.llm_powered.search.llm_search_service import llm_search
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


async def extract_certificates(
    extraction_timestamp: datetime,
    mfg_etld1: str,
    text: str,
) -> ConceptExtractionResults:
    """
    Extract certificates for a manufacturer text.
    """
    ontology_service = await get_ontology_service()
    prompt_service = await get_prompt_service()
    ontology_version_id, known_certificates = ontology_service.certificates
    return await _extract_concept_data(
        extraction_timestamp,
        "certificates",
        mfg_etld1,
        text,
        ontology_version_id,
        known_certificates,
        prompt_service.extract_any_certificate_prompt,
        prompt_service.unknown_to_known_certificate_prompt,
        ChunkingStrat(overlap=0.0, max_tokens=7500),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def extract_industries(
    extraction_timestamp: datetime,
    mfg_etld1: str,
    text: str,
) -> ConceptExtractionResults:
    """
    Extract industries for a manufacturer text.
    """
    ontology_service = await get_ontology_service()
    prompt_service = await get_prompt_service()
    ontology_version_id, known_industries = ontology_service.industries
    return await _extract_concept_data(
        extraction_timestamp,
        "industries",
        mfg_etld1,
        text,
        ontology_version_id,
        known_industries,
        prompt_service.extract_any_industry_prompt,
        prompt_service.unknown_to_known_industry_prompt,
        ChunkingStrat(overlap=0.15, max_tokens=5000),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def extract_processes(
    extraction_timestamp: datetime,
    mfg_etld1: str,
    text: str,
) -> ConceptExtractionResults:
    """
    Extract process capabilities for a manufacturer text.
    """
    ontology_service = await get_ontology_service()
    prompt_service = await get_prompt_service()
    ontology_version_id, known_processes = ontology_service.process_caps
    return await _extract_concept_data(
        extraction_timestamp,
        "process_caps",
        mfg_etld1,
        text,
        ontology_version_id,
        known_processes,
        prompt_service.extract_any_process_cap_prompt,
        prompt_service.unknown_to_known_process_cap_prompt,
        ChunkingStrat(overlap=0.15, max_tokens=2500),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def extract_materials(
    extraction_timestamp: datetime,
    mfg_etld1: str,
    text: str,
) -> ConceptExtractionResults:
    """
    Extract material capabilities for a manufacturer text.
    """
    ontology_service = await get_ontology_service()
    prompt_service = await get_prompt_service()
    ontology_version_id, known_materials = ontology_service.material_caps
    return await _extract_concept_data(
        extraction_timestamp,
        "material_caps",
        mfg_etld1,
        text,
        ontology_version_id,
        known_materials,
        prompt_service.extract_any_material_cap_prompt,
        prompt_service.unknown_to_known_material_cap_prompt,
        ChunkingStrat(overlap=0.1, max_tokens=5000),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def _extract_concept_data(
    extraction_timestamp: datetime,
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
) -> ConceptExtractionResults:
    logger.debug(
        f"Extracting {concept_type} for {mfg_etld1} at {extraction_timestamp} with ontology version {ontology_version_id}"
    )

    results = set[str]()
    stats: ConceptExtractionStats = ConceptExtractionStats(
        extract_prompt_version_id=search_prompt.s3_version_id,
        map_prompt_version_id=map_prompt.s3_version_id,
        ontology_version_id=ontology_version_id,
        mapping={},
        chunked_stats={},
        unmapped_llm=[],
    )

    """
    BRUTE KEYWORD EXTRACTION ---------------------------------------------------- #
    step1: brute force
    pros:
        1. doesn't miss
    cons:
        1. limited to known vocabulary
        2. can have false positives because :
        - ignores surrounding context
        - relies on the fact that keywords are so specific, it's unlikely they are used in irrelevant/incorrect context)
    
    KEYWORD EXTRACTION ---------------------------------------------------------- #
    step2: ask LLM (without passing vocabulary)
    pros:
        1. considers surrounding context
    cons:
        1. can hallucinate false positives (unlikely for small texts)
        2. multiple passes may be required to get everything (soln: increase num_passes)
    """

    chunk_map = get_chunks_respecting_line_boundaries(
        text, chunk_strategy.max_tokens, chunk_strategy.overlap
    )

    # Run brute_search and llm_search for each chunk concurrently
    async def _process_chunk(bounds: str, text_chunk: str):
        # NOTE: doing brute search individually for each chunk is more expensive than all at once
        # and is computationally expensive in general, but we need chunk level results
        brute_set = brute_search(text_chunk, known_concepts)
        llm_set = await llm_search(
            text_chunk, search_prompt.text, gpt_model, model_params, True
        )
        return bounds, brute_set, llm_set

    tasks = [asyncio.create_task(_process_chunk(b, t)) for b, t in chunk_map.items()]
    chunk_results = await asyncio.gather(*tasks)

    # orphan_brutes: set[Concept] = set()
    unmapped_llm: set[str] = set()
    mutually_agreed_concepts: set[Concept] = set()

    for bounds, brute_set, llm_set in chunk_results:
        stats.chunked_stats[bounds] = ConceptSearchChunkStats(
            results=set(),  # will be filled later
            brute={b.name for b in brute_set},
            llm=llm_set.copy(),
            mapping={},  # will be filled later after llm produces full mapping
            unmapped_llm=set(),
        )

        # add MUTUALLY AGREED to results
        for kc in known_concepts:
            common = kc.matchLabels & llm_set
            if common:
                stats.chunked_stats[bounds].results.add(kc.name)
                mutually_agreed_concepts.add(kc)
                llm_set -= common

        # recalculate orphan brute and llm sets
        # orphan_brutes |= brute_set - mutually_agreed_concepts
        unmapped_llm |= llm_set

    results = {
        c.name for c in mutually_agreed_concepts
    }  # add mutually agreed concept labels to results

    """
    UNKNOWN TO KNOWN MAPPING --------------------------------------------------- #
    step3: map unmapped_brute_labels and (flat_knowns - unmapped_brute_labels) to orphan_llm (necessary)
    pros:
        1. discover implied keywords
        1. discovers new altLabels for old keywords
        2. discovers new out-of-vocab
    cons:
        1. may discard true positives (highly unlikely, can be solved by inc num_passes and keeping intersections)
    """
    map_results = await mapKnownToUnknown(
        concept_type,
        mfg_etld1,
        known_concepts,
        unmapped_llm.copy(),
        map_prompt.text,
    )

    # UPDATE unmapped_llm and mapping in chunk_stats
    final_unmapped_llm = map_results[
        "unmapped_unknowns"
    ]  # not needed if we don't pass unmapped_llm.copy()

    for _, chunk_stats in stats.chunked_stats.items():
        chunk_stats.unmapped_llm = final_unmapped_llm & chunk_stats.llm
        for mk, mu in map_results["known_to_unknowns"].items():
            # find all elements in mu that are also in chunk_level unmapped_llm
            # insert mk as key and those elements as value

            chunk_mu = [unknown for unknown in mu if unknown in chunk_stats.llm]
            # NOTE: it is a gurantee that unknown belongs to unmapped_llm because of mapKnownToUnknown logic
            # so we need not find the subset unmapped_unknowns in chunk_stats.llm,
            # we can directly use chunk_stats.llm for filtering
            if chunk_mu:
                chunk_stats.results.add(mk.name)
                chunk_stats.mapping[mk.name] = chunk_mu

    # UPDATE results with newly mapped known concept labels
    mapped_known_concept_labels = set(
        concept.name for concept in map_results["known_to_unknowns"].keys()
    )
    results |= mapped_known_concept_labels  # union because overlap is expected

    logger.debug(f"Remaining orphan llm:")
    logger.debug(f"final_unmapped_llm {len(final_unmapped_llm)}:{final_unmapped_llm}")

    stats.unmapped_llm = list(final_unmapped_llm)
    stats.mapping = {mk.name: mu for mk, mu in map_results["known_to_unknowns"].items()}

    return ConceptExtractionResults(
        extracted_at=extraction_timestamp,
        results=list(results),
        stats=stats,
    )
