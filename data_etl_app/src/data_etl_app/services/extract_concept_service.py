from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time — they're stored as strings automatically.
import asyncio
import logging
from datetime import datetime

from shared.models.types import (
    OntologyVersionIDType,
)
from shared.models.db.extraction_results import (
    ChunkSearchStats,
    ExtractionStats,
    ExtractionResults,
)

from data_etl_app.models.skos_concept import Concept

from data_etl_app.services.map_unknown_to_known_service import mapKnownToUnknown
from data_etl_app.services.brute_search_service import brute_search
from data_etl_app.services.llm_search_service import llm_search
from data_etl_app.services.ontology_service import ontology_service
from data_etl_app.services.prompt_service import prompt_service

from data_etl_app.utils.chunk_util import (
    ChunkingStrat,
    get_chunks_with_boundaries,
)

logger = logging.getLogger(__name__)

from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)


async def extract_industries(
    extraction_timestamp: datetime,
    manufacturer_url: str,
    text: str,
) -> ExtractionResults:
    """
    Extract industries for a manufacturer text.
    """
    ontology_version_id, known_industries = ontology_service.industries
    return await _extract_concept_data(
        extraction_timestamp,
        "industries",
        manufacturer_url,
        text,
        ontology_version_id,
        known_industries,
        prompt_service.extract_industry_prompt,
        prompt_service.unknown_to_known_industry_prompt,
        ChunkingStrat(overlap=0.15, max_tokens=5000),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def extract_certificates(
    extraction_timestamp: datetime,
    manufacturer_url: str,
    text: str,
) -> ExtractionResults:
    """
    Extract certificates for a manufacturer text.
    """
    ontology_version_id, known_certificates = ontology_service.certificates
    return await _extract_concept_data(
        extraction_timestamp,
        "certificates",
        manufacturer_url,
        text,
        ontology_version_id,
        known_certificates,
        prompt_service.extract_certificate_prompt,
        prompt_service.unknown_to_known_certificate_prompt,
        ChunkingStrat(overlap=0.0, max_tokens=7500),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def extract_processes(
    extraction_timestamp: datetime,
    manufacturer_url: str,
    text: str,
) -> ExtractionResults:
    """
    Extract process capabilities for a manufacturer text.
    """
    ontology_version_id, known_processes = ontology_service.process_caps
    return await _extract_concept_data(
        extraction_timestamp,
        "process_caps",
        manufacturer_url,
        text,
        ontology_version_id,
        known_processes,
        prompt_service.extract_process_prompt,
        prompt_service.unknown_to_known_process_prompt,
        ChunkingStrat(overlap=0.15, max_tokens=2500),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )


async def extract_materials(
    extraction_timestamp: datetime,
    manufacturer_url: str,
    text: str,
) -> ExtractionResults:
    """
    Extract material capabilities for a manufacturer text.
    """
    ontology_version_id, known_materials = ontology_service.material_caps
    return await _extract_concept_data(
        extraction_timestamp,
        "material_caps",
        manufacturer_url,
        text,
        ontology_version_id,
        known_materials,
        prompt_service.extract_material_prompt,
        prompt_service.unknown_to_known_material_prompt,
        ChunkingStrat(overlap=0.1, max_tokens=5000),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )

async def extract_products(   extraction_timestamp: datetime,
    manufacturer_url: str,
    text: str,
) -> ExtractionResults:
    """
    Extract products for a manufacturer's text.
    """
    ontology_version_id, known_products = ontology_service.products
    return await _extract_free_range_concept_data(
        extraction_timestamp,
        "products",
        manufacturer_url,
        text,
        ontology_version_id,
        prompt_service.extract_product_prompt,
        ChunkingStrat(overlap=0.15, max_tokens=5000),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
    )

async def _extract_free_range_concept_data( extraction_timestamp: datetime,
    concept_type: str,  # used for logging/debug
    manufacturer_url: str,
    text: str,
    ontology_version_id: OntologyVersionIDType,
    search_prompt: str,
    chunk_strategy: ChunkingStrat,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> ExtractionResults:
    logger.info(
        f"Extracting {concept_type} (NO BRUTE) for {manufacturer_url} at {extraction_timestamp} "
        f"with ontology version {ontology_version_id}"
    )

    # 1) Chunk
    chunk_map = get_chunks_with_boundaries(
        text, chunk_strategy.max_tokens, chunk_strategy.overlap
    )

    # 2) LLM search per chunk (no brute)
    async def _process_chunk(bounds: str, text_chunk: str):
        llm_set = await llm_search(
            text_chunk, search_prompt, gpt_model, model_params, True  # dedupe/normalize
        )
        return bounds, llm_set

    tasks = [asyncio.create_task(_process_chunk(b, t)) for b, t in chunk_map.items()]
    chunk_results = await asyncio.gather(*tasks)

    # Prepare stats
    stats = ExtractionStats(
        ontology_version_id=ontology_version_id,
        mapping={},       # global mapping filled later
        search={},        # per-chunk stats
        unmapped_llm=[],  # global residual filled later
    )

    # Track all unknowns to feed the mapping stage
    global_llm_unknowns: set[str] = set()

    # Seed per-chunk stats (brute is always empty)
    for bounds, llm_set in chunk_results:
        stats.search[bounds] = ChunkSearchStats(
            results=set(llm_set),  # Put LLM results directly here
            brute=set(),  # no brute in this extractor
            llm=set(llm_set),  # raw llm labels seen in this chunk
            mapping={},  # no mapping in free range
            unmapped_llm=set()  # no unmapped since we keep all results
        )
        global_llm_unknowns |= llm_set



    # 3) Aggregate final results and global fields
    final_results: set[str] = global_llm_unknowns
    stats.unmapped_llm = []
    stats.mapping = {}

    logger.info(
        f"[{concept_type}] NO-BRUTE unmapped_unknowns=0; "
        f"raw_results={len(final_results)}"
    )

    return ExtractionResults(
        extracted_at=extraction_timestamp,
        results=list(final_results),
        stats=stats,
    )

async def _extract_concept_data(
    extraction_timestamp: datetime,
    concept_type: str,  # used for logging and debugging
    manufacturer_url: str,
    text: str,
    ontology_version_id: OntologyVersionIDType,
    known_concepts: list[Concept],
    search_prompt: str,
    map_prompt: str,
    chunk_strategy: ChunkingStrat,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> ExtractionResults:
    logger.debug(
        f"Extracting {concept_type} for {manufacturer_url} at {extraction_timestamp} with ontology version {ontology_version_id}"
    )

    results = set[str]()
    stats: ExtractionStats = ExtractionStats(
        ontology_version_id=ontology_version_id,
        mapping={},
        search={},
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

    chunk_map = get_chunks_with_boundaries(
        text, chunk_strategy.max_tokens, chunk_strategy.overlap
    )

    # Run brute_search and llm_search for each chunk concurrently
    async def _process_chunk(bounds: str, text_chunk: str):
        # NOTE: doing brute search individually for each chunk is more expensive than all at once
        # and is computationally expensive in general, but we need chunk level results
        brute_set = brute_search(text_chunk, known_concepts)
        llm_set = await llm_search(
            text_chunk, search_prompt, gpt_model, model_params, True
        )
        return bounds, brute_set, llm_set

    tasks = [asyncio.create_task(_process_chunk(b, t)) for b, t in chunk_map.items()]
    chunk_results = await asyncio.gather(*tasks)

    # orphan_brutes: set[Concept] = set()
    unmapped_llm: set[str] = set()
    mutually_agreed_concepts: set[Concept] = set()

    for bounds, brute_set, llm_set in chunk_results:
        stats.search[bounds] = ChunkSearchStats(
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
                stats.search[bounds].results.add(kc.name)
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
        manufacturer_url,
        known_concepts,
        unmapped_llm.copy(),
        map_prompt,
    )

    # UPDATE unmapped_llm and mapping in chunk_stats
    final_unmapped_llm = map_results[
        "unmapped_unknowns"
    ]  # not needed if we don't pass unmapped_llm.copy()

    for _, chunk_stats in stats.search.items():
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

    return ExtractionResults(
        extracted_at=extraction_timestamp,
        results=list(results),
        stats=stats,
    )
