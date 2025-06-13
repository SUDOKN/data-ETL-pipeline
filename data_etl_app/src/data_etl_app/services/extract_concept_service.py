from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time — they're stored as strings automatically.
import asyncio


from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.db.extraction_stats import ExtractionStats
from data_etl_app.models.db.extraction_results import ExtractionResult

from data_etl_app.services.map_unknown_to_known_service import mapKnownToUnknown
from data_etl_app.services.brute_search_service import brute_search
from data_etl_app.services.llm_search_service import llm_search
from data_etl_app.services.ontology_service import ontology_service
from data_etl_app.services.prompt_service import prompt_service

from data_etl_app.utils.chunk_util import (
    ChunkingStrat,
    get_chunks_with_boundaries,
)

from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)


async def extract_industries(
    manufacturer_url: str, text: str, debug: bool = False
) -> ExtractionResult:
    """
    Extract industries for a manufacturer text.
    """
    return await _extract_concepts(
        "industries",
        manufacturer_url,
        text,
        ontology_service.industries,
        prompt_service.extract_industry_prompt,
        prompt_service.unknown_to_known_industry_prompt,
        ChunkingStrat(overlap=0.15, max_tokens=5000),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
        debug=debug,
    )


async def extract_certificates(
    manufacturer_url: str, text: str, debug: bool = False
) -> ExtractionResult:
    """
    Extract certificates for a manufacturer text.
    """
    return await _extract_concepts(
        "certificates",
        manufacturer_url,
        text,
        ontology_service.certificates,
        prompt_service.extract_certificate_prompt,
        prompt_service.unknown_to_known_certificate_prompt,
        ChunkingStrat(overlap=0.0, max_tokens=7500),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
        debug=debug,
    )


async def extract_processes(
    manufacturer_url: str, text: str, debug: bool = False
) -> ExtractionResult:
    """
    Extract process capabilities for a manufacturer text.
    """
    return await _extract_concepts(
        "process_caps",
        manufacturer_url,
        text,
        ontology_service.process_capabilities,
        prompt_service.extract_process_prompt,
        prompt_service.unknown_to_known_process_prompt,
        ChunkingStrat(overlap=0.15, max_tokens=2500),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
        debug=debug,
    )


async def extract_materials(
    manufacturer_url: str, text: str, debug: bool = False
) -> ExtractionResult:
    """
    Extract material capabilities for a manufacturer text.
    """
    return await _extract_concepts(
        "material_caps",
        manufacturer_url,
        text,
        ontology_service.material_capabilities,
        prompt_service.extract_material_prompt,
        prompt_service.unknown_to_known_material_prompt,
        ChunkingStrat(overlap=0.1, max_tokens=5000),
        gpt_model=GPT_4o_mini,
        model_params=DefaultModelParameters,
        debug=debug,
    )


async def _extract_concepts(
    concept_type: str,  # used for logging and debugging
    manufacturer_url: str,
    text: str,
    known_concepts: list[Concept],
    search_prompt: str,
    map_prompt: str,
    chunk_strategy: ChunkingStrat,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
    debug: bool = False,
) -> ExtractionResult:

    results = set[str]()
    stats: ExtractionStats = {
        "search": {},
        "mapping": {},
        "unmapped_brute": set[Concept](),
        "unmapped_llm": set[str](),
    }

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

    chunks = get_chunks_with_boundaries(
        text, chunk_strategy.max_tokens, chunk_strategy.overlap
    )

    # Run brute_search and llm_search for each chunk concurrently
    async def _process_chunk(bounds: str, text_chunk: str):
        brute_set = brute_search(text_chunk, known_concepts)
        llm_set = await llm_search(
            text_chunk, search_prompt, gpt_model, model_params, True
        )
        return bounds, brute_set, llm_set

    tasks = [asyncio.create_task(_process_chunk(b, t)) for b, t in chunks.items()]
    chunk_results = await asyncio.gather(*tasks)

    orphan_brutes: set[Concept] = set()
    orphan_llm: set[str] = set()
    mutually_agreed_concepts: set[Concept] = set()

    for bounds, brute_set, llm_set in chunk_results:
        stats["search"][bounds] = {"brute": brute_set, "llm": llm_set, "human": set()}
        # MUTUALLY AGREED
        for kc in known_concepts:
            common = kc.matchLabels & llm_set
            if common:
                mutually_agreed_concepts.add(kc)
                llm_set -= common
        results |= {str(c) for c in mutually_agreed_concepts}
        orphan_brutes |= brute_set - mutually_agreed_concepts
        orphan_llm |= llm_set

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
        list(orphan_llm),
        map_prompt,
        debug,
    )
    mapped_known_concept_labels = set(
        str(concept) for concept in map_results["known_to_unknowns"].keys()
    )
    results = results | mapped_known_concept_labels

    mapped_orphan_brutes: set[Concept] = {
        ob for ob in orphan_brutes if str(ob) in mapped_known_concept_labels
    }

    orphan_brutes -= mapped_orphan_brutes
    orphan_llm = map_results["unmapped_unknowns"]

    if debug:
        print(f"Remaining orphan brute and llm:")
        orphan_brute_labels = {str(k) for k in orphan_brutes}
        print(f"unmapped_brute_labels {len(orphan_brute_labels)}:{orphan_brute_labels}")
        print(f"orphan llm {len(orphan_llm)}:{orphan_llm}")

    stats["unmapped_brute"] |= orphan_brutes
    stats["unmapped_llm"] |= orphan_llm
    stats["mapping"] = map_results["known_to_unknowns"]

    return {"results": results, "stats": stats}
