from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time — they're stored as strings automatically.
import json
import re
from typing import TypedDict

from data_etl_app.models.skos_concept import Concept, ConceptJSONEncoder
from data_etl_app.models.extraction import (
    ExtractionResult,
)
from data_etl_app.models.extractor import (
    ExtractionResultsStats,
    ExtractionResultJSONSerialized,
)

from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)
from open_ai_key_app.utils.ask_gpt import (
    ask_gpt_async,
    num_tokens_from_string,
)
from data_etl_app.utils.chunk_util import (
    ChunkingStrat,
    get_chunks_with_boundaries,
)


def keyword_regex(keyword: str):
    # (?<!\w) asserts that the preceding character (if any) is not a word character.
    # (?=\W|$) asserts that the following character is either a non-word character or the end of the string.
    return r"(?<!\w)" + re.escape(keyword) + r"(?=\W|$)"


# only considers concept and altLabels, ignores ancestors
def brute_search(
    text: str, concepts: list[Concept], debug: bool = False
) -> set[Concept]:
    brute_search_concepts: set[Concept] = set()

    for c in concepts:
        if any(
            re.search(keyword_regex(label.lower()), text) for label in c.matchLabels
        ):
            brute_search_concepts.add(c)

    if debug:
        print(
            f"brute_search_concepts {len(brute_search_concepts)}:{brute_search_concepts}"
        )

    return brute_search_concepts


# LLM's independent search
async def llm_search(
    text: str,
    prompt: str,
    gpt_model: GPTModel,
    model_params: ModelParameters,
    num_passes: int = 1,
    debug: bool = False,
) -> set[str]:
    # print(f'prompt:{prompt}')
    llm_results: set[str] = set()
    for _ in range(num_passes):
        gpt_response = await ask_gpt_async(text, prompt, gpt_model, model_params)

        # if debug:
        #     print(f"llm_search gpt_response:{gpt_response}")

        if not gpt_response:
            print(f"Invalid gpt_response:{gpt_response}")
            raise ValueError("llm_results: Empty or invalid response from GPT")

        try:
            gpt_response = gpt_response.replace("```", "").replace("json", "")
            new_extracted: set[str] = set(json.loads(gpt_response)) - llm_results
        except:
            raise ValueError(f"llm_results: Invalid response from GPT:{gpt_response}")

        if debug:
            print(
                f"llm_results new_extracted {len(new_extracted)}:{list(new_extracted)}"
            )

        llm_results = llm_results | new_extracted

    # print(f'llm_results:{llm_results}')

    return llm_results


def search_result_to_json_serializable(
    search_result: ExtractionResult,
) -> ExtractionResultJSONSerialized:
    mapping = {str(k): v for k, v in search_result["stats"]["mapping"].items()}
    return {
        "results": list(search_result["results"]),
        "stats": {
            "search": {
                chunk_bounds: {
                    "human": list(v["human"] or []),
                    "brute": [str(c) for c in v["brute"]],
                    "llm": list(v["llm"]),
                }
                for chunk_bounds, v in search_result["stats"]["search"].items()
            },
            "mapping": mapping,
            "unmapped_brute": [
                str(k) for k in search_result["stats"]["unmapped_brute"]
            ],
            "unmapped_llm": list(search_result["stats"]["unmapped_llm"]),
        },
    }


async def extract_keywords(
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
    stats: ExtractionResultsStats = {
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

    orphan_brutes: set[Concept] = set()
    orphan_llm: set[str] = set()
    orphan_brute_labels: set[str] = set()
    mutually_agreed_concepts: set[Concept] = (
        set()
    )  # needs to be outside else orphan_brutes is incorrect if an old llm search result is found in the next chunk

    for chunk_bounds, chunk in chunks.items():
        # BRUTE SEARCH
        stats["search"][f"{chunk_bounds}"]["brute"] = brute_search(
            chunk, known_concepts
        )

        if debug:
            print(
                f'\n\n-----------------\nstats["search"][f"{chunk_bounds}"]["brute"]\n{len(stats["search"][f"{chunk_bounds}"]["brute"])}: {stats["search"][f"{chunk_bounds}"]["brute"]}'
            )

        # LLM SEARCH
        llm_search_results: set[str] = await llm_search(
            chunk, search_prompt, gpt_model, model_params, True
        )
        stats["search"][f"{chunk_bounds}"]["llm"] |= llm_search_results
        if debug:
            print(
                f"llm_search_results {chunk_bounds} {len(llm_search_results)}: {llm_search_results}"
            )

        # MUTUALLY AGREED
        for kc in known_concepts:
            common_labels = kc.matchLabels & llm_search_results
            if common_labels:
                if debug:
                    print(f"common_labels {len(common_labels)}: {common_labels}")
                mutually_agreed_concepts.add(kc)
                llm_search_results -= common_labels
                if debug:
                    print(
                        f"remaining llm_search_results {len(llm_search_results)}: {llm_search_results}"
                    )

        mutually_agreed_concept_labels = {str(c) for c in mutually_agreed_concepts}

        if debug:
            print(
                f"mutually agreed {len(mutually_agreed_concept_labels)}:{mutually_agreed_concept_labels}\n"
            )

        results |= mutually_agreed_concept_labels  # add to final results because mutually agreed

        orphan_brutes |= (
            stats["search"][f"{chunk_bounds}"]["brute"] - mutually_agreed_concepts
        )
        orphan_brute_labels = {str(k) for k in orphan_brutes}

        orphan_llm |= llm_search_results
        if debug:
            print(
                f"unmapped_brute_labels {len(orphan_brute_labels)}: {orphan_brute_labels}"
            )
            print(f"orphan_llm {len(orphan_llm)}: {orphan_llm}")
        # break

    """
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
        print(f"unmapped_brute_labels {len(orphan_brute_labels)}:{orphan_brute_labels}")
        print(f"orphan llm {len(orphan_llm)}:{orphan_llm}")

    stats["unmapped_brute"] |= orphan_brutes
    stats["unmapped_llm"] |= orphan_llm
    stats["mapping"] = map_results["known_to_unknowns"]

    return {"results": results, "stats": stats}


class MapKnownToUnknownResult(TypedDict):
    # raw_mapping: dict[str, list[str]]
    known_to_unknowns: dict[Concept, list[str]]
    unmapped_knowns: set[Concept]
    unmapped_unknowns: set[str]


async def mapKnownToUnknown(
    concept_type: str,
    manufacturer_url: str,
    known_concepts: list[Concept],
    unknowns: list[str],
    prompt: str,
    # gpt_model: GPTModel,
    # model_params: ModelParameters,
    debug: bool = False,
) -> MapKnownToUnknownResult:

    context = json.dumps(
        {"unknowns": unknowns, "knowns": known_concepts}, cls=ConceptJSONEncoder
    )
    if debug:
        print(f"\nmapping unknown_to_known")
        print(f"context {num_tokens_from_string(context)}:{context}")

    gpt_response = await ask_gpt_async(
        context, prompt, GPT_4o_mini, DefaultModelParameters
    )
    # gpt_response = await ask_gpt_async(context, prompt, pool, gpt_model, model_params)

    if debug:
        print(f"gptresponse:{gpt_response}")

    if not gpt_response:
        print(f"gptresponse:{gpt_response}")
        raise ValueError(
            f"{manufacturer_url}:{concept_type} unknown_to_known: Empty response from GPT"
        )

    gpt_response = gpt_response.replace("```", "").replace("json", "")

    # NOTE: mapping can be {1 unknowns:M knowns} or {M unknowns:1 knowns}
    mapping: dict[str, str] = json.loads(gpt_response)  # from unknown --> known
    if debug:
        print(f"mapping:{json.dumps(mapping, indent=2)}")

    known_concept_labels = [label for k in known_concepts for label in k.matchLabels]
    unmapped_knowns = set(known_concepts)  # starts as the full set of passed knowns
    unmapped_unknowns = set(unknowns)
    known_to_unknowns: dict[Concept, list[str]] = {k: [] for k in known_concepts}

    # mapped_unknown "biotech" -> ["pharmaceutical"] mapped_knowns
    for mu, mk in mapping.items():
        if mu not in unmapped_unknowns:
            # case 2: mapped_unknown was either hallucinated, in which case we will still check if mapped_knowns are valid, so just raise a warning
            print(
                f"WARNING: {manufacturer_url}:{concept_type} mapped_unknown:{mu} was not in the original unknowns list"
            )
        else:
            if mk:  # mk must not be null/None
                if mk not in known_concept_labels:
                    print(
                        f"WARNING: {manufacturer_url}:{concept_type} mapped_known:{mk} was not in the original knowns list"
                    )
                else:
                    known_concept = next(
                        (k for k in known_concepts if mk in k.matchLabels), None
                    )
                    if not known_concept:
                        raise ValueError(
                            f"{manufacturer_url}:{concept_type} mapped_known:{mk} was not found in known_concepts"
                        )
                    known_to_unknowns[known_concept].append(mu)

    # Derive unmapped_knowns and unmapped_unknowns
    for kc, unknowns in known_to_unknowns.items():
        if (
            unknowns
        ):  # meaning there was at least one mapping from unknown to known by llm
            unmapped_knowns.remove(kc)
            if debug:
                print(f"removing mapped_knowns:{kc}")
                print(f"removing mapped_unknowns:{unknowns}")
            unmapped_unknowns -= set(
                unknowns
            )  # NOTE: unknowns may contain hallucinations, but subtraction is safe
        # else: known was not mapped and stays in both known_to_unknowns and unmapped_knowns

    # pack everything and send back
    final_bundle: MapKnownToUnknownResult = {
        # "raw_mapping": mapping,
        "known_to_unknowns": {
            k: v for k, v in known_to_unknowns.items() if v
        },  # actively used
        "unmapped_knowns": unmapped_knowns,
        "unmapped_unknowns": unmapped_unknowns,
    }

    return final_bundle
