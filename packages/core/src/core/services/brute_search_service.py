import re
import logging

from core.models.skos_concept import Concept

logger = logging.getLogger(__name__)


def word_regex(keyword: str):
    # (?<!\w) asserts that the preceding character (if any) is not a word character.
    # (?=\W|$) asserts that the following character is either a non-word character or the end of the string.
    return r"(?<!\w)" + re.escape(keyword) + r"(?=\W|$)"


# only considers concept and altLabels, ignores ancestors
def brute_search(text: str, concepts: set[Concept]) -> set[str]:
    found_brute_search_labels: set[str] = set()

    for c in concepts:
        for label in c.matchLabels:
            if re.search(word_regex(label), text, re.IGNORECASE):
                found_brute_search_labels.add(label)

    logger.info(
        f"Brute search found {len(found_brute_search_labels)}:{found_brute_search_labels} concepts in text."
    )

    return found_brute_search_labels


def filter_non_overlapping_brute_results(
    llm_search_results: set[str],
    brute_search_results: set[str],
) -> set[str]:
    """
    Filter brute force search results down to those not already covered by an LLM result.

    A brute force result is kept only when it is not already a substring of any
    LLM search result, avoiding redundancy while preserving recall. Preserves
    casing of the brute results, but does case-insensitive comparison for
    filtering.
    """
    lowered_llm = {r.lower() for r in llm_search_results}
    return {
        r
        for r in brute_search_results
        if not any(r.lower() in llm_result for llm_result in lowered_llm)
    }


def merge_llm_and_brute_search_results(
    llm_search_results: set[str],
    brute_search_results: set[str],
) -> set[str]:
    """
    Merge LLM search results with filtered brute force search results.

    A brute force result is included only when it is not already a substring of
    any LLM search result, avoiding redundancy while preserving recall.
    Preserves casing of both LLM and brute results, but does case-insensitive
    comparison for filtering.
    """

    filtered_brute = filter_non_overlapping_brute_results(
        llm_search_results=llm_search_results,
        brute_search_results=brute_search_results,
    )
    logger.info(f"LLM search results: {llm_search_results}")
    logger.info(f"Brute force search results: {brute_search_results}")
    logger.info(f"Non-overlapping brute force results: {filtered_brute}")

    merged_results = llm_search_results | filtered_brute
    logger.info(f"Merged search results: {merged_results}")
    return merged_results
