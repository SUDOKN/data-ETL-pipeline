import logging

from core.models.db.concept_ground_truth import (
    YES_PREFIX,
    NO_PREFIX,
    CORRECT_PREFIX,
    INCORRECT_PREFIX,
    HumanConceptCorrection,
)
from core.models.db.keyword_ground_truth import (
    KeywordGroundTruth,
)
from core.models.field_types import (
    HumanVerificationResults,
    LLMDistillationResults,
    RawLLMMappingResult,
)
from data_etl_app.models.skos_concept import Concept

from data_etl_app.utils.llm_mapping_helper import (
    get_matched_concepts_and_unmatched_keywords,
)

logger = logging.getLogger(__name__)


def is_distillation_evidence_format_correct(reason: str) -> bool:
    """
    Checks if the reason for confirming or rejecting distillation results is in the correct format.
    The reason must start with "Yes, " for confirmed evidence or "No, " for rejected evidence.
    """
    return (reason.startswith(YES_PREFIX) or reason.startswith(NO_PREFIX)) and len(
        reason
    ) > 5  # ensure that there is some explanation following "Yes, " or "No, "


def is_mapping_reason_format_correct(reason: str) -> bool:
    """
    Checks if the reason for mapping an unknown term to a known concept is in the correct format.
    The reason must start with "Correct, " for correct mappings or "Incorrect, " for incorrect mappings.
    """
    return reason.startswith(CORRECT_PREFIX) or reason.startswith(INCORRECT_PREFIX)


def get_verified_results_from_human_distillation_correction(
    human_correction: HumanConceptCorrection,
) -> HumanVerificationResults:
    """
    Get the final distillation stage results after applying human corrections.
    Returns an empty dictionary if no distillation corrections were made.
    """
    return get_verified_distillation_results(
        human_correction.llm_distillation_correction.upsert
    )


def get_verified_distillation_results(
    llm_distillation_results: LLMDistillationResults,
) -> LLMDistillationResults:
    confirmed_keywords_w_evidence = {
        kw: reason
        for kw, reason in llm_distillation_results.items()
        if reason.startswith(YES_PREFIX)
    }
    return confirmed_keywords_w_evidence


def calculate_corrected_concept_results(
    known_concepts: set[Concept],
    human_correction: HumanConceptCorrection,
) -> list[str]:
    """
    Get the final results after applying human corrections.
    Returns None if no corrections were made.
    """

    verified_llm_distillation_results: dict[str, str] = (
        get_verified_results_from_human_distillation_correction(
            human_correction=human_correction
        )
    )

    (
        matched_concepts,
        _unmatched_keywords,
    ) = get_matched_concepts_and_unmatched_keywords(
        known_concepts, verified_llm_distillation_results
    )
    corrected_llm_mapping_results = human_correction.llm_mapping_correction.upsert
    results = {c.name for c in matched_concepts}
    results.update(get_verified_results_from_raw_mapping(corrected_llm_mapping_results))

    return list(results)


def get_verified_results_from_raw_mapping(
    llm_mapping_results: RawLLMMappingResult,
) -> set[str]:
    verified_mapped_known_concepts: set[str] = set()
    for mu, mk_dict in llm_mapping_results.items():
        for mk, reason in mk_dict.items():
            if reason.startswith(CORRECT_PREFIX):
                verified_mapped_known_concepts.add(mk)
    return verified_mapped_known_concepts


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

    lowered_llm = {r.lower() for r in llm_search_results}
    filtered_brute = {
        r
        for r in brute_search_results
        if not any(r.lower() in llm_result for llm_result in lowered_llm)
    }
    logger.info(f"LLM search results: {llm_search_results}")
    logger.info(f"Brute force search results: {brute_search_results}")
    logger.info(f"Non-overlapping brute force results: {filtered_brute}")

    merged_results = llm_search_results | filtered_brute
    logger.info(f"Merged search results: {merged_results}")
    return merged_results


def calculate_final_keyword_results(
    keyword_gt: KeywordGroundTruth,
) -> list[str] | None:
    """
    Get the final results after applying human corrections.
    Returns None if no corrections were made.
    """

    final_results: set[str] = set(keyword_gt.extraction_stats.results)

    last_correction_log = keyword_gt.corrections[-1] if keyword_gt.corrections else None
    if not last_correction_log:
        return list(final_results)

    final_results -= set(
        last_correction_log.human_correction.llm_search.remove
    )  # ensure beforehand that every element in remove must be present in results
    final_results |= set(last_correction_log.human_correction.llm_search.add)

    return list(final_results)
