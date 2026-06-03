from core.models.db.concept_ground_truth import (
    HumanConceptCorrection,
)
from core.models.db.keyword_ground_truth import (
    KeywordGroundTruth,
)
from core.models.field_types import (
    HumanEvidenceResults,
)
from data_etl_app.models.skos_concept import Concept

from data_etl_app.utils.llm_mapping_helper import (
    get_matched_concepts_and_unmatched_keywords,
)


def is_evidence_reason_format_correct(reason: str) -> bool:
    """
    Checks if the reason for confirming or rejecting evidence is in the correct format.
    The reason must start with "Yes, " for confirmed evidence or "No, " for rejected evidence.
    """
    return reason.lower().startswith("yes, ") or reason.lower().startswith("no, ")


def is_mapping_reason_format_correct(reason: str) -> bool:
    """
    Checks if the reason for mapping an unknown term to a known concept is in the correct format.
    The reason must start with "Correct, " for correct mappings or "Incorrect, " for incorrect mappings.
    """
    return reason.lower().startswith("correct, ") or reason.lower().startswith(
        "incorrect, "
    )


def calculate_verified_concept_evidence_results(
    human_correction: HumanConceptCorrection,
) -> HumanEvidenceResults:
    """
    Get the final evidence stage results after applying human corrections.
    Returns None if no corrections were made.
    """
    confirmed_keywords_w_evidence = {
        kw: reason
        for kw, reason in human_correction.llm_evidence_correction.upsert.items()
        if reason.lower().startswith("yes, ")
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

    verified_llm_evidence_results: dict[str, str] = (
        calculate_verified_concept_evidence_results(human_correction=human_correction)
    )
    verified_keywords_w_evidence = {
        kw: evidence for kw, evidence in verified_llm_evidence_results.items()
    }

    (
        matched_concepts,
        _unmatched_keywords,
    ) = get_matched_concepts_and_unmatched_keywords(
        known_concepts, verified_keywords_w_evidence
    )
    corrected_llm_mapping_results = human_correction.llm_mapping_correction.upsert
    results = {c.name for c in matched_concepts}
    for mk, mu_dict in corrected_llm_mapping_results.items():
        for _k, v in mu_dict.items():
            if v.startswith("Correct, "):
                print(
                    f"Mapping correction for unknown term '{_k}' in mapping result for known concept '{mk}' is marked as correct. Adding '{mk}' to results."
                )
                results.add(mk)

    return list(results)


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
