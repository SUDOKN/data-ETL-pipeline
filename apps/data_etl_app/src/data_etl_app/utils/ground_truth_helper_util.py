import logging

from data_etl_app.db_models.concept_ground_truth import (
    DistillationResultVerificationEnum,
    MappingResultVerificationEnum,
    HumanConceptCorrection,
)
from data_etl_app.db_models.keyword_ground_truth import (
    KeywordGroundTruth,
)
from core.models.extraction_schemas.legacy_mapping_types import (
    HumanVerificationResults,
    RawLLMMappingResult,
)
from core.models.extraction_schemas.relationship import (
    LLMPhraseRelationshipResults,
)
from core.models.extraction_schemas.screening import (
    LLMScreeningResults,
)
from core.models.skos_concept import Concept

from core.services.brute_search_service import (
    get_matched_concepts_and_unmatched_keywords,
    filter_non_overlapping_brute_results,
)

logger = logging.getLogger(__name__)


def is_phrase_relationship_evidence_format_correct(reason: str) -> bool:
    """
    Checks if the reason for confirming or rejecting phrase_relationship results is in the correct format.
    The reason must start with "Yes, " for confirmed evidence or "No, " for rejected evidence.
    """
    return (
        reason.startswith(DistillationResultVerificationEnum.YES_PREFIX)
        or reason.startswith(DistillationResultVerificationEnum.NO_PREFIX)
        or reason.startswith(DistillationResultVerificationEnum.OUT_OF_SCOPE_YES_PREFIX)
        or reason.startswith(DistillationResultVerificationEnum.OUT_OF_SCOPE_NO_PREFIX)
    ) and len(reason) > 10
    # to ensure that there is some explanation following
    # "Yes, ", "No, ",
    # "Yes_though_out-of-scope, " or "No_though_out-of-scope, "


def is_mapping_reason_format_correct(reason: str) -> bool:
    """
    Checks if the reason for mapping an unknown term to a known concept is in the correct format.
    The reason must start with "Correct, " for correct mappings or "Incorrect, " for incorrect mappings.
    """
    return reason.startswith(
        MappingResultVerificationEnum.CORRECT_PREFIX
    ) or reason.startswith(MappingResultVerificationEnum.INCORRECT_PREFIX)


def get_verified_results_from_human_phrase_relationship_correction(
    human_correction: HumanConceptCorrection,
) -> HumanVerificationResults:
    """
    Get the final phrase_relationship stage results after applying human corrections.
    Returns an empty dictionary if no phrase_relationship corrections were made.
    """
    return get_verified_phrase_relationship_results(
        human_correction.llm_phrase_relationship_screening.upsert
    )


def get_verified_phrase_relationship_results(
    llm_phrase_relationship_screening_results: LLMPhraseRelationshipResults,
) -> LLMScreeningResults:
    confirmed_keywords_w_evidence = {
        kw: reason
        for kw, reason in llm_phrase_relationship_screening_results.items()
        if (
            reason.startswith(DistillationResultVerificationEnum.YES_PREFIX)
            or reason.startswith(
                DistillationResultVerificationEnum.OUT_OF_SCOPE_YES_PREFIX
            )
        )
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

    verified_llm_phrase_relationship_results: dict[str, str] = (
        get_verified_results_from_human_phrase_relationship_correction(
            human_correction=human_correction
        )
    )

    (
        matched_concepts,
        _unmatched_keywords,
    ) = get_matched_concepts_and_unmatched_keywords(
        known_concepts, verified_llm_phrase_relationship_results
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
            if reason.startswith(MappingResultVerificationEnum.CORRECT_PREFIX):
                verified_mapped_known_concepts.add(mk)
    return verified_mapped_known_concepts


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
