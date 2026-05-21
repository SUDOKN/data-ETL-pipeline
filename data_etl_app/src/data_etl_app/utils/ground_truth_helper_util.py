from core.models.db.concept_ground_truth import (
    ConceptCorrectionLog,
    ConceptGroundTruth,
    HumanConceptCorrection,
)
from core.models.db.keyword_ground_truth import (
    KeywordGroundTruth,
)
from core.models.field_types import (
    HumanEvidenceResults,
    LLMEvidenceResults,
    LLMMappingType,
)
from data_etl_app.models.skos_concept import Concept
from data_etl_app.services.extraction.deferred_concept_mapping_service import (
    get_matched_concepts_and_unmatched_keywords,
)


def calculate_corrected_concept_evidence_results(
    original_llm_evidence: LLMEvidenceResults,
    human_correction: HumanConceptCorrection,
) -> HumanEvidenceResults:
    """
    Get the final evidence stage results after applying human corrections.
    Returns None if no corrections were made.
    """
    confirmed_keywords_w_evidence = {
        kw: evidence for kw, evidence in original_llm_evidence.items() if evidence
    }

    for kw, reason in human_correction.llm_evidence_correction.upsert.items():
        confirmed_keywords_w_evidence[kw] = reason

    for kw in human_correction.llm_evidence_correction.reject:
        if kw in confirmed_keywords_w_evidence:
            del confirmed_keywords_w_evidence[kw]

    return confirmed_keywords_w_evidence


def calculate_corrected_concept_mapping_results(
    original_llm_mapping: LLMMappingType,
    human_correction: HumanConceptCorrection,
) -> LLMMappingType:
    """
    Get the final mapping stage results after applying human corrections.
    Returns None if no corrections were made.
    """

    for mk, mus in human_correction.llm_mapping_correction.upsert.items():
        original_llm_mapping[mk] = mus

    for mk in human_correction.llm_mapping_correction.remove:
        if mk in original_llm_mapping:
            del original_llm_mapping[mk]

    return original_llm_mapping


def calculate_corrected_concept_results(
    corrected_llm_evidence_results: dict[str, str],
    known_concepts: set[Concept],
    original_mapping: LLMMappingType,
    human_correction: HumanConceptCorrection,
) -> list[str]:
    """
    Get the final results after applying human corrections.
    Returns None if no corrections were made.
    """

    # corrected_llm_evidence_results = calculate_corrected_concept_evidence_results(
    #     chunk_keyword_gt
    # )
    verified_keywords_w_evidence = {
        kw: evidence
        for kw, evidence in corrected_llm_evidence_results.items()
        if evidence
    }

    (
        matched_concepts,
        _unmatched_keywords,
    ) = get_matched_concepts_and_unmatched_keywords(
        known_concepts, verified_keywords_w_evidence
    )
    corrected_llm_mapping_results = calculate_corrected_concept_mapping_results(
        original_mapping,
        human_correction,
    )
    results = {c.name for c in matched_concepts} | set(
        corrected_llm_mapping_results.keys()
    )
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
