from core.models.db.concept_ground_truth import (
    ConceptGroundTruth,
)
from core.models.db.keyword_ground_truth import (
    KeywordGroundTruth,
)


def calculate_final_concept_results(
    chunk_keyword_gt: ConceptGroundTruth,
) -> list[str] | None:
    """
    Get the final results after applying human corrections.
    Returns None if no corrections were made.
    """

    final_results: set[str] = set(chunk_keyword_gt.chunk_search_stats.results)

    if not chunk_keyword_gt.correction_logs:
        return list(final_results)

    last_correction_log = chunk_keyword_gt.correction_logs[-1]
    final_results -= set(
        last_correction_log.result_correction.remove
    )  # ensure beforehand that every element in remove must be present in results
    final_results |= set(last_correction_log.result_correction.add.keys())

    return list(final_results)


def calculate_final_keyword_results(
    chunk_keyword_gt: KeywordGroundTruth,
) -> list[str] | None:
    """
    Get the final results after applying human corrections.
    Returns None if no corrections were made.
    """

    final_results: set[str] = set(chunk_keyword_gt.chunk_search_stats.results)

    if not chunk_keyword_gt.correction_logs:
        return list(final_results)

    for log in chunk_keyword_gt.correction_logs:
        final_results -= set(
            log.result_correction.remove
        )  # ensure beforehand that every element in remove must be present in results
        final_results |= set(log.result_correction.add)

    return list(final_results)
