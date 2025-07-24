from data_etl_app.models.keyword_ground_truth import (
    KeywordGroundTruth,
)


def calculate_final_results(
    keyword_gt: KeywordGroundTruth,
) -> list[str] | None:
    """
    Get the final results after applying human corrections.
    Returns None if no corrections were made.
    """

    final_results: set[str] = set(keyword_gt.chunk_search_stats.results)

    if not keyword_gt.human_correction_logs:
        return list(final_results)

    for log in reversed(keyword_gt.human_correction_logs):
        final_results -= set(
            log.result_correction.remove
        )  # ensure beforehand that every element in remove must be present in results
        final_results |= set(log.result_correction.add.keys())

    return list(final_results)
