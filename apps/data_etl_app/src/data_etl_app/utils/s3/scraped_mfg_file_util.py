from data_etl_app.services.ground_truth.binary_ground_truth_service import (
    does_a_bgt_exist_with_scraped_file_version,
)


async def is_scraped_mfg_file_version_deletable(s3_version_id: str) -> bool:
    """Safe to delete only if no ground truth references this version. Binary
    GT is the one live instrument; the v1 LLM phrase-GT instrument was retired
    at the pipeline-v2 flip (its successor pins text versions again when the
    Phase-5 rebuild lands, and this guard is where that check returns)."""
    if await does_a_bgt_exist_with_scraped_file_version(s3_version_id):
        return False
    return True
