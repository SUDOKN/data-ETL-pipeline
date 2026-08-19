from data_etl_app.services.ground_truth.binary_ground_truth_service import (
    does_a_bgt_exist_with_scraped_file_version,
)
from data_etl_app.services.ground_truth.llm_phrase_gt_document_service import (
    does_an_llm_phrase_gt_exist_with_scraped_file_version,
)


async def is_scraped_mfg_file_version_deletable(s3_version_id: str) -> bool:
    """Safe to delete only if no ground truth references this version:
    binary (legacy, still live) or the LLM phrase-GT instrument, whose
    detail plane fetches the pinned text at read time."""
    if await does_a_bgt_exist_with_scraped_file_version(
        s3_version_id
    ) or await does_an_llm_phrase_gt_exist_with_scraped_file_version(s3_version_id):
        return False
    return True
