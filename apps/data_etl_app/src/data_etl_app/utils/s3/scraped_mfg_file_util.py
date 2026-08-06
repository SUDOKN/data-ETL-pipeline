from apps.data_etl_app.src.data_etl_app.services.ground_truth.concept_ground_truth_service import (
    does_a_cgt_exist_with_scraped_file_version,
)
from apps.data_etl_app.src.data_etl_app.services.ground_truth.keyword_ground_truth_service import (
    does_a_kgt_exist_with_scraped_file_version,
)
from apps.data_etl_app.src.data_etl_app.services.ground_truth.binary_ground_truth_service import (
    does_a_bgt_exist_with_scraped_file_version,
)


async def is_scraped_mfg_file_version_deletable(s3_version_id: str) -> bool:
    """Safe to delete only if no concept/keyword/binary ground truth references this version."""
    if (
        await does_a_kgt_exist_with_scraped_file_version(s3_version_id)
        or await does_a_cgt_exist_with_scraped_file_version(s3_version_id)
        or await does_a_bgt_exist_with_scraped_file_version(s3_version_id)
    ):
        return False
    return True
