import re
import logging
from datetime import datetime

# Configure logger
logger = logging.getLogger(__name__)


from shared.models.field_types import S3FileVersionIDType
from shared.utils.aws.s3.scraped_text_util import (
    get_file_name_from_mfg_etld,
    download_scraped_text_from_s3_by_filename,
)
from shared.models.db.manufacturer import Manufacturer

from data_etl_app.models.keyword_extraction_results import KeywordExtractionResults
from data_etl_app.models.db.keyword_ground_truth import (
    KeywordGroundTruth,
    KeywordResultCorrection,
    KeywordResultCorrectionLog,
)
from data_etl_app.models.types_and_enums import KeywordTypeEnum

from data_etl_app.services.brute_search_service import word_regex


async def get_keyword_ground_truth(
    manufacturer: Manufacturer,
    keyword_type: KeywordTypeEnum,
    chunk_no: int,
) -> KeywordGroundTruth | None:
    """
    Fetch the keyword ground truth for a given manufacturer URL, keyword type, and chunk number.
    Check if the scraped text file version ID and ontology version ID match the manufacturer's
    most recently scraped and extracted data.

    Assumed keyword_type is a valid KeywordTypeEnum value and present in manufacturer's data.
    """
    keyword_extraction_results: KeywordExtractionResults | None = getattr(
        manufacturer, keyword_type, None
    )
    assert (
        keyword_extraction_results is not None
    ), f"Keyword type '{keyword_type}' not found in manufacturer '{manufacturer.etld1}'."
    return await KeywordGroundTruth.find_one(
        KeywordGroundTruth.mfg_etld1 == manufacturer.etld1,
        KeywordGroundTruth.keyword_type == keyword_type,
        # ------------------ knowledge ids ------------------- #
        KeywordGroundTruth.scraped_text_file_version_id
        == manufacturer.scraped_text_file_version_id,
        KeywordGroundTruth.extract_prompt_version_id
        == keyword_extraction_results.stats.extract_prompt_version_id,
        # ---------------------------------------------------- #
        KeywordGroundTruth.chunk_no == chunk_no,
    )


async def does_a_kgt_exist_with_scraped_file_version(
    scraped_file_version_id: S3FileVersionIDType,
) -> KeywordGroundTruth | None:
    """
    Fetch the keyword ground truth for a given manufacturer URL.
    Check if the scraped text file version ID matches the manufacturer's
    most recently scraped and extracted data.

    """
    return await KeywordGroundTruth.find_one(
        KeywordGroundTruth.scraped_text_file_version_id == scraped_file_version_id
    )


async def add_new_correction_to_keyword_ground_truth(
    timestamp: datetime,
    manufacturer: Manufacturer,
    existing_keyword_gt: KeywordGroundTruth,
    new_correction: KeywordResultCorrection,
    s3_client,
) -> KeywordGroundTruth:

    existing_keyword_gt.updated_at = timestamp
    if (
        existing_keyword_gt.correction_logs[-1].result_correction.author_email
        == new_correction.author_email
    ):
        existing_keyword_gt.correction_logs.pop()  # the new correction will replace the last one because it is from the same author

    await _validate_keyword_ground_truth(
        manufacturer, existing_keyword_gt, new_correction, s3_client
    )

    existing_keyword_gt.correction_logs.append(
        KeywordResultCorrectionLog(
            created_at=existing_keyword_gt.created_at,
            result_correction=new_correction,
        )
    )

    logger.debug(
        f"Updating existing keyword ground truth {existing_keyword_gt} to the database."
    )
    return await existing_keyword_gt.save()


async def save_new_keyword_ground_truth(
    timestamp: datetime,
    manufacturer: Manufacturer,
    new_keyword_gt: KeywordGroundTruth,
    new_correction: KeywordResultCorrection,
    s3_client,
) -> KeywordGroundTruth:
    """
    Prepare a new keyword ground truth instance with the provided timestamp.
    This function is used when creating a new keyword ground truth entry.
    """

    new_keyword_gt.created_at = timestamp
    new_keyword_gt.updated_at = timestamp

    await _validate_keyword_ground_truth(
        manufacturer, new_keyword_gt, new_correction, s3_client
    )

    new_keyword_gt.correction_logs.append(
        KeywordResultCorrectionLog(
            created_at=new_keyword_gt.created_at,
            result_correction=new_correction,
        )
    )

    logger.debug(
        f"Inserting new keyword ground truth {new_keyword_gt} to the database."
    )
    return await new_keyword_gt.save()


async def _validate_keyword_ground_truth(
    manufacturer: Manufacturer,
    keyword_gt: KeywordGroundTruth,
    new_correction: KeywordResultCorrection,
    s3_client,
) -> None:
    """
    Validate and save the keyword ground truth to the database.

    keyword_ground_truth passed may be a new or existing instance.

    Note:
    - Make sure to set created_at and updated_at beforehand.
    - Ensure that the manufacturer is prevalidated and exists in the database.
    """

    keyword_gt = KeywordGroundTruth.model_validate(keyword_gt.model_dump())

    # keyword_data check
    keyword_extraction_results = getattr(manufacturer, keyword_gt.keyword_type, None)
    if not keyword_extraction_results:
        raise ValueError(
            f"No extraction results found for keyword type '{keyword_gt.keyword_type}'"
            f" in manufacturer '{keyword_gt.mfg_etld1}'."
        )

    assert (
        type(keyword_extraction_results) is KeywordExtractionResults
    ), f"Expected keyword_extraction_results to be of type KeywordExtractionResults, got {type(keyword_extraction_results)}."

    # chunk bounds and stats check
    chunk_bounds, chunk_search_stats = [
        (cb, css)
        for cb, css in sorted(
            keyword_extraction_results.stats.chunked_stats.items(),
            key=lambda item: item[0],
        )
    ][keyword_gt.chunk_no - 1]

    if keyword_gt.chunk_bounds != chunk_bounds:
        raise ValueError(
            f"Chunk bounds '{keyword_gt.chunk_bounds}' do not match the expected bounds '{chunk_bounds}' for chunk number {keyword_gt.chunk_no}."
        )

    if keyword_gt.chunk_search_stats != chunk_search_stats:
        raise ValueError(
            "Chunk search stats do not match the expected stats for the given chunk bounds."
        )

    # file and version ID check
    scraped_text, version_id = await download_scraped_text_from_s3_by_filename(
        s3_client,
        file_name=get_file_name_from_mfg_etld(keyword_gt.mfg_etld1),
        version_id=manufacturer.scraped_text_file_version_id,
    )
    if manufacturer.scraped_text_file_version_id != version_id:
        raise ValueError(
            f"Scraped text version ID mismatch for mfg_url: {keyword_gt.mfg_etld1}. Expected: {manufacturer.scraped_text_file_version_id}, got: {version_id}. (manufacturer.scraped_text_file_version_id != version_id)"
        )
    if keyword_gt.scraped_text_file_version_id != version_id:
        raise ValueError(
            f"Scraped text version ID mismatch for keyword ground truth. Expected: {version_id}, got: {keyword_gt.scraped_text_file_version_id}. (keyword_ground_truth.scraped_text_file_version_id != version_id)"
        )
    if (
        manufacturer.scraped_text_file_version_id
        != keyword_gt.scraped_text_file_version_id
    ):
        raise ValueError(
            f"Scraped text version ID mismatch for keyword ground truth. Expected: {manufacturer.scraped_text_file_version_id}, got: {keyword_gt.scraped_text_file_version_id}. (manufacturer.scraped_text_file_version_id != keyword_ground_truth.scraped_text_file_version_id)"
        )

    # chunk text check
    start, end = map(int, keyword_gt.chunk_bounds.split(":"))
    if start < 0 or end > len(scraped_text):
        raise ValueError(
            f"Chunk bounds {keyword_gt.chunk_bounds} are out of range for the scraped text."
        )

    if keyword_gt.chunk_text != scraped_text[start:end]:
        raise ValueError(
            "Chunk text does not match the expected text for the given chunk bounds."
        )

    _validate_new_human_correction(keyword_gt, new_correction)


def _validate_new_human_correction(
    chunk_keyword_gt: KeywordGroundTruth,
    new_correction: KeywordResultCorrection,
):
    """
    VALIDATE RESULT CORRECTION

    result_correction.add: (list of keywords present in chunk_text)
    - each value must not be already present in chunk_keyword_gt.chunk_search_stats.results

    result_correction.remove:
    - each item in remove must already be present in chunk_keyword_gt.chunk_search_stats.results
    """

    if not new_correction:
        raise ValueError(
            "result_correction must be provided to validate the keyword ground truth."
        )

    for kw in new_correction.add:
        if not re.search(word_regex(kw), chunk_keyword_gt.chunk_text):
            raise ValueError(
                f"Keyword '{kw}' to be added is not present in the chunk text."
            )
        if kw in chunk_keyword_gt.chunk_search_stats.results:
            raise ValueError(
                f"Keyword '{kw}' to be added is already present in the extracted results."
            )

    for kw in new_correction.remove:
        if kw not in chunk_keyword_gt.chunk_search_stats.results:
            raise ValueError(
                f"Keyword '{kw}' to be removed is not present in the extracted results."
            )
