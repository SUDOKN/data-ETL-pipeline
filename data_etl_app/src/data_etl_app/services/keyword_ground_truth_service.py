import re
import logging
from datetime import datetime

# Configure logger
logger = logging.getLogger(__name__)


from shared.utils.aws.s3.scraped_text_util import (
    get_file_name_from_mfg_etld,
    download_scraped_text_from_s3_by_filename,
)
from shared.models.db.manufacturer import Manufacturer
from shared.models.extraction_results import ExtractionResults
from shared.models.types import OntologyVersionIDType

from data_etl_app.models.keyword_ground_truth import KeywordGroundTruth
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.keyword_ground_truth import (
    KeywordGroundTruth,
    HumanCorrection,
    HumanCorrectionLog,
)
from data_etl_app.utils.keyword_ground_truth_helper_util import calculate_final_results
from data_etl_app.utils.route_url_util import (
    get_full_ontology_concept_flat_url,
)
from data_etl_app.services.ontology_service import ontology_service
from data_etl_app.services.brute_search_service import keyword_regex
from data_etl_app.models.types import ConceptTypeEnum


async def get_keyword_ground_truth(
    manufacturer: Manufacturer,
    concept_type: ConceptTypeEnum,
    chunk_no: int,
) -> KeywordGroundTruth | None:
    """
    Fetch the keyword ground truth for a given manufacturer URL, concept type, and chunk number.
    Check if the scraped text file version ID and ontology version ID match the manufacturer's
    most recently scraped and extracted data.

    Assumed concept_type is a valid ConceptTypeEnum value and present in manufacturer's data.
    """
    extracted_concept_data: ExtractionResults | None = getattr(
        manufacturer, concept_type, None
    )
    assert (
        extracted_concept_data is not None
    ), f"Concept type '{concept_type}' not found in manufacturer '{manufacturer.etld1}'."
    return await KeywordGroundTruth.find_one(
        KeywordGroundTruth.mfg_etld1 == manufacturer.etld1,
        KeywordGroundTruth.scraped_text_file_version_id
        == manufacturer.scraped_text_file_version_id,
        KeywordGroundTruth.ontology_version_id
        == extracted_concept_data.stats.ontology_version_id,
        KeywordGroundTruth.concept_type == concept_type,
        KeywordGroundTruth.chunk_no == chunk_no,
    )


async def update_existing_with_new_keyword_ground_truth(
    timestamp: datetime,
    manufacturer: Manufacturer,
    existing_keyword_gt: KeywordGroundTruth,
    new_correction: HumanCorrection,
    s3_client,
) -> KeywordGroundTruth:

    existing_keyword_gt.updated_at = timestamp

    await _validate_keyword_ground_truth(
        manufacturer, existing_keyword_gt, new_correction, s3_client
    )

    existing_keyword_gt.human_correction_logs.append(
        HumanCorrectionLog(
            created_at=existing_keyword_gt.created_at,
            result_correction=new_correction,
        )
    )

    logger.debug(
        f"Updating existingkeyword ground truth {existing_keyword_gt} to the database."
    )
    return await existing_keyword_gt.save()


async def save_new_keyword_ground_truth(
    timestamp: datetime,
    manufacturer: Manufacturer,
    new_keyword_gt: KeywordGroundTruth,
    new_correction: HumanCorrection,
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

    new_keyword_gt.human_correction_logs.append(
        HumanCorrectionLog(
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
    new_correction: HumanCorrection,
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

    # concept_data check
    extracted_concept_data = getattr(manufacturer, keyword_gt.concept_type, None)
    if not extracted_concept_data:
        raise ValueError(
            f"No extraction results found for concept type '{keyword_gt.concept_type}'"
            f" in manufacturer '{keyword_gt.mfg_etld1}'."
        )

    assert (
        type(extracted_concept_data) is ExtractionResults
    ), f"Expected extracted_concept_data to be of type ExtractionResults, got {type(extracted_concept_data)}."

    # chunk bounds and stats check
    chunk_bounds, chunk_search_stats = [
        (cb, css)
        for cb, css in sorted(
            extracted_concept_data.stats.search.items(), key=lambda item: item[0]
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
        s3_client, file_name=get_file_name_from_mfg_etld(keyword_gt.mfg_etld1)
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

    # ontology version ID check in case it is different from the latest version
    ontology_info: tuple[OntologyVersionIDType, list[Concept]] = getattr(
        ontology_service, keyword_gt.concept_type
    )

    # because what if we have updated the ontology since user fetched original keyword_ground_truth
    latest_ontology_version_id: OntologyVersionIDType = ontology_info[0]
    known_concept_labels: set[str] = {c.name for c in ontology_info[1]}

    if latest_ontology_version_id != keyword_gt.ontology_version_id:
        raise ValueError(
            f"Ontology version ID mismatch for concept type '{keyword_gt.concept_type}'. Expected: {latest_ontology_version_id}, got: {keyword_gt.ontology_version_id}."
        )

    _validate_new_human_correction(keyword_gt, new_correction, known_concept_labels)


def _validate_new_human_correction(
    keyword_gt: KeywordGroundTruth,
    new_correction: HumanCorrection,
    known_concept_labels: set[str],
):
    """
    VALIDATE RESULT CORRECTION

    result_correction.add:
    - each key in add must be a known concept present in the latest ontology version
    - each value must be present in keyword_gt.chunk_text
    - each value must not be already present in keyword_gt.chunk_search_stats.mapping
    - each value must not be already present in any of the older human correction logs

    result_correction.remove:
    - each item in remove must be present in keyword_gt.chunk_search_stats.results
    - each item in remove must not be already present in any of the older human correction logs
    """

    if not new_correction:
        raise ValueError(
            "result_correction must be provided to validate the keyword ground truth."
        )
    existing_human_correction_logs = keyword_gt.human_correction_logs[:-1]

    for mk, mus in new_correction.add.items():
        logger.debug(f"Checking result_correction.add for key: {mk} with terms: {mus}")
        if not mus:
            raise ValueError(
                f"Key '{mk}' in the object result_correction.add cannot have an empty list of terms."
            )

        logger.debug(f"mk in known_concept_labels: {mk in known_concept_labels}")
        # Check if mk is a known concept in the latest ontology version
        if mk not in known_concept_labels:
            raise ValueError(
                f"Key '{mk}' in the object result_correction.add is not a known concept in the latest ontology version. "
                f"Please visit {get_full_ontology_concept_flat_url(keyword_gt.concept_type)}"
            )
        # Check if each value in result_correction.add is present in chunk_text
        for mu in mus:
            if not re.search(keyword_regex(mu), keyword_gt.chunk_text):
                raise ValueError(
                    f"The term '{mu}' in the list result_correction.add['{mk}'] is not present in chunk_text."
                )

        # Find which mk to mus mapping is already present in chunk_search_stats.mapping
        for mu in mus:
            if mu in keyword_gt.chunk_search_stats.mapping.get(mk, []):
                raise ValueError(
                    f"The term '{mu}' in the list result_correction.add['{mk}'] was already mapped by LLM and is present in chunk_search_stats.mapping. Please remove the term '{mu}' from the add mapping or provide a different term."
                )

            # Check if mu is already present in any of the existing human correction logs
            for log in existing_human_correction_logs:
                if mu in log.result_correction.add.get(mk, []):
                    raise ValueError(
                        f"The term '{mu}' in the list result_correction.add['{mk}'] is already present in a later human correction log added by {log.result_correction.author_email}."
                    )

    # Find which items in result_correction.remove are not present in chunk_search_stats.results
    existing_final_results = calculate_final_results(
        keyword_gt
    )  # ensure final_results is computed before checking remove items
    if existing_final_results is None:
        existing_final_results = []  # dummy
    invalid_remove_items = [
        item for item in new_correction.remove if item not in existing_final_results
    ]
    if invalid_remove_items:
        raise ValueError(
            f"The items:{invalid_remove_items} in result_correction.remove are not present in computed `final_results`: {existing_final_results}."
        )
