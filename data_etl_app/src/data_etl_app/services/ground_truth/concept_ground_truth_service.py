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
from shared.models.field_types import OntologyVersionIDType, S3FileVersionIDType

from data_etl_app.models.concept_extraction_results import ConceptExtractionResults
from data_etl_app.models.db.concept_ground_truth import (
    ConceptGroundTruth,
    ConceptResultCorrection,
    ConceptResultCorrectionLog,
)
from data_etl_app.models.skos_concept import Concept
from data_etl_app.utils.route_url_util import (
    get_full_ontology_concept_flat_url,
)
from data_etl_app.services.knowledge.ontology_service import ontology_service
from data_etl_app.services.brute_search_service import word_regex
from data_etl_app.models.types_and_enums import ConceptTypeEnum


async def get_extracted_concept_ground_truth(
    manufacturer: Manufacturer,
    concept_type: ConceptTypeEnum,
    chunk_no: int,
) -> ConceptGroundTruth | None:
    """
    Fetch the concept ground truth for a given manufacturer URL, concept type, and chunk number.
    Check if the scraped text file version ID and ontology version ID match the manufacturer's
    most recently scraped and extracted data.

    Assumed concept_type is a valid ConceptTypeEnum value and present in manufacturer's data.
    """
    concept_extraction_results: ConceptExtractionResults | None = getattr(
        manufacturer, concept_type, None
    )
    assert (
        concept_extraction_results is not None
    ), f"Concept type '{concept_type}' not found in manufacturer '{manufacturer.etld1}'."
    return await ConceptGroundTruth.find_one(
        ConceptGroundTruth.mfg_etld1 == manufacturer.etld1,
        ConceptGroundTruth.concept_type == concept_type,
        # ------------------ knowledge ids ------------------- #
        ConceptGroundTruth.scraped_text_file_version_id
        == manufacturer.scraped_text_file_version_id,
        ConceptGroundTruth.ontology_version_id
        == concept_extraction_results.stats.ontology_version_id,
        ConceptGroundTruth.extract_prompt_version_id
        == concept_extraction_results.stats.extract_prompt_version_id,
        ConceptGroundTruth.map_prompt_version_id
        == concept_extraction_results.stats.map_prompt_version_id,
        # ---------------------------------------------------- #
        ConceptGroundTruth.chunk_no == chunk_no,
    )


async def does_a_cgt_exist_with_scraped_file_version(
    scraped_file_version_id: S3FileVersionIDType,
) -> ConceptGroundTruth | None:
    """
    Fetch the concept ground truth for a given manufacturer URL.
    Check if the scraped text file version ID matches the manufacturer's
    most recently scraped and extracted data.

    """
    return await ConceptGroundTruth.find_one(
        ConceptGroundTruth.scraped_text_file_version_id == scraped_file_version_id
    )


async def add_new_correction_to_concept_ground_truth(
    timestamp: datetime,
    manufacturer: Manufacturer,
    existing_concept_gt: ConceptGroundTruth,
    new_correction: ConceptResultCorrection,
    s3_client,
) -> ConceptGroundTruth:

    existing_concept_gt.updated_at = timestamp
    if (
        existing_concept_gt.correction_logs[-1].result_correction.author_email
        == new_correction.author_email
    ):
        existing_concept_gt.correction_logs.pop()  # the new correction will replace the last one because it is from the same author

    await _validate_concept_ground_truth(
        manufacturer, existing_concept_gt, new_correction, s3_client
    )

    existing_concept_gt.correction_logs.append(
        ConceptResultCorrectionLog(
            created_at=existing_concept_gt.created_at,
            result_correction=new_correction,
        )
    )

    logger.debug(
        f"Updating existing concept ground truth {existing_concept_gt} to the database."
    )
    return await existing_concept_gt.save()


async def save_new_concept_ground_truth(
    timestamp: datetime,
    manufacturer: Manufacturer,
    new_concept_gt: ConceptGroundTruth,
    new_correction: ConceptResultCorrection,
    s3_client,
) -> ConceptGroundTruth:
    """
    Prepare a new concept ground truth instance with the provided timestamp.
    This function is used when creating a new concept ground truth entry.
    """

    new_concept_gt.created_at = timestamp
    new_concept_gt.updated_at = timestamp

    await _validate_concept_ground_truth(
        manufacturer, new_concept_gt, new_correction, s3_client
    )

    new_concept_gt.correction_logs.append(
        ConceptResultCorrectionLog(
            created_at=new_concept_gt.created_at,
            result_correction=new_correction,
        )
    )

    logger.debug(
        f"Inserting new concept ground truth {new_concept_gt} to the database."
    )
    return await new_concept_gt.save()


async def _validate_concept_ground_truth(
    manufacturer: Manufacturer,
    concept_gt: ConceptGroundTruth,
    new_correction: ConceptResultCorrection,
    s3_client,
) -> None:
    """
    Validate and save the concept ground truth to the database.

    concept_ground_truth passed may be a new or existing instance.

    Note:
    - Make sure to set created_at and updated_at beforehand.
    - Ensure that the manufacturer is prevalidated and exists in the database.
    """

    concept_gt = ConceptGroundTruth.model_validate(concept_gt.model_dump())

    # concept_data check
    concept_extraction_results = getattr(manufacturer, concept_gt.concept_type, None)
    if not concept_extraction_results:
        raise ValueError(
            f"No extraction results found for concept type '{concept_gt.concept_type}'"
            f" in manufacturer '{concept_gt.mfg_etld1}'."
        )

    assert (
        type(concept_extraction_results) is ConceptExtractionResults
    ), f"Expected concept_extraction_results to be of type ExtractionResults, got {type(concept_extraction_results)}."

    # chunk bounds and stats check
    chunk_bounds, chunk_search_stats = [
        (cb, css)
        for cb, css in sorted(
            concept_extraction_results.stats.chunked_stats.items(),
            key=lambda item: item[0],
        )
    ][concept_gt.chunk_no - 1]

    if concept_gt.chunk_bounds != chunk_bounds:
        raise ValueError(
            f"Chunk bounds '{concept_gt.chunk_bounds}' do not match the expected bounds '{chunk_bounds}' for chunk number {concept_gt.chunk_no}."
        )

    if concept_gt.chunk_search_stats != chunk_search_stats:
        raise ValueError(
            "Chunk search stats do not match the expected stats for the given chunk bounds."
        )

    # file and version ID check
    scraped_text, version_id = await download_scraped_text_from_s3_by_filename(
        s3_client,
        file_name=get_file_name_from_mfg_etld(concept_gt.mfg_etld1),
        version_id=manufacturer.scraped_text_file_version_id,
    )
    if manufacturer.scraped_text_file_version_id != version_id:
        raise ValueError(
            f"Scraped text version ID mismatch for mfg_url: {concept_gt.mfg_etld1}. Expected: {manufacturer.scraped_text_file_version_id}, got: {version_id}. (manufacturer.scraped_text_file_version_id != version_id)"
        )
    if concept_gt.scraped_text_file_version_id != version_id:
        raise ValueError(
            f"Scraped text version ID mismatch for concept ground truth. Expected: {version_id}, got: {concept_gt.scraped_text_file_version_id}. (concept_ground_truth.scraped_text_file_version_id != version_id)"
        )
    if (
        manufacturer.scraped_text_file_version_id
        != concept_gt.scraped_text_file_version_id
    ):
        raise ValueError(
            f"Scraped text version ID mismatch for concept ground truth. Expected: {manufacturer.scraped_text_file_version_id}, got: {concept_gt.scraped_text_file_version_id}. (manufacturer.scraped_text_file_version_id != concept_ground_truth.scraped_text_file_version_id)"
        )

    # chunk text check
    start, end = map(int, concept_gt.chunk_bounds.split(":"))
    if start < 0 or end > len(scraped_text):
        raise ValueError(
            f"Chunk bounds {concept_gt.chunk_bounds} are out of range for the scraped text."
        )

    if concept_gt.chunk_text != scraped_text[start:end]:
        raise ValueError(
            "Chunk text does not match the expected text for the given chunk bounds."
        )

    # ontology version ID check in case it is different from the latest version
    ontology_info: tuple[OntologyVersionIDType, list[Concept]] = getattr(
        ontology_service, concept_gt.concept_type
    )

    # because what if we have updated the ontology since user fetched original concept_ground_truth
    # this blocks users from submitting corrections on old ontology versions
    latest_ontology_version_id: OntologyVersionIDType = ontology_info[0]
    known_concept_labels: set[str] = {c.name for c in ontology_info[1]}

    if latest_ontology_version_id != concept_gt.ontology_version_id:
        raise ValueError(
            f"Ontology version ID mismatch for concept type '{concept_gt.concept_type}'. Expected: {latest_ontology_version_id}, got: {concept_gt.ontology_version_id}."
        )

    _validate_new_human_correction(concept_gt, new_correction, known_concept_labels)


# always call before adding the new correction to correction_logs
def _validate_new_human_correction(
    chunk_concept_gt: ConceptGroundTruth,
    new_correction: ConceptResultCorrection,
    known_concept_labels: set[str],
):
    """
    VALIDATE RESULT CORRECTION

    result_correction.add: (keys are concept names, values are list of unknowns present in chunk_text)
    - each key in add must be a known concept present in the latest ontology version
    - each value must be present in chunk_concept_gt.chunk_text
    - any key, value pair must not be already present in chunk_concept_gt.chunk_search_stats.mapping | value by itself can be present for a different key, hence the correction

    result_correction.remove:
    - each item in remove must be present in chunk_concept_gt.chunk_search_stats.results
    """

    if not new_correction:
        raise ValueError(
            "result_correction must be provided to validate the concept ground truth."
        )

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
                f"Please visit {get_full_ontology_concept_flat_url(chunk_concept_gt.concept_type)}"
            )
        # Check if each value in result_correction.add is present in chunk_text
        for mu in mus:
            if not re.search(word_regex(mu), chunk_concept_gt.chunk_text):
                raise ValueError(
                    f"The term '{mu}' in the list result_correction.add['{mk}'] is not present in chunk_text."
                )

        # Find which mk to mus mapping is already present in chunk_search_stats.mapping
        for mu in mus:
            if mu in chunk_concept_gt.chunk_search_stats.mapping.get(mk, []):
                raise ValueError(
                    f"The term '{mu}' in the list result_correction.add['{mk}'] was already mapped by LLM and is present in chunk_search_stats.mapping. Please remove the term '{mu}' from the add mapping or provide a different term."
                )

    for rm in new_correction.remove:
        logger.debug(f"Checking result_correction.remove for term: {rm}")
        if rm not in chunk_concept_gt.chunk_search_stats.results:
            raise ValueError(
                f"The term '{rm}' in the list result_correction.remove is not present in chunk_search_stats.results. Please remove the term '{rm}' from the remove list or provide a different term."
            )
