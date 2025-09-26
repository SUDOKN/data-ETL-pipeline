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


from data_etl_app.models.concept_extraction_results import ConceptExtractionResults
from data_etl_app.models.generic_extraction_results import GenericExtractionResults
from data_etl_app.models.keyword_extraction_results import KeywordExtractionResults
from data_etl_app.models.db.generic_ground_truth import (
    GenericGroundTruth,
    GenericResultCorrection,
    GenericResultCorrectionLog,
)

from data_etl_app.services.knowledge.ontology_service import (
    ontology_service,
    Concept,
    OntologyVersionIDType,
)
from data_etl_app.services.brute_search_service import word_regex
from data_etl_app.models.types_and_enums import (
    GenericFieldTypeEnum,
    KeywordTypeEnum,
    ConceptTypeEnum,
)


async def get_generic_ground_truth(
    manufacturer: Manufacturer,
    generic_field_type: GenericFieldTypeEnum,
) -> GenericGroundTruth | None:
    """
    Fetch the generic ground truth for a given manufacturer URL and generic type.
    Check if the scraped text file version ID and ontology version ID match the manufacturer's
    most recently scraped and extracted data.

    Assumed generic_type is a valid GenericFieldTypeEnum value and present in manufacturer's data.
    """
    extracted_generic_data: GenericExtractionResults | None = getattr(
        manufacturer, generic_field_type, None
    )
    assert (
        extracted_generic_data is not None
    ), f"Generic field type '{generic_field_type}' not found in manufacturer '{manufacturer.etld1}'."
    return await GenericGroundTruth.find_one(
        GenericGroundTruth.mfg_etld1 == manufacturer.etld1,
        GenericGroundTruth.field_type == generic_field_type,
        # ------------------ knowledge ids ------------------- #
        GenericGroundTruth.scraped_text_file_version_id
        == manufacturer.scraped_text_file_version_id,
        GenericGroundTruth.extract_prompt_version_id
        == extracted_generic_data.stats.extract_prompt_version_id,
    )


async def add_new_correction_to_generic_ground_truth(
    timestamp: datetime,
    manufacturer: Manufacturer,
    existing_generic_gt: GenericGroundTruth,
    new_correction: GenericResultCorrection,
    s3_client,
) -> GenericGroundTruth:

    existing_generic_gt.updated_at = timestamp
    if (
        existing_generic_gt.correction_logs[-1].result_correction.author_email
        == new_correction.author_email
    ):
        existing_generic_gt.correction_logs.pop()  # the new correction will replace the last one because it is from the same author

    await _validate_generic_ground_truth(
        manufacturer, existing_generic_gt, new_correction, s3_client
    )

    existing_generic_gt.correction_logs.append(
        GenericResultCorrectionLog(
            created_at=existing_generic_gt.created_at,
            result_correction=new_correction,
        )
    )

    logger.debug(
        f"Updating existing generic ground truth {existing_generic_gt} to the database."
    )
    return await existing_generic_gt.save()


async def save_new_generic_ground_truth(
    timestamp: datetime,
    manufacturer: Manufacturer,
    new_generic_gt: GenericGroundTruth,
    new_correction: GenericResultCorrection,
    s3_client,
) -> GenericGroundTruth:
    """
    Prepare a new generic ground truth instance with the provided timestamp.
    This function is used when creating a new generic ground truth entry.
    """

    new_generic_gt.created_at = timestamp
    new_generic_gt.updated_at = timestamp

    await _validate_generic_ground_truth(
        manufacturer, new_generic_gt, new_correction, s3_client
    )

    new_generic_gt.correction_logs.append(
        GenericResultCorrectionLog(
            created_at=new_generic_gt.created_at,
            result_correction=new_correction,
        )
    )

    logger.debug(
        f"Inserting new generic ground truth {new_generic_gt} to the database."
    )
    return await new_generic_gt.save()


async def _validate_generic_ground_truth(
    manufacturer: Manufacturer,
    generic_gt: GenericGroundTruth,
    new_correction: GenericResultCorrection,
    s3_client,
) -> None:
    """
    Validate and save the generic ground truth to the database.

    generic_ground_truth passed may be a new or existing instance.

    Note:
    - Make sure to set created_at and updated_at beforehand.
    - Ensure that the manufacturer is prevalidated and exists in the database.
    """

    generic_gt = GenericGroundTruth.model_validate(generic_gt.model_dump())

    # generic_data check
    extracted_generic_data = getattr(manufacturer, generic_gt.generic_type, None)
    if not extracted_generic_data:
        raise ValueError(
            f"No extraction results found for generic type '{generic_gt.generic_type}'"
            f" in manufacturer '{generic_gt.mfg_etld1}'."
        )

    assert (
        type(extracted_generic_data) is GenericExtractionResults
    ), f"Expected extracted_generic_data to be of type GenericExtractionResults, got {type(extracted_generic_data)}."

    # file and version ID check
    _scraped_text, version_id = await download_scraped_text_from_s3_by_filename(
        s3_client, file_name=get_file_name_from_mfg_etld(generic_gt.mfg_etld1)
    )
    if manufacturer.scraped_text_file_version_id != version_id:
        raise ValueError(
            f"Scraped text version ID mismatch for mfg_url: {generic_gt.mfg_etld1}. Expected: {manufacturer.scraped_text_file_version_id}, got: {version_id}. (manufacturer.scraped_text_file_version_id != version_id)"
        )
    if generic_gt.scraped_text_file_version_id != version_id:
        raise ValueError(
            f"Scraped text version ID mismatch for generic ground truth. Expected: {version_id}, got: {generic_gt.scraped_text_file_version_id}. (generic_ground_truth.scraped_text_file_version_id != version_id)"
        )
    if (
        manufacturer.scraped_text_file_version_id
        != generic_gt.scraped_text_file_version_id
    ):
        raise ValueError(
            f"Scraped text version ID mismatch for generic ground truth. Expected: {manufacturer.scraped_text_file_version_id}, got: {generic_gt.scraped_text_file_version_id}. (manufacturer.scraped_text_file_version_id != generic_ground_truth.scraped_text_file_version_id)"
        )

    # because what if we have updated the ontology since user fetched original concept_ground_truth
    # this blocks users from submitting corrections on old ontology versions
    if generic_gt.field_type in ConceptTypeEnum:
        concept_extraction_results: ConceptExtractionResults | None = getattr(
            manufacturer, generic_gt.field_type, None
        )
        # extract_prompt_version_id check
        if not concept_extraction_results:
            raise ValueError(
                f"No extraction results found for concept type '{generic_gt.field_type}'"
                f" in manufacturer '{generic_gt.mfg_etld1}'."
            )

        if (
            concept_extraction_results.stats.extract_prompt_version_id
            != generic_gt.extract_prompt_version_id
        ):
            raise ValueError(
                f"Extract prompt version ID mismatch for concept type '{generic_gt.field_type}'. Expected: {concept_extraction_results.stats.extract_prompt_version_id}, got: {generic_gt.extract_prompt_version_id}."
            )

        # map_prompt_version_id check
        if (
            concept_extraction_results.stats.map_prompt_version_id
            != generic_gt.map_prompt_version_id
        ):
            raise ValueError(
                f"Map prompt version ID mismatch for concept type '{generic_gt.field_type}'. Expected: {concept_extraction_results.stats.map_prompt_version_id}, got: {generic_gt.map_prompt_version_id}."
            )

        # ontology version ID check in case it is different from the latest version
        ontology_info: tuple[OntologyVersionIDType, list[Concept]] = getattr(
            ontology_service, generic_gt.field_type
        )

        latest_ontology_version_id: OntologyVersionIDType = ontology_info[0]

        if latest_ontology_version_id != generic_gt.ontology_version_id:
            raise ValueError(
                f"Ontology version ID mismatch for concept type '{generic_gt.concept_type}'. Expected: {latest_ontology_version_id}, got: {generic_gt.ontology_version_id}."
            )
    elif generic_gt.field_type in KeywordTypeEnum:
        keyword_extraction_results: KeywordExtractionResults | None = getattr(
            manufacturer, generic_gt.field_type, None
        )
        if not keyword_extraction_results:
            raise ValueError(
                f"No extraction results found for keyword type '{generic_gt.field_type}'"
                f" in manufacturer '{generic_gt.mfg_etld1}'."
            )
        # only extract_prompt_version_id check
        if (
            keyword_extraction_results.stats.extract_prompt_version_id
            != generic_gt.extract_prompt_version_id
        ):
            raise ValueError(
                f"Extract prompt version ID mismatch for keyword type '{generic_gt.field_type}'. Expected: {keyword_extraction_results.stats.extract_prompt_version_id}, got: {generic_gt.extract_prompt_version_id}."
            )
    else:
        raise ValueError(
            f"Invalid field_type '{generic_gt.field_type}' for generic ground truth."
        )

    _validate_new_human_correction(generic_gt, new_correction)


def _validate_new_human_correction(
    chunk_generic_gt: GenericGroundTruth,
    new_correction: GenericResultCorrection,
):
    """
    VALIDATE RESULT CORRECTION

    result_correction.add: (list of generics present in chunk_text)
    - each value must not be already present in chunk_generic_gt.chunk_search_stats.results

    result_correction.remove:
    - each item in remove must already be present in chunk_generic_gt.chunk_search_stats.results
    """

    if not new_correction:
        raise ValueError(
            "result_correction must be provided to validate the generic ground truth."
        )

    for kw in new_correction.add:
        if not re.search(word_regex(kw), chunk_generic_gt.chunk_text):
            raise ValueError(
                f"Generic '{kw}' to be added is not present in the chunk text."
            )
        if kw in chunk_generic_gt.chunk_search_stats.results:
            raise ValueError(
                f"Generic '{kw}' to be added is already present in the extracted results."
            )

    for kw in new_correction.remove:
        if kw not in chunk_generic_gt.chunk_search_stats.results:
            raise ValueError(
                f"Generic '{kw}' to be removed is not present in the extracted results."
            )
