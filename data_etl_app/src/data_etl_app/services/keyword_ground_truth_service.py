import re

from shared.utils.aws.s3.scraped_text_util import (
    get_file_name_from_mfg_url,
    download_scraped_text_from_s3_by_filename,
)
from shared.models.db.manufacturer import Manufacturer
from shared.models.types import OntologyVersionIDType

from data_etl_app.models.keyword_ground_truth import KeywordGroundTruth
from data_etl_app.models.skos_concept import Concept
from data_etl_app.services.ontology_service import ontology_service
from data_etl_app.services.brute_search_service import keyword_regex
from data_etl_app.utils.ontology_rdf_util import does_ontology_version_exist


async def get_keyword_ground_truth(
    mfg_url: str,
    concept_type: str,
    chunk_no: int,
) -> KeywordGroundTruth | None:
    """
    Fetch the keyword ground truth for a given manufacturer URL, concept type, and chunk number.
    """
    return await KeywordGroundTruth.find_one(
        KeywordGroundTruth.mfg_url == mfg_url,
        KeywordGroundTruth.concept_type == concept_type,
        KeywordGroundTruth.chunk_no == chunk_no,
        sort=["-created_at"],
    )


async def save_keyword_ground_truth(
    keyword_ground_truth: KeywordGroundTruth, s3_client
) -> KeywordGroundTruth:
    """
    Validate and save the keyword ground truth to the database.
    """
    keyword_ground_truth = KeywordGroundTruth.model_validate(
        keyword_ground_truth.model_dump()
    )

    if not keyword_ground_truth.result_correction:
        raise ValueError(
            "result_correction must be provided to save the keyword ground truth."
        )

    # existing manufacturer and concept_data check
    existing_manufacturer = await Manufacturer.find_one(
        {"url": keyword_ground_truth.mfg_url, "is_manufacturer.answer": True}
    )
    if not existing_manufacturer:
        raise ValueError(
            f"Manufacturer with URL '{keyword_ground_truth.mfg_url}' does not exist or is not a valid manufacturer."
        )

    extracted_concept_data = getattr(
        existing_manufacturer, keyword_ground_truth.concept_type, None
    )
    if not extracted_concept_data:
        raise ValueError(
            f"Concept data for type '{keyword_ground_truth.concept_type}' does not exist in the manufacturer with URL '{keyword_ground_truth.mfg_url}'."
        )

    # chunk bounds and stats check
    chunk_bounds, chunk_search_stats = [
        (cb, css)
        for cb, css in sorted(
            extracted_concept_data.stats.search.items(), key=lambda item: item[0]
        )
    ][keyword_ground_truth.chunk_no - 1]
    if keyword_ground_truth.chunk_bounds != chunk_bounds:
        raise ValueError(
            f"Chunk bounds '{keyword_ground_truth.chunk_bounds}' do not match the expected bounds '{chunk_bounds}' for chunk number {keyword_ground_truth.chunk_no}."
        )
    if (
        keyword_ground_truth.chunk_search_stats != chunk_search_stats
    ):  # must be deep equality check
        raise ValueError(
            "Chunk search stats do not match the expected stats for the given chunk bounds."
        )

    # file and version ID check
    version_id, scraped_text = await download_scraped_text_from_s3_by_filename(
        s3_client, file_name=get_file_name_from_mfg_url(keyword_ground_truth.mfg_url)
    )
    if existing_manufacturer.scraped_text_file_version_id != version_id:
        raise ValueError(
            f"Scraped text version ID mismatch for mfg_url: {keyword_ground_truth.mfg_url}. Expected: {existing_manufacturer.scraped_text_file_version_id}, got: {version_id}. (existing_manufacturer.scraped_text_file_version_id != version_id)"
        )
    elif keyword_ground_truth.scraped_text_file_version_id != version_id:
        raise ValueError(
            f"Scraped text version ID mismatch for keyword ground truth. Expected: {version_id}, got: {keyword_ground_truth.scraped_text_file_version_id}. (keyword_ground_truth.scraped_text_file_version_id != version_id)"
        )
    elif (
        existing_manufacturer.scraped_text_file_version_id
        != keyword_ground_truth.scraped_text_file_version_id
    ):
        raise ValueError(
            f"Scraped text version ID mismatch for keyword ground truth. Expected: {existing_manufacturer.scraped_text_file_version_id}, got: {keyword_ground_truth.scraped_text_file_version_id}. (existing_manufacturer.scraped_text_file_version_id != keyword_ground_truth.scraped_text_file_version_id)"
        )

    # chunk text check
    start, end = map(int, keyword_ground_truth.chunk_bounds.split(":"))
    if start < 0 or end > len(scraped_text):
        raise ValueError(
            f"Chunk bounds {keyword_ground_truth.chunk_bounds} are out of range for the scraped text."
        )

    if keyword_ground_truth.chunk_text != scraped_text[start:end]:
        raise ValueError(
            "Chunk text does not match the expected text for the given chunk bounds."
        )

    # ontology version ID check in case it is different from the latest version
    ontology_info = getattr(ontology_service, keyword_ground_truth.concept_type)
    latest_ontology_version_id: OntologyVersionIDType = (
        ontology_info.latest_version_id
    )  # may be more recent than the one in keyword_ground_truth
    known_concept_labels: set[str] = {c.name for c in ontology_info.concepts}

    if latest_ontology_version_id != keyword_ground_truth.ontology_version_id:
        raise ValueError(
            f"Ontology version ID mismatch for concept type '{keyword_ground_truth.concept_type}'. Expected: {latest_ontology_version_id}, got: {keyword_ground_truth.ontology_version_id}."
        )

    validate_result_correction(keyword_ground_truth, known_concept_labels)
    await keyword_ground_truth.insert()
    return keyword_ground_truth


def validate_result_correction(
    keyword_ground_truth: KeywordGroundTruth,
    known_concept_labels: set[str],
):
    """
    VALIDATE RESULT CORRECTION

    result_correction.add:
    - each key in add must be a known concept present in the latest ontology version
    - each value must be present in keyword_ground_truth.chunk_text

    result_correction.remove:
    - each item in remove must be present in keyword_ground_truth.chunk_search_stats.results
    """
    if not keyword_ground_truth.result_correction:
        raise ValueError(
            "result_correction must be provided to validate the keyword ground truth."
        )

    for mk, mus in keyword_ground_truth.result_correction.add.items():
        # Check if mk is a known concept in the latest ontology version
        if mk not in known_concept_labels:
            raise ValueError(
                f"Key '{mk}' in result_correction.add is not a known concept in the latest ontology version."
            )
        # Check if each value in result_correction.add is present in chunk_text
        for mu in mus:
            if not re.search(keyword_regex(mu), keyword_ground_truth.chunk_text):
                raise ValueError(
                    f"Value '{mu}' in result_correction.add['{mk}'] is not present in chunk_text."
                )

    # Find which items in result_correction.remove are not present in chunk_search_stats.results
    invalid_remove_items = [
        item
        for item in keyword_ground_truth.result_correction.remove
        if item not in keyword_ground_truth.chunk_search_stats.results
    ]
    if invalid_remove_items:
        raise ValueError(
            f"The following items in result_correction.remove are not present in chunk_search_stats.results: {invalid_remove_items}"
        )
