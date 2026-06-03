import re
import logging
from datetime import datetime


from core.models.db.manufacturer import Manufacturer
from core.models.field_types import (
    LLMEvidenceResults,
    OntologyVersionIDType,
    S3FileVersionIDType,
)
from core.models.concept_extraction_results import ConceptExtractionResults
from core.models.db.concept_ground_truth import (
    ConceptGroundTruth,
    ConceptCorrectionLog,
    HumanConceptCorrection,
)
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.types_and_enums import ConceptTypeEnum

from data_etl_app.services.knowledge.ontology_service import get_ontology_service
from data_etl_app.services.brute_search_service import word_regex

from core.utils.aws.s3.scraped_text_util import (
    download_scraped_text_from_s3_by_mfg_etld1,
)
from data_etl_app.utils.route_url_util import (
    get_full_ontology_concept_flat_url,
)
from data_etl_app.utils.ground_truth_helper_util import (
    calculate_verified_concept_evidence_results,
    calculate_corrected_concept_results,
    is_evidence_reason_format_correct,
    is_mapping_reason_format_correct,
)

# Configure logger
logger = logging.getLogger(__name__)


async def get_extracted_concept_ground_truth(
    linked_manufacturer: Manufacturer,
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
        linked_manufacturer, concept_type, None
    )
    assert (
        concept_extraction_results is not None
    ), f"Concept type '{concept_type}' not found in manufacturer '{linked_manufacturer.etld1}'."
    return await ConceptGroundTruth.find_one(
        ConceptGroundTruth.mfg_etld1 == linked_manufacturer.etld1,
        ConceptGroundTruth.concept_type == concept_type,
        # ------------------ knowledge ids ------------------- #
        ConceptGroundTruth.scraped_text_file_version_id
        == linked_manufacturer.scraped_text_file_version_id,
        ConceptGroundTruth.metadata.ontology_version_id
        == concept_extraction_results.metadata.ontology_version_id,
        ConceptGroundTruth.metadata.search_prompt_version_id
        == concept_extraction_results.metadata.search_prompt_version_id,
        ConceptGroundTruth.metadata.evidence_prompt_version_id
        == concept_extraction_results.metadata.evidence_prompt_version_id,
        ConceptGroundTruth.metadata.mapping_prompt_version_id
        == concept_extraction_results.metadata.mapping_prompt_version_id,
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


async def add_correction_to_concept_ground_truth(
    linked_manufacturer: Manufacturer,
    existing_concept_gt: ConceptGroundTruth,
    new_correction: HumanConceptCorrection,
    timestamp: datetime,
) -> ConceptGroundTruth:

    existing_concept_gt.updated_at = timestamp
    if (
        existing_concept_gt.corrections[-1].human_correction.author_email
        == new_correction.author_email
    ):
        existing_concept_gt.corrections.pop()  # the new correction will replace the last one because it is from the same author

    await _validate_concept_ground_truth_correction(
        linked_manufacturer, existing_concept_gt, new_correction
    )

    existing_concept_gt.corrections.append(
        ConceptCorrectionLog(
            created_at=timestamp,
            human_correction=new_correction,
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
    new_correction: HumanConceptCorrection,
) -> ConceptGroundTruth:
    """
    Prepare a new concept ground truth instance with the provided timestamp.
    This function is used when creating a new concept ground truth entry.
    """

    new_concept_gt.created_at = timestamp
    new_concept_gt.updated_at = timestamp

    await _validate_concept_ground_truth_correction(
        manufacturer, new_concept_gt, new_correction
    )

    new_concept_gt.corrections.append(
        ConceptCorrectionLog(
            created_at=new_concept_gt.created_at,
            human_correction=new_correction,
        )
    )

    logger.debug(
        f"Inserting new concept ground truth {new_concept_gt} to the database."
    )
    return await new_concept_gt.save()


async def _validate_concept_ground_truth_correction(
    linked_manufacturer: Manufacturer,
    concept_gt: ConceptGroundTruth,
    new_correction: HumanConceptCorrection,
) -> None:
    """
    Validate and save the concept ground truth to the database.

    concept_ground_truth passed may be a new or existing instance.

    Note:
    - Make sure to set created_at and updated_at beforehand.
    - Ensure that the manufacturer is prevalidated and exists in the database.
    """

    # concept_data check
    concept_extraction_results = getattr(
        linked_manufacturer, concept_gt.concept_type, None
    )
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
            concept_extraction_results.chunk_stats.items(),
            key=lambda item: int(item[0].split(":")[0]),
        )
    ][concept_gt.chunk_no - 1]

    if concept_gt.chunk_bounds != chunk_bounds:
        raise ValueError(
            f"Chunk bounds '{concept_gt.chunk_bounds}' do not match the expected bounds '{chunk_bounds}' for chunk number {concept_gt.chunk_no}."
        )

    if concept_gt.extraction_stats != chunk_search_stats:
        raise ValueError(
            "Chunk search stats do not match the expected stats for the given chunk bounds."
        )

    # file and version ID check
    scraped_text, version_id = await download_scraped_text_from_s3_by_mfg_etld1(
        etld1=concept_gt.mfg_etld1,
        version_id=linked_manufacturer.scraped_text_file_version_id,
    )
    if linked_manufacturer.scraped_text_file_version_id != version_id:
        raise ValueError(
            f"Scraped text version ID mismatch for mfg_url: {concept_gt.mfg_etld1}. Expected: {linked_manufacturer.scraped_text_file_version_id}, got: {version_id}. (manufacturer.scraped_text_file_version_id != version_id)"
        )
    if concept_gt.scraped_text_file_version_id != version_id:
        raise ValueError(
            f"Scraped text version ID mismatch for concept ground truth. Expected: {version_id}, got: {concept_gt.scraped_text_file_version_id}. (concept_ground_truth.scraped_text_file_version_id != version_id)"
        )
    if (
        linked_manufacturer.scraped_text_file_version_id
        != concept_gt.scraped_text_file_version_id
    ):
        raise ValueError(
            f"Scraped text version ID mismatch for concept ground truth. Expected: {linked_manufacturer.scraped_text_file_version_id}, got: {concept_gt.scraped_text_file_version_id}. (manufacturer.scraped_text_file_version_id != concept_ground_truth.scraped_text_file_version_id)"
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
    ontology_service = await get_ontology_service()
    ontology_info: tuple[OntologyVersionIDType, list[Concept]] = getattr(
        ontology_service, concept_gt.concept_type
    )

    # because what if we have updated the ontology since user fetched original concept_ground_truth
    # this blocks users from submitting corrections on old ontology versions
    latest_ontology_version_id: OntologyVersionIDType = ontology_info[0]
    known_concepts: set[Concept] = set(ontology_info[1])

    if latest_ontology_version_id != concept_gt.metadata.ontology_version_id:
        raise ValueError(
            f"Ontology version ID mismatch for concept type '{concept_gt.concept_type}'. Expected: {latest_ontology_version_id}, got: {concept_gt.metadata.ontology_version_id}."
        )

    _validate_new_human_correction(concept_gt, new_correction, known_concepts)


# always call before adding the new correction to corrections
def _validate_new_human_correction(
    chunk_concept_gt: ConceptGroundTruth,
    new_correction: HumanConceptCorrection,
    known_concepts: set[Concept],
):

    if not new_correction:
        raise ValueError(
            "result_correction must be provided to validate the concept ground truth."
        )

    original_evidence_results = chunk_concept_gt.extraction_stats.llm_evidence
    skipped_terms = set(original_evidence_results.keys()) - set(
        new_correction.llm_evidence_correction.upsert.keys()
    )
    if skipped_terms:
        raise ValueError(
            f"The following terms were present in the original LLM evidence results but are missing in the new correction's llm_evidence_correction.upsert: {skipped_terms}. "
            f"Please provide corrections for these terms or leave the evidence unchanged."
        )

    # Check evidence corrections
    for unk, reason in new_correction.llm_evidence_correction.upsert.items():
        if not re.search(word_regex(unk), chunk_concept_gt.chunk_text, re.IGNORECASE):
            raise ValueError(
                f"The term '{unk}' in the list llm_evidence.upsert is not present in chunk_text."
            )
        if not reason:
            raise ValueError(
                f"The unknown term '{unk}' in llm_evidence.upsert must have a reason provided."
            )
        elif not is_evidence_reason_format_correct(reason):
            raise ValueError(
                f"The reason for the unknown term '{unk}' in llm_evidence.upsert must start with 'Yes, ' or 'No, '."
            )

    verified_concept_evidence_results: LLMEvidenceResults = (
        calculate_verified_concept_evidence_results(
            human_correction=new_correction,
        )
    )
    verified_concept_evidence_kws = set(verified_concept_evidence_results.keys())

    known_concept_labels: set[str] = {c.name for c in known_concepts}
    for mk, mu_dict in new_correction.llm_mapping_correction.upsert.items():
        logger.debug(
            f"Checking mapping_result_correction.upsert for key: {mk} with terms: {mu_dict}"
        )

        # Check if mk is a known concept in the latest ontology version
        if mk not in known_concept_labels:
            raise ValueError(
                f"Key '{mk}' in the object mapping_result_correction.add is not a known concept in the latest ontology version. "
                f"Please visit {get_full_ontology_concept_flat_url(chunk_concept_gt.concept_type)}"
            )
        # Check if each value in mapping_result_correction.add is present in chunk_text
        for mu in mu_dict:
            if mu not in verified_concept_evidence_kws:
                raise ValueError(
                    f"The term '{mu}' in the list mapping_result_correction.upsert['{mk}'] is not present in verified_concept_evidence_results."
                )
            elif not mu_dict[mu]:
                raise ValueError(
                    f"The unknown term '{mu}' in mapping_result_correction.upsert['{mk}'] must have a reason provided."
                )
            elif not is_mapping_reason_format_correct(mu_dict[mu]):
                raise ValueError(
                    f"The reason for the unknown term '{mu}' in mapping_result_correction.upsert['{mk}'] must start with 'Correct, ' or 'Incorrect, '."
                )


async def get_corrected_results(
    concept_gt: ConceptGroundTruth,
) -> list[str]:
    """
    Get the final results after applying human corrections.
    Returns None if no corrections were made.
    """

    ontology_service = await get_ontology_service()
    ontology_info: tuple[OntologyVersionIDType, list[Concept]] = getattr(
        ontology_service, concept_gt.concept_type
    )

    # because what if we have updated the ontology since user fetched original concept_ground_truth
    # this blocks users from submitting corrections on old ontology versions
    latest_ontology_version_id: OntologyVersionIDType = ontology_info[0]
    known_concepts: set[Concept] = set(ontology_info[1])
    if latest_ontology_version_id != concept_gt.metadata.ontology_version_id:
        raise ValueError(
            f"Ontology version ID mismatch for concept type '{concept_gt.concept_type}'. Expected: {latest_ontology_version_id}, got: {concept_gt.metadata.ontology_version_id}."
        )

    last_correction_log = concept_gt.corrections[-1] if concept_gt.corrections else None
    if not last_correction_log:
        return list(concept_gt.extraction_stats.results)
    else:
        return calculate_corrected_concept_results(
            known_concepts=known_concepts,
            human_correction=last_correction_log.human_correction,
        )
