import re
import logging
from datetime import datetime


from core.models.db.manufacturer import Manufacturer
from core.models.field_types import (
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
from core.services.out_of_vocab_labels_service import (
    get_case_matched_existing_label,
    get_out_of_vocab_labels,
    upsert_out_of_vocab_labels,
)
from data_etl_app.utils.ground_truth_helper_util import (
    CORRECT_PREFIX,
    INCORRECT_PREFIX,
    get_verified_results_from_human_distillation_correction,
    calculate_corrected_concept_results,
    is_distillation_evidence_format_correct,
    is_mapping_reason_format_correct,
    merge_llm_and_brute_search_results,
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
        ConceptGroundTruth.metadata.distillation_prompt_version_id
        == concept_extraction_results.metadata.distillation_prompt_version_id,
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

    out_of_vocab_keywords = await _validate_concept_ground_truth_correction(
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
    if out_of_vocab_keywords:
        await upsert_out_of_vocab_labels(
            ontology_version_id=existing_concept_gt.metadata.ontology_version_id,
            concept_type=existing_concept_gt.concept_type,
            new_labels=out_of_vocab_keywords,
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

    new_out_of_vocab_labels = await _validate_concept_ground_truth_correction(
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
    saved = await new_concept_gt.save()
    if new_out_of_vocab_labels:
        await upsert_out_of_vocab_labels(
            ontology_version_id=new_concept_gt.metadata.ontology_version_id,
            concept_type=new_concept_gt.concept_type,
            new_labels=new_out_of_vocab_labels,
        )
    return saved


async def _validate_concept_ground_truth_correction(
    linked_manufacturer: Manufacturer,
    concept_gt: ConceptGroundTruth,
    new_correction: HumanConceptCorrection,
) -> set[str]:
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

    # Load the specific ontology version that was used for extraction
    ontology_service = await get_ontology_service()
    ontology = await ontology_service.get_ontology(
        concept_gt.metadata.ontology_version_id
    )
    known_concepts: set[Concept] = getattr(ontology, concept_gt.concept_type)

    new_out_of_vocab_labels = await _validate_new_human_correction(
        concept_gt, new_correction, known_concepts
    )
    return new_out_of_vocab_labels


# always call before adding the new correction to corrections
async def _validate_new_human_correction(
    concept_gt: ConceptGroundTruth,
    new_correction: HumanConceptCorrection,
    known_concepts: set[Concept],
) -> set[str]:

    if not new_correction:
        raise ValueError(
            "new_correction must be provided to validate the concept ground truth."
        )

    # VERIFY LLM SEARCH
    for unk in new_correction.llm_search_correction.upsert:
        if not re.search(word_regex(unk), concept_gt.chunk_text, re.IGNORECASE):
            raise ValueError(
                f"The phrase '{unk}' in the list llm_search_correction.upsert is not present in chunk_text."
            )

    if (  # llm_search is add only, so llm_search_correction.upsert = extraction_stats.llm_search + any new phrases
        concept_gt.extraction_stats.llm_search
        - new_correction.llm_search_correction.upsert
    ):
        raise ValueError(
            f"The following phrases were present in the original LLM search results but were skipped in the new correction's llm_search_correction.upsert: {concept_gt.extraction_stats.llm_search - new_correction.llm_search_correction.upsert}."
        )

    # VERIFY distillation
    original_distillation_candidate_phrases = set(  # phrases originally present in the LLM distillation results, which is chunk_concept_gt.extraction_stats.llm_search | chunk_concept_gt.extraction_stats.brute_search
        concept_gt.extraction_stats.llm_distillation.keys()
    )
    logger.info(
        f"original_distillation_candidate_phrases: {original_distillation_candidate_phrases}"
    )
    corrected_distillation_candidate_phrases = merge_llm_and_brute_search_results(
        llm_search_results=new_correction.llm_search_correction.upsert,  # llm_search_correction.upsert = extraction_stats.llm_search + any new phrases added by human
        brute_search_results=concept_gt.extraction_stats.brute_search,  # brute_search phrases are also passed on to the distillation stage
    )
    logger.info(
        f"corrected_distillation_candidate_phrases: {corrected_distillation_candidate_phrases}"
    )
    if (
        original_distillation_candidate_phrases
        - corrected_distillation_candidate_phrases
    ):
        # none of the original phrases can be removed, llm_distillation is edit existing or add new only
        raise ValueError(
            f"The following phrases were present in the original LLM distillation results but are missing in the new correction's llm_distillation_correction.upsert: {original_distillation_candidate_phrases - corrected_distillation_candidate_phrases}. "
            f"Please provide corrections for these phrases or leave the distillation unchanged."
        )
    if corrected_distillation_candidate_phrases != set(
        new_correction.llm_distillation_correction.upsert.keys()
    ):
        raise ValueError(
            f"The phrases for your llm_distillation_correction must match the llm_search_correction.upsert + chunk_concept_gt.extraction_stats.brute_search. "
            f"However, the [llm_search_correction.upsert + chunk_concept_gt.extraction_stats.brute_search] contains phrases: {corrected_distillation_candidate_phrases - set(new_correction.llm_distillation_correction.upsert.keys())} that are missing in your llm_distillation_correction.upsert. "
            f"Or, the your llm_distillation_correction contains extra phrases: {set(new_correction.llm_distillation_correction.upsert.keys()) - corrected_distillation_candidate_phrases} that are not present in [llm_search_correction.upsert + chunk_concept_gt.extraction_stats.brute_search]. "
        )
    # Check distillation reasons
    for unk, reason in new_correction.llm_distillation_correction.upsert.items():
        if not reason:
            raise ValueError(
                f"The unknown phrase '{unk}' in llm_distillation_correction.upsert must have a reason provided."
            )
        elif not is_distillation_evidence_format_correct(reason):
            raise ValueError(
                f"The reason for the unknown phrase '{unk}' in llm_distillation_correction.upsert must start with 'Yes, ' or 'No, ' followed by an explanation."
            )

    # VERIFY MAPPING
    original_mapping_candidate_phrases = set(
        # phrases originally present in the LLM mapping results,
        # which is the keys of the llm_mapping dict, these phrases
        # had "Yes, " distillation originally
        concept_gt.extraction_stats.llm_mapping.keys()
    )
    if original_mapping_candidate_phrases - set(
        new_correction.llm_mapping_correction.upsert.keys()
    ):
        raise ValueError(
            f"The following phrases were present in the original LLM mapping results but are skipped in the new correction's llm_mapping_correction.upsert: {original_mapping_candidate_phrases - set(new_correction.llm_mapping_correction.upsert.keys())}. "
            f"Please provide corrections for these phrases or leave the mapping unchanged."
        )

    # corrected_mapping_candidate_phrases_w_evid must contain all of corrected_distillation_candidate_phrases
    corrected_mapping_candidate_phrases_w_evid = (
        set(  # These phrases are supported by reasons starting with "Yes, "
            get_verified_results_from_human_distillation_correction(
                human_correction=new_correction,
            ).keys()
        )
    )
    # in fact they must be equal
    logger.info(
        f"corrected_distillation_candidate_phrases: {corrected_distillation_candidate_phrases}"
    )
    logger.info(
        f"corrected_mapping_candidate_phrases_w_evid: {corrected_mapping_candidate_phrases_w_evid}"
    )
    if (
        new_correction.llm_mapping_correction.upsert.keys()
        != corrected_mapping_candidate_phrases_w_evid
    ):
        raise ValueError(
            f"The candidate phrases for llm_mapping_correction must match the verified distillation candidate phrases. "
            f"However, your provided llm_mapping_correction.upsert contains phrases {set(new_correction.llm_mapping_correction.upsert.keys()) - corrected_mapping_candidate_phrases_w_evid} that are absent in the verified distillation candidate phrases (i.e. those with 'Yes, ' reason). "
            f"Or, the corrected_mapping_candidate_phrases_w_evid contains phrases {corrected_mapping_candidate_phrases_w_evid - set(new_correction.llm_mapping_correction.upsert.keys())} that are missing in your llm_mapping_correction.upsert. "
            f"Please ensure phrases in mapping correction match the verified distillation candidate phrases (Beginning with 'Yes, ')."
        )

    known_concept_labels: set[str] = {c.name for c in known_concepts}
    new_out_of_vocab_labels: set[str] = set()

    existing_out_of_vocab_labels_doc = await get_out_of_vocab_labels(
        concept_type=concept_gt.concept_type,
        ontology_version_id=concept_gt.metadata.ontology_version_id,
    )

    for mu, mk_dict in new_correction.llm_mapping_correction.upsert.items():
        logger.debug(
            f"Checking mapping_result_correction.upsert for key: {mu} with phrases: {mk_dict}"
        )
        if mu not in corrected_mapping_candidate_phrases_w_evid:
            raise ValueError(
                f"The phrase '{mu}' in the list llm_mapping_correction.upsert keys is not present in corrected_mapping_candidate_phrases_w_evid."
            )
        elif not mk_dict and (mu not in original_mapping_candidate_phrases):
            # mk_dict can be empty only when the phrase was present in the original mapping results, which is allowed.
            # But if mk_dict is empty for a new phrase, then it's an error because new phrases must have at least one mapping reason provided.
            raise ValueError(
                f"The unknown phrase '{mu}' in llm_mapping_correction.upsert must have at least one mapping reason provided since it is a new addition."
            )

        # at this point
        # mk_dict can be empty iff mu was in original mapping results
        # if mk_dict is not empty, then mu is
        #       either a new phrase with mapping reasons provided,
        #       or an original phrase with updated mapping reasons,
        # both are allowed
        for mk in list(
            mk_dict
        ):  # mk_dict contains {mapped_known_concept_label: mapping_reason}
            reason = mk_dict[mk]
            if not reason:
                raise ValueError(
                    f"The out-of-vocabulary keyword '{mk}' in llm_mapping_correction.upsert['{mu}'] must have a reason provided."
                )
            elif not is_mapping_reason_format_correct(reason):
                raise ValueError(
                    f"The reason for the unknown phrase '{mk}' in llm_mapping_correction.upsert['{mu}'] must start with '{CORRECT_PREFIX}' or '{INCORRECT_PREFIX}'."
                )

            if mk not in known_concept_labels:
                # Out-of-vocabulary keyword: only accepted with a "Correct, " assertion.
                # These are tracked for future ontology expansion.
                logger.info(
                    f"Found out-of-vocabulary keyword '{mk}' in mapping correction for phrase '{mu}'."
                )
                if not reason.startswith(CORRECT_PREFIX):
                    raise ValueError(
                        f"Out-of-vocabulary keyword '{mk}' in llm_mapping_correction.upsert['{mu}'] must start with '{CORRECT_PREFIX}'. "
                        f"Keywords not in the ontology can only be submitted as correct mappings. "
                        f"To check the current vocabulary, visit {get_full_ontology_concept_flat_url(concept_gt.concept_type)}"
                    )
                logger.debug(
                    f"Accepted out-of-vocabulary keyword '{mk}' for concept type '{concept_gt.concept_type}'."
                )
                case_matched_existing_label = get_case_matched_existing_label(
                    existing_out_of_vocab_labels_doc, mk
                )
                if case_matched_existing_label:
                    # update mk with case_matched_existing_label in mk_dict
                    mk_dict[case_matched_existing_label] = mk_dict.pop(mk)
                    logger.info(
                        f"Out-of-vocabulary keyword '{mk}' already exists in the database with case-matched label '{case_matched_existing_label}'. "
                        f"Updated the mapping correction to use the existing label."
                    )
                else:
                    new_out_of_vocab_labels.add(mk)

    return new_out_of_vocab_labels


async def get_corrected_results(
    concept_gt: ConceptGroundTruth,
) -> list[str]:
    """
    Get the final results after applying human corrections.
    Returns None if no corrections were made.
    """

    # Load the specific ontology version that was used for extraction
    ontology_service = await get_ontology_service()
    ontology = await ontology_service.get_ontology(
        concept_gt.metadata.ontology_version_id
    )
    known_concepts: set[Concept] = getattr(ontology, concept_gt.concept_type)

    last_correction_log = concept_gt.corrections[-1] if concept_gt.corrections else None
    if not last_correction_log:
        return list(concept_gt.extraction_stats.results)
    else:
        return calculate_corrected_concept_results(
            known_concepts=known_concepts,
            human_correction=last_correction_log.human_correction,
        )
