from datetime import datetime
import logging

from core.models.db.manufacturer import Manufacturer
from core.models.field_types import (
    S3FileVersionIDType,
)

from data_etl_app.models.types_and_enums import BinaryClassificationTypeEnum
from core.models.db.binary_ground_truth import (
    BinaryGroundTruth,
    HumanDecisionLog,
    HumanBinaryDecision,
)
from core.utils.aws.s3.scraped_text_util import (
    download_scraped_text_from_s3_by_mfg_etld1,
)
from data_etl_app.utils.prompt_s3_util import (
    does_prompt_version_exist,
    get_prompt_filename,
)

logger = logging.getLogger(__name__)


async def get_binary_ground_truth(
    linked_manufacturer: Manufacturer,
    prompt_version_id: S3FileVersionIDType,
    classification_type: BinaryClassificationTypeEnum,
) -> BinaryGroundTruth | None:
    """
    Fetch the binary ground truth for a given manufacturer URL.
    Check if the scraped text file version ID matches the manufacturer's
    most recently scraped and extracted data.

    """
    return await BinaryGroundTruth.find_one(
        BinaryGroundTruth.mfg_etld1 == linked_manufacturer.etld1,
        BinaryGroundTruth.scraped_text_file_version_id
        == linked_manufacturer.scraped_text_file_version_id,
        BinaryGroundTruth.llm_decision.stats.prompt_version_id  # CAUTION: does not throw an error if stats doesn't exist
        == prompt_version_id,  # critical
        BinaryGroundTruth.classification_type == classification_type,
    )


async def does_a_bgt_exist_with_scraped_file_version(
    scraped_file_version_id: S3FileVersionIDType,
) -> BinaryGroundTruth | None:
    """
    Fetch the binary ground truth for a given manufacturer URL.
    Check if the scraped text file version ID matches the manufacturer's
    most recently scraped and extracted data.

    """
    return await BinaryGroundTruth.find_one(
        BinaryGroundTruth.scraped_text_file_version_id == scraped_file_version_id
    )


async def add_decision_to_binary_ground_truth(
    linked_manufacturer: Manufacturer,
    existing_binary_gt: BinaryGroundTruth,
    new_human_decision: HumanBinaryDecision,
    timestamp: datetime,
) -> BinaryGroundTruth:
    """
    Update the existing binary ground truth with the new one.

    This function assumes that the existing binary ground truth is already fetched
    and passed as an argument.
    """
    existing_binary_gt.updated_at = timestamp
    if (
        existing_binary_gt.human_decision_logs[-1].human_decision.author_email
        == new_human_decision.author_email
    ):
        existing_binary_gt.human_decision_logs.pop()  # the new decision will replace the last one because it is from the same author

    await _validate_binary_ground_truth(
        linked_manufacturer=linked_manufacturer,
        new_human_decision=new_human_decision,
        binary_ground_truth=existing_binary_gt,
    )

    existing_binary_gt.human_decision_logs.append(
        HumanDecisionLog(
            created_at=timestamp,
            human_decision=new_human_decision,
        )
    )

    logger.debug(f"Updating existing binary ground truth {existing_binary_gt}")
    return await existing_binary_gt.save()


async def save_new_binary_ground_truth(
    linked_manufacturer: Manufacturer,
    binary_gt: BinaryGroundTruth,
    new_human_decision: HumanBinaryDecision,
    timestamp: datetime,
) -> BinaryGroundTruth:
    """
    Prepare a new binary ground truth instance with the provided timestamp.
    This function is used when creating a new binary ground truth entry.
    """
    binary_gt.created_at = timestamp
    binary_gt.updated_at = timestamp

    await _validate_binary_ground_truth(
        linked_manufacturer=linked_manufacturer,
        new_human_decision=new_human_decision,
        binary_ground_truth=binary_gt,
    )

    binary_gt.human_decision_logs.append(
        HumanDecisionLog(
            created_at=timestamp,
            human_decision=new_human_decision,
        )
    )

    logger.debug(f"Inserting new binary ground truth {binary_gt}")
    return await binary_gt.save()


async def _validate_binary_ground_truth(
    linked_manufacturer: Manufacturer,
    new_human_decision: HumanBinaryDecision,
    binary_ground_truth: BinaryGroundTruth,
) -> None:
    """
    Validate and save the binary ground truth to the database.

    binary_ground_truth passed may be a new or existing instance.

    Note:
    Make sure to set created_at and updated_at beforehand.
    """

    if not new_human_decision:
        raise ValueError("human_decision must be provided if llm_decision is present.")

    # existing manufacturer check
    if linked_manufacturer.etld1 != binary_ground_truth.mfg_etld1:
        raise ValueError(
            f"Manufacturer etld1 mismatch: {linked_manufacturer.etld1} vs {binary_ground_truth.mfg_etld1}"
        )

    # check if scraped file exists
    await download_scraped_text_from_s3_by_mfg_etld1(
        etld1=binary_ground_truth.mfg_etld1,
        version_id=binary_ground_truth.scraped_text_file_version_id,
    )

    # check if prompt file exists
    prompt_filename = get_prompt_filename(
        f"{binary_ground_truth.classification_type.value}"
    )

    if not await does_prompt_version_exist(
        prompt_filename, binary_ground_truth.llm_decision.stats.prompt_version_id
    ):
        raise ValueError(f"Prompt file '{prompt_filename}' does not exist.")

    _validate_new_human_decision(
        new_human_decision=new_human_decision,
        binary_ground_truth=binary_ground_truth,
    )


def _validate_new_human_decision(
    new_human_decision: HumanBinaryDecision,
    binary_ground_truth: BinaryGroundTruth,
) -> None:
    """
    Validate the human decision against the LLM decision.
    If the human decision differs from the LLM decision, a reason must be provided.
    """
    if not new_human_decision or new_human_decision.answer is None:
        raise ValueError(
            "A Human decision must be provided if LLM decision is present."
        )

    logger.info(
        f"Validating human decision: {new_human_decision} vs LLM decision: {binary_ground_truth.llm_decision}"
    )

    if (
        new_human_decision.answer != binary_ground_truth.llm_decision.answer
        and not new_human_decision.reason
    ):
        raise ValueError(
            "Human decision, if different from LLM decision, must have a reason."
        )
