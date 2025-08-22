from datetime import datetime
import logging

from shared.models.db.manufacturer import Manufacturer

from data_etl_app.models.types import BinaryClassificationTypeEnum
from data_etl_app.models.binary_ground_truth import (
    BinaryGroundTruth,
    HumanDecisionLog,
    HumanBinaryDecision,
)
from shared.services.manufacturer_service import (
    find_manufacturer_by_url_and_scraped_file_version,
)

logger = logging.getLogger(__name__)


async def get_binary_ground_truth(
    manufacturer: Manufacturer,
    classification_type: BinaryClassificationTypeEnum,
) -> BinaryGroundTruth | None:
    """
    Fetch the binary ground truth for a given manufacturer URL.
    Check if the scraped text file version ID matches the manufacturer's
    most recently scraped and extracted data.

    """
    return await BinaryGroundTruth.find_one(
        BinaryGroundTruth.mfg_etld1 == manufacturer.etld1,
        BinaryGroundTruth.scraped_text_file_version_id
        == manufacturer.scraped_text_file_version_id,
        BinaryGroundTruth.classification_type == classification_type,
    )


async def update_existing_with_new_binary_ground_truth(
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

    await _validate_binary_ground_truth(
        new_human_decision=new_human_decision,
        binary_ground_truth=existing_binary_gt,
    )

    existing_binary_gt.human_decision_logs.append(
        HumanDecisionLog(
            created_at=existing_binary_gt.created_at,
            human_decision=new_human_decision,
        )
    )

    logger.debug(f"Updating existing binary ground truth {existing_binary_gt}")
    return await existing_binary_gt.save()


async def save_new_binary_ground_truth(
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
        new_human_decision=new_human_decision,
        binary_ground_truth=binary_gt,
    )

    binary_gt.human_decision_logs.append(
        HumanDecisionLog(
            created_at=binary_gt.created_at,
            human_decision=new_human_decision,
        )
    )

    logger.debug(f"Inserting new binary ground truth {binary_gt}")
    return await binary_gt.save()


async def _validate_binary_ground_truth(
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

    binary_ground_truth = BinaryGroundTruth.model_validate(
        binary_ground_truth.model_dump()
    )

    # existing manufacturer check
    existing_manufacturer = await find_manufacturer_by_url_and_scraped_file_version(
        binary_ground_truth.mfg_etld1, binary_ground_truth.scraped_text_file_version_id
    )
    if not existing_manufacturer:
        raise ValueError(
            f"Manufacturer with URL '{binary_ground_truth.mfg_etld1}' does not exist or has a different scraped text file version ID."
        )

    _validate_new_human_decision(new_human_decision, binary_ground_truth)


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
