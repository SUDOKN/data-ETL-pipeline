import json
import random
import logging
from fastapi import APIRouter, HTTPException, Request, Query, Depends
from fastapi.responses import JSONResponse

from core.models.db.user import User
from core.models.db.binary_ground_truth import (
    BinaryGroundTruth,
    HumanBinaryDecision,
)
from data_etl_app.models.pipeline_nodes.classification.binary_reconcile_node import (
    BinaryClassificationResult,
)
from data_etl_app.models.types_and_enums import (
    BinaryClassificationTypeEnum,
    GroundTruthSource,
)

from core.services.manufacturer_service import (
    find_manufacturer_by_etld1,
)
from core.services.user_service import find_by_email
from data_etl_app.services.ground_truth.binary_ground_truth_service import (
    get_binary_ground_truth,
    save_new_binary_ground_truth,
    add_decision_to_binary_ground_truth,
)

from core.utils.time_util import get_current_time
from core.utils.aws.s3.scraped_text_util import (
    download_scraped_text_from_s3_by_mfg_etld1,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get(
    "/ground_truth/binary-classification/template",
    response_class=JSONResponse,
)
async def fetch_binary_classification_user_form_template(
    author_email: str = Query(
        description=(
            f"Author email query param is required. This is used to generate customized template for you. "
            f"To add your email as query param, simply append the URL with `?author_email=your_email@example.com`. "
            f"If you are using postman, you can use the `Params` tab to add a query param."
        ),
    ),
    mfg_etld1: str = Query(description="Manufacturer effective top-level domain"),
    classification_type: BinaryClassificationTypeEnum = Query(
        default=random.choice(list(BinaryClassificationTypeEnum)),
        description=f"Any one of {[concept.value for concept in BinaryClassificationTypeEnum]}",
    ),
):

    user = await find_by_email(author_email)
    if not user:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sorry, no registered user found with email: {author_email}. "
                f"Please register on `sudokn.com` to fetch binary classification ground truth template."
            ),
        )

    manufacturer = await find_manufacturer_by_etld1(mfg_etld1)
    if not manufacturer:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sorry, no manufacturer found with effective top-level domain: {mfg_etld1}. "
                f"Please provide a valid mfg_etld1."
            ),
        )

    binary_classification_result = getattr(
        manufacturer, classification_type.value, None
    )
    if binary_classification_result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No LLM decision found for classification type: {classification_type.value} for manufacturer with etld1: {mfg_etld1}.",
        )
    elif type(binary_classification_result) is not BinaryClassificationResult:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected data type for LLM decision: {type(binary_classification_result)}. Expected BinaryClassificationResult.",
        )

    existing_binary_gt = await get_binary_ground_truth(
        linked_manufacturer=manufacturer,
        prompt_version_id=binary_classification_result.metadata.prompt_version_id,
        classification_type=classification_type,
    )
    if existing_binary_gt:
        response = existing_binary_gt.model_dump()
        response["new_human_decision"] = HumanBinaryDecision(
            author_email=author_email,
            answer=existing_binary_gt.final_decision.answer,
            reason=existing_binary_gt.final_decision.reason,
            source=GroundTruthSource.USER_FORM,
        )
        logger.debug(f"Existing binary ground truth found: {response}")
        response.pop("id", None)  # remove id from response
        return response

    logger.debug(
        f"No existing binary ground truth found for mfg_etld1: {mfg_etld1}, classification_type: {classification_type}. Creating new template."
    )
    # if not, then we need to create a new binary ground truth

    first_chunk_bounds, first_chunk_stats = list(
        binary_classification_result.chunk_stats.items()
    )[0]
    start, end = first_chunk_bounds.split(":")
    start, end = int(start), int(end)
    scraped_text, _version_id = await download_scraped_text_from_s3_by_mfg_etld1(
        etld1=manufacturer.etld1,
        version_id=manufacturer.scraped_text_file_version_id,
    )
    binary_ground_truth = BinaryGroundTruth(
        mfg_etld1=manufacturer.etld1,
        classification_type=classification_type,
        scraped_text_file_version_id=manufacturer.scraped_text_file_version_id,
        chunk_bounds=first_chunk_bounds,
        chunk_text=scraped_text[start:end],
        metadata=binary_classification_result.metadata,
        extraction_stats=first_chunk_stats,
        corrections=[],  # empty logs initially
    )

    response = binary_ground_truth.model_dump()

    response["new_human_decision"] = HumanBinaryDecision(
        author_email=author_email,
        answer=first_chunk_stats.result.answer,
        reason=first_chunk_stats.result.reason,
        source=GroundTruthSource.USER_FORM,
    )
    response.pop("id", None)  # remove id from response

    return response


def get_human_decision_help_info():
    return (
        f"Also, ensure human_decision.answer is a boolean value (`true` or `false`) and "
        f"human_decision.reason is a string explaining the decision.",
        f"For ex: `answer: true` and `reason: 'This is a valid manufacturer because..'`. "
        f"Providing a reason is optional if your answer is the same as llm's decision.",
    )


async def parse_binary_ground_truth_with_new_decision(
    request: Request,
) -> tuple[BinaryGroundTruth, HumanBinaryDecision]:
    """Parse request body and handle new_human_decision field"""
    body = await request.body()
    data = json.loads(body)

    # Extract new_human_decision
    new_decision_data = data.pop("new_human_decision", None)
    if not new_decision_data:
        raise ValueError(
            "new_human_decision must be provided in the request body. "
            f"{get_human_decision_help_info()}"
        )

    # Validate new_human_decision
    new_decision = HumanBinaryDecision(**new_decision_data)

    # Create BinaryGroundTruth instance (validates all other fields)
    binary_gt = BinaryGroundTruth(**data)

    return binary_gt, new_decision


@router.post(
    "/ground_truth/binary-classification/correction", response_class=JSONResponse
)
async def collect_binary_ground_truth(
    parsed_data: tuple[BinaryGroundTruth, HumanBinaryDecision] = Depends(
        parse_binary_ground_truth_with_new_decision
    ),
):
    """
    Endpoint to collect the ground truth results for a given binary ground truth.
    """
    binary_gt, new_decision = parsed_data
    current_time = get_current_time()

    user = await User.find_one(User.email == new_decision.author_email)
    if not user:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sorry, no registered user found with email: `{new_decision.author_email}`. "
                f"Please register on `sudokn.com` to submit corrections."
            ),
        )

    manufacturer = await find_manufacturer_by_etld1(binary_gt.mfg_etld1)
    if not manufacturer:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Manufacturer with URL '{binary_gt.mfg_etld1}' does not exist. Cannot submit correction for non-existent manufacturer."
            ),
        )

    existing_binary_gt = await get_binary_ground_truth(
        linked_manufacturer=manufacturer,
        prompt_version_id=binary_gt.llm_decision.chunk_stats.prompt_version_id,
        classification_type=binary_gt.classification_type,
    )
    if existing_binary_gt:
        if not existing_binary_gt.corrections:
            # because a binary gt is never saved without at least one human decision, this is an unexpected state
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Existing binary ground truth for mfg_url: {binary_gt.mfg_etld1}, "
                    f"classification_type: {binary_gt.classification_type} does not have any previous human decisions. "
                    f"Please contact the administrator."
                ),
            )

        # in case two people fetched the same binary ground truth, one submitted first
        if existing_binary_gt.corrections != binary_gt.corrections:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Human decision logs do not match the existing ground truth. Someone might have submitted a decision before you. "
                    f"Please fetch and use the latest ground truth template.",
                ),
            )

        binary_gt = await add_decision_to_binary_ground_truth(
            manufacturer, existing_binary_gt, new_decision, current_time
        )
    else:
        binary_gt = await save_new_binary_ground_truth(
            manufacturer, binary_gt, new_decision, current_time
        )

    response = binary_gt.model_dump()

    response.pop("id", None)
    return response
