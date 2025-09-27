import json
import random
import logging
from fastapi import APIRouter, HTTPException, Request, Query, Depends
from fastapi.responses import JSONResponse

from core.models.db.user import User
from core.models.db.manufacturer import Batch
from core.models.to_scrape_item import ToScrapeItem

from core.services.manufacturer_service import (
    find_manufacturer_by_etld1,
    find_manufacturer_by_url,
    find_random_manufacturer_url,
)
from core.services.user_service import find_by_email

from core.utils.url_util import (
    get_normalized_url,
    get_complete_url_with_compatible_protocol,
)
from core.utils.time_util import get_current_time
from core.utils.aws.queue.priority_scrape_queue_util import (
    push_item_to_priority_scrape_queue,
)

from data_etl_app.models.db.binary_ground_truth import (
    BinaryGroundTruth,
    HumanBinaryDecision,
)
from data_etl_app.services.ground_truth.binary_ground_truth_service import (
    get_binary_ground_truth,
    save_new_binary_ground_truth,
    add_decision_to_binary_ground_truth,
)
from data_etl_app.models.types_and_enums import (
    BinaryClassificationTypeEnum,
    GroundTruthSource,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get(
    "/ground_truth/binary-classification/user-form-template",
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

    llm_decision = getattr(manufacturer, classification_type.value, None)
    if llm_decision is None:
        raise HTTPException(
            status_code=404,
            detail=f"No LLM decision found for classification type: {classification_type.value} for manufacturer with etld1: {mfg_etld1}.",
        )

    existing_binary_gt = await get_binary_ground_truth(
        manufacturer=manufacturer,
        prompt_version_id=llm_decision.stats.prompt_version_id,
        classification_type=classification_type,
    )
    if existing_binary_gt:
        response = existing_binary_gt.model_dump()
        response["new_human_decision"] = HumanBinaryDecision(
            author_email=author_email,
            answer=None,  # initially None, user will fill this
            reason=None,  # initially None, user will fill this
            source=GroundTruthSource.USER_FORM,
        )
        logger.debug(f"Existing binary ground truth found: {response}")
        response.pop("id", None)  # remove id from response
        return response

    logger.debug(
        f"No existing binary ground truth found for mfg_etld1: {mfg_etld1}, classification_type: {classification_type}. Creating new template."
    )
    # if not, then we need to create a new binary ground truth
    binary_ground_truth = BinaryGroundTruth(
        mfg_etld1=manufacturer.etld1,
        scraped_text_file_version_id=manufacturer.scraped_text_file_version_id,
        classification_type=classification_type,
        llm_decision=llm_decision,
        human_decision_logs=[],  # empty logs initially
    )

    response = binary_ground_truth.model_dump()

    response["new_human_decision"] = HumanBinaryDecision(
        author_email=author_email,
        answer=None,  # initially None, user will fill this
        reason=None,  # initially None, user will fill this
        source=GroundTruthSource.USER_FORM,
    )
    response.pop("id", None)  # remove id from response

    return response


@router.get("/ground_truth/binary-classification/template", response_class=JSONResponse)
async def fetch_binary_classification_template(
    author_email: str = Query(
        description=(
            f"Author email query param is required. This is used to generate customized template for you. "
            f"To add your email as query param, simply append the URL with `?author_email=your_email@example.com`. "
            f"If you are using postman, you can use the `Params` tab to add a query param."
        ),
    ),
    mfg_url: str | None = Query(
        default=None, description="Manufacturer URL (optional, randomized otherwise)"
    ),
    classification_type: BinaryClassificationTypeEnum = Query(
        default=random.choice(list(BinaryClassificationTypeEnum)),
        description=f"Any one of {[concept.value for concept in BinaryClassificationTypeEnum]}",
    ),
):
    current_timestamp = get_current_time()

    user = await find_by_email(author_email)
    if not user:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sorry, no registered user found with email: {author_email}. "
                f"Please register on `sudokn.com` to fetch binary classification ground truth template."
            ),
        )

    if not mfg_url:  # then find a random manufacturer and set mfg_url
        mfg_url = await find_random_manufacturer_url()

        if not mfg_url:  # then raise HTTPException
            raise HTTPException(
                status_code=404,
                detail="Something went wrong finding a random mfg_url. Please provide a valid mfg_url instead.",
            )
    else:
        try:
            _, mfg_url = get_normalized_url(
                get_complete_url_with_compatible_protocol(mfg_url)
            )
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid URL: '{mfg_url}' has no valid hostname. Error: {str(e)}",
            )

    manufacturer = await find_manufacturer_by_url(mfg_url)
    if not manufacturer or not manufacturer.is_manufacturer:
        # this will only be the case with user provided mfg_url
        # or when for some reason the manufacturer was not extracted correctly
        # push this new potential manufacturer to scrape queue and ask user to try again in a few minutes
        await push_item_to_priority_scrape_queue(
            ToScrapeItem(
                accessible_normalized_url=mfg_url,
                batch=Batch(
                    title="Ground Truth API: Binary Classification",
                    timestamp=current_timestamp,  # ISO format for timestamp
                ),
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=f"Manufacturer not found for mfg_url:`{mfg_url}`. Pushed to scrape queue. Please try again in a few minutes.",
        )

    if (
        manufacturer.is_manufacturer.answer is False
        and classification_type != BinaryClassificationTypeEnum.is_manufacturer
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                f"The provided URL:`{mfg_url}` does not belong to a valid manufacturer, "
                f"Following is the reason. {manufacturer.is_manufacturer.reason} "
                f"Please specify `is_manufacturer` classification type to get the "
                f"binary ground truth template for this manufacturer."
            ),
        )

    llm_decision = getattr(manufacturer, classification_type.value, None)
    if llm_decision is None:
        # if the LLM decision is not available, we need to push this manufacturer to the scrape queue
        await push_item_to_priority_scrape_queue(
            ToScrapeItem(
                accessible_normalized_url=mfg_url,
                batch=Batch(
                    title=f"Ground Truth API: LLM Decision for `{classification_type.value}` missing",
                    timestamp=current_timestamp,  # ISO format for timestamp
                ),
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=f"No LLM decision found for classification type: {classification_type.value} for manufacturer: {mfg_url}. Pushed to scrape queue for re-extraction. Please try again in a few minutes.",
        )

    # check if binary ground truth already exists for this mfg_url and classification_type
    existing_binary_gt = await get_binary_ground_truth(
        manufacturer=manufacturer,
        prompt_version_id=llm_decision.stats.prompt_version_id,
        classification_type=classification_type,
    )
    if existing_binary_gt:
        response = existing_binary_gt.model_dump()
        response["new_human_decision"] = HumanBinaryDecision(
            author_email=author_email,
            answer=None,  # initially None, user will fill this
            reason=None,  # initially None, user will fill this
            source=GroundTruthSource.API_SURVEY,
        )
        response.pop("id", None)  # remove id from response
        return response

    # if not, then we need to create a new binary ground truth
    binary_ground_truth = BinaryGroundTruth(
        mfg_etld1=manufacturer.etld1,
        scraped_text_file_version_id=manufacturer.scraped_text_file_version_id,
        classification_type=classification_type,
        llm_decision=llm_decision,
        human_decision_logs=[],  # empty logs initially
    )

    response = binary_ground_truth.model_dump()

    response["new_human_decision"] = HumanBinaryDecision(
        author_email=author_email,
        answer=None,  # initially None, user will fill this
        reason=None,  # initially None, user will fill this
        source=GroundTruthSource.API_SURVEY,
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

    user = await User.find_one({"email": new_decision.author_email})
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
        manufacturer=manufacturer,
        prompt_version_id=binary_gt.llm_decision.stats.prompt_version_id,
        classification_type=binary_gt.classification_type,
    )
    if existing_binary_gt:
        if not existing_binary_gt.human_decision_logs:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Existing binary ground truth for mfg_url: {binary_gt.mfg_etld1}, "
                    f"classification_type: {binary_gt.classification_type} does not have any previous human decisions. "
                    f"Please contact the administrator."
                ),
            )

        # in case two people fetched the same binary ground truth, one submitted first
        if new_decision == existing_binary_gt.human_decision_logs[-1].human_decision:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Human decision logs do not match the existing ground truth. Someone might have submitted a decision before you. "
                    f"Please fetch and use the latest ground truth template.",
                ),
            )

        binary_gt = await add_decision_to_binary_ground_truth(
            existing_binary_gt, new_decision, current_time
        )
    else:
        binary_gt = await save_new_binary_ground_truth(
            binary_gt, new_decision, current_time
        )

    response = binary_gt.model_dump()

    response.pop("id", None)
    return response
