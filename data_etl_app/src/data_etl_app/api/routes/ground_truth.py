import json
import random
from fastapi import APIRouter, HTTPException, Request, Query, Depends
from fastapi.responses import JSONResponse

from shared.models.db.user import User
from shared.models.db.manufacturer import Batch
from shared.models.extraction_results import ExtractionResults
from shared.models.to_scrape_item import ToScrapeItem

from shared.services.manufacturer_service import (
    find_manufacturer_by_etld1,
    find_manufacturer_by_url,
    find_random_manufacturer_url,
)
from shared.services.user_service import findByEmail

from shared.utils.url_util import (
    get_normalized_url,
    get_complete_url_with_compatible_protocol,
)
from shared.utils.time_util import get_current_time
from shared.utils.aws.queue.priority_scrape_queue_util import (
    push_item_to_priority_scrape_queue,
)

from shared.utils.aws.s3.scraped_text_util import (
    get_file_name_from_mfg_etld,
    download_scraped_text_from_s3_by_filename,
)

from data_etl_app.models.keyword_ground_truth import (
    KeywordGroundTruth,
    HumanCorrection,
)
from data_etl_app.services.keyword_ground_truth_service import (
    get_keyword_ground_truth,
    save_new_keyword_ground_truth,
    update_existing_with_new_keyword_ground_truth,
)
from data_etl_app.models.binary_ground_truth import (
    BinaryGroundTruth,
    HumanBinaryDecision,
)
from data_etl_app.services.binary_ground_truth_service import (
    get_binary_ground_truth,
    save_new_binary_ground_truth,
    update_existing_with_new_binary_ground_truth,
)
from data_etl_app.models.types import BinaryClassificationTypeEnum, ConceptTypeEnum
from data_etl_app.dependencies.aws_deps import get_sqs_scraper_client, get_s3_client

router = APIRouter()


@router.get("/template/keyword-extraction-result", response_class=JSONResponse)
async def fetch_ground_truth_template(
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
    concept_type: ConceptTypeEnum = Query(
        default=random.choice(list(ConceptTypeEnum)),
        description=f"Any one of {[concept.value for concept in ConceptTypeEnum]}",
    ),
    chunk_no: int | None = Query(
        default=None, ge=1, description="Chunk number starting from 1."
    ),
    sqs_client=Depends(get_sqs_scraper_client),
    s3_client=Depends(get_s3_client),
):
    current_timestamp = get_current_time()

    user = await findByEmail(author_email)
    if not user:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sorry, no registered user found with email: {author_email}. "
                f"Please register on `sudokn.com` to fetch keyword extraction ground truth template."
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

    # fetch the manufacturer from the database
    manufacturer = await find_manufacturer_by_url(mfg_url)

    if not manufacturer or not manufacturer.is_manufacturer:
        # this will only be the case with user provided mfg_url
        # or when for some reason the manufacturer was not extracted correctly
        # push this new potential manufacturer to scrape queue and ask user to try again in a few minutes
        await push_item_to_priority_scrape_queue(
            sqs_client,
            ToScrapeItem(
                accessible_normalized_url=mfg_url,
                batch=Batch(
                    title="Ground Truth API: Keyword Extraction Result",
                    timestamp=current_timestamp,  # ISO format for timestamp
                ),
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=f"Manufacturer not found for mfg_url:`{mfg_url}`. Pushed to scrape queue. Please try again in a few minutes.",
        )
    elif manufacturer and manufacturer.is_manufacturer.answer is False:
        raise HTTPException(
            status_code=400,
            detail=f"The provided URL:`{mfg_url}` does not belong to a valid manufacturer, because {manufacturer.is_manufacturer.reason}.",
        )

    extracted_concept_data: ExtractionResults | None = getattr(
        manufacturer, concept_type, None
    )
    if not extracted_concept_data:
        await push_item_to_priority_scrape_queue(
            sqs_client,
            ToScrapeItem(
                accessible_normalized_url=mfg_url,
                batch=Batch(
                    title=f"Ground Truth API: Concept Data for `{concept_type.value}` missing",
                    timestamp=current_timestamp,  # ISO format for timestamp
                ),
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=f"No data found for concept type: {concept_type.value} for manufacturer: {mfg_url}. Pushed to scrape queue for re-extraction. Please try again in a few minutes.",
        )

    # sort concept_data.stats.search by chunk_bounds
    sorted_search_data = [
        (key, value)
        for key, value in sorted(
            extracted_concept_data.stats.search.items(), key=lambda item: item[0]
        )
    ]
    last_chunk_no = len(sorted_search_data)

    if chunk_no:
        if chunk_no > last_chunk_no:
            raise HTTPException(
                status_code=404,
                detail=f"Requested chunk number:{chunk_no} exceeds last available chunk number:{last_chunk_no} for concept type:{concept_type.value}.",
            )
    else:
        # pick random chunk_no if not provided
        chunk_no = random.randint(1, last_chunk_no)

    # check if keyword ground truth already exists for this mfg_url, concept_type, and chunk_no
    existing_keyword_gt = await get_keyword_ground_truth(
        manufacturer=manufacturer,
        concept_type=concept_type,
        chunk_no=chunk_no,
    )
    if existing_keyword_gt:
        response = existing_keyword_gt.model_dump()

        response["new_human_correction"] = HumanCorrection(
            author_email=author_email,
            add={},  # initially None, user will fill this
            remove=[],  # initially None, user will fill this
        )
        response.pop("id", None)  # remove id from response
        return response

    # At this point, chunk_no was either picked randomly or provided by user, but no existing keyword ground truth was found
    chunk_bounds, chunk_search_stats = sorted_search_data[chunk_no - 1]

    # TODO: maybe cache downloaded text
    scraped_text, version_id = await download_scraped_text_from_s3_by_filename(
        s3_client, file_name=get_file_name_from_mfg_etld(manufacturer.etld1)
    )

    if manufacturer.scraped_text_file_version_id != version_id:
        # maybe a new extraction is underway and only scraping has been completed so far
        raise HTTPException(
            status_code=400,
            detail=f"Scraped text version ID mismatch for mfg_url: {mfg_url}. Expected: {manufacturer.scraped_text_file_version_id}, got: {version_id}.",
        )

    start, end = int(chunk_bounds.split(":")[0]), int(chunk_bounds.split(":")[1])
    if start < 0 or end > len(scraped_text):
        # beg and pray this never happens
        raise HTTPException(
            status_code=400,
            detail=f"Chunk bounds {chunk_bounds} are out of range for the scraped text.",
        )

    keyword_ground_truth = KeywordGroundTruth(
        scraped_text_file_version_id=version_id,
        ontology_version_id=extracted_concept_data.stats.ontology_version_id,
        mfg_etld1=manufacturer.etld1,
        concept_type=concept_type,
        chunk_bounds=chunk_bounds,
        chunk_text=scraped_text[start:end],
        chunk_no=chunk_no,
        last_chunk_no=last_chunk_no,
        chunk_extracted_at=extracted_concept_data.extracted_at,
        chunk_search_stats=chunk_search_stats,
        human_correction_logs=[],  # empty logs initially
    )

    response = keyword_ground_truth.model_dump()

    response["new_human_correction"] = HumanCorrection(
        author_email=author_email,
        add={},  # initially None, user will fill this
        remove=[],  # initially None, user will fill this
    )

    response.pop("id", None)  # remove id from response
    return response


def get_human_correction_help_info() -> str:
    return (
        f"Also, please ensure `new_human_correction.add` is a map FROM:in-vocab known concepts TO:terms present in chunk text, "
        f"and `new_human_correction.remove` is a list of results to remove. "
        f"For ex: `add: {{'Healthcare': ['Medical', 'Hospital Industry']}}` and `remove: ['Defense', 'Military']`. "
        f"If you wish to add or remove nothing, set `add: {{}}` and `remove: []`"
    )


async def parse_keyword_ground_truth_with_new_correction(
    request: Request,
) -> tuple[KeywordGroundTruth, HumanCorrection]:
    """Parse request body and handle new_human_correction field"""
    body = await request.body()
    data = json.loads(body)

    # Extract new_human_correction
    new_correction_data = data.pop("new_human_correction", None)
    if not new_correction_data:
        raise ValueError(
            "new_human_correction must be provided in the request body. "
            f"{get_human_correction_help_info()}"
        )

    # Validate new_human_correction
    new_correction = HumanCorrection(**new_correction_data)

    # Create KeywordGroundTruth instance (validates all other fields)
    keyword_gt = KeywordGroundTruth(**data)

    return keyword_gt, new_correction


@router.post("/truth/keyword-extraction-result", response_class=JSONResponse)
async def collect_keyword_extraction_ground_truth(
    parsed_data: tuple[KeywordGroundTruth, HumanCorrection] = Depends(
        parse_keyword_ground_truth_with_new_correction
    ),
    s3_client=Depends(get_s3_client),
):
    """
    Endpoint to collect the ground truth results for a given keyword ground truth.
    """
    keyword_gt, new_correction = parsed_data
    current_time = get_current_time()

    user = await findByEmail(new_correction.author_email)
    if not user:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sorry, no registered user found with email: {new_correction.author_email}. "
                f"Please register on `sudokn.com` to submit corrections."
            ),
        )

    manufacturer = await find_manufacturer_by_etld1(keyword_gt.mfg_etld1)
    if not manufacturer:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Manufacturer not found for etld1: {keyword_gt.mfg_etld1}. "
                f"Cannot submit correction for non-existent manufacturer."
            ),
        )

    # decide if this is a new insert or update
    existing_keyword_gt = await get_keyword_ground_truth(
        manufacturer=manufacturer,
        concept_type=keyword_gt.concept_type,
        chunk_no=keyword_gt.chunk_no,
    )

    if existing_keyword_gt:
        if not existing_keyword_gt.human_correction_logs:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Existing keyword ground truth for mfg_url: {keyword_gt.mfg_etld1}, "
                    f"concept_type: {keyword_gt.concept_type}, chunk_no: {keyword_gt.chunk_no} does not have any previous human corrections. "
                    f"Please contact the administrator."
                ),
            )

        if (
            new_correction
            == existing_keyword_gt.human_correction_logs[-1].result_correction
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Keyword ground truth for mfg_url: {keyword_gt.mfg_etld1}, "
                    f"concept_type: {keyword_gt.concept_type}, "
                    f"chunk_no: {keyword_gt.chunk_no} already contains the same result correction. "
                    f"Please check the latest `human_correction_logs`. {get_human_correction_help_info()}"
                ),
            )

        # in case two people fetched the same keyword ground truth, one submitted first
        if (
            existing_keyword_gt.human_correction_logs
            != keyword_gt.human_correction_logs
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Result correction logs do not match the existing ground truth."
                    f"Please fetch the latest ground truth before making corrections.",
                ),
            )

        keyword_gt = await update_existing_with_new_keyword_ground_truth(
            timestamp=current_time,
            manufacturer=manufacturer,
            existing_keyword_gt=existing_keyword_gt,
            new_correction=new_correction,
            s3_client=s3_client,
        )
    else:
        # this is a new keyword ground truth, so we need to set the created_at and updated_at fields
        keyword_gt = await save_new_keyword_ground_truth(
            timestamp=current_time,
            manufacturer=manufacturer,
            new_keyword_gt=keyword_gt,
            new_correction=new_correction,
            s3_client=s3_client,
        )

    response = keyword_gt.model_dump()

    response.pop("id", None)  # remove id from response
    return response


@router.get("/template/binary-classification-result", response_class=JSONResponse)
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
    sqs_client=Depends(get_sqs_scraper_client),
):
    current_timestamp = get_current_time()

    user = await findByEmail(author_email)
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
            sqs_client,
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
                f"because {manufacturer.is_manufacturer.reason}. "
                f"Please specify `is_manufacturer` classification type to get the "
                f"binary ground truth template for this manufacturer."
            ),
        )

    # check if binary ground truth already exists for this mfg_url and classification_type
    existing_binary_gt = await get_binary_ground_truth(
        manufacturer=manufacturer,
        classification_type=classification_type,
    )
    if existing_binary_gt:
        response = existing_binary_gt.model_dump()
        response["new_human_decision"] = HumanBinaryDecision(
            author_email=author_email,
            answer=None,  # initially None, user will fill this
            reason=None,  # initially None, user will fill this
        )
        response.pop("id", None)  # remove id from response
        return response

    llm_decision = getattr(manufacturer, classification_type.value, None)
    if llm_decision is None:
        # if the LLM decision is not available, we need to push this manufacturer to the scrape queue
        await push_item_to_priority_scrape_queue(
            sqs_client,
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


@router.post("/truth/binary-classification-result", response_class=JSONResponse)
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

        binary_gt = await update_existing_with_new_binary_ground_truth(
            existing_binary_gt, new_decision, current_time
        )
    else:
        binary_gt = await save_new_binary_ground_truth(
            binary_gt, new_decision, current_time
        )

    response = binary_gt.model_dump()

    response.pop("id", None)
    return response
