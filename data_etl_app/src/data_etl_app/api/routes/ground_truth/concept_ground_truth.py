import json
import random
from fastapi import APIRouter, HTTPException, Request, Query, Depends
from fastapi.responses import JSONResponse

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

from core.utils.aws.s3.scraped_text_util import (
    download_scraped_text_from_s3_by_mfg_etld1,
)

from core.models.concept_extraction_results import ConceptExtractionResults
from core.models.db.concept_ground_truth import (
    ConceptGroundTruth,
    ConceptResultCorrection,
)
from data_etl_app.services.ground_truth.concept_ground_truth_service import (
    get_extracted_concept_ground_truth,
    save_new_concept_ground_truth,
    add_correction_to_concept_ground_truth,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum, GroundTruthSource

router = APIRouter()


@router.get("/ground_truth/extracted-concepts/template", response_class=JSONResponse)
async def fetch_concept_ground_truth_template(
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
):
    current_timestamp = get_current_time()

    user = await find_by_email(author_email)
    if not user:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sorry, no registered user found with email: {author_email}. "
                f"Please register on `sudokn.com` to fetch concept extraction ground truth template."
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
            ToScrapeItem(
                accessible_normalized_url=mfg_url,
                batch=Batch(
                    title="Ground Truth API: concept Extraction Result",
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
            detail=(
                f"The provided URL:`{mfg_url}` does not belong to a valid manufacturer. "
                f"Following is the reason. {manufacturer.is_manufacturer.reason}` If you think otherwise, "
                f"please submit a ground truth for `is_manufacturer` on binary classification endpoint."
            ),
        )

    concept_extraction_results: ConceptExtractionResults | None = getattr(
        manufacturer, concept_type, None
    )
    if not concept_extraction_results:
        await push_item_to_priority_scrape_queue(
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
            concept_extraction_results.stats.chunked_stats.items(),
            key=lambda item: item[0],
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

    # check if concept ground truth already exists for this mfg_url, concept_type, and chunk_no
    existing_concept_gt = await get_extracted_concept_ground_truth(
        linked_manufacturer=manufacturer,
        concept_type=concept_type,
        chunk_no=chunk_no,
    )
    if existing_concept_gt:  # then logs must be non-empty
        last_correction_log = existing_concept_gt.correction_logs[-1]
        response = existing_concept_gt.model_dump()
        response["your_correction"] = ConceptResultCorrection(
            author_email=author_email,
            add=last_correction_log.result_correction.add,  # pre-fill with last correction
            remove=last_correction_log.result_correction.remove,  # pre-fill with last correction
            source=GroundTruthSource.API_SURVEY,
        )
        response.pop("id", None)  # remove id from response
        return response

    # At this point, chunk_no was either picked randomly or provided by user, but no existing concept ground truth was found
    chunk_bounds, chunk_search_stats = sorted_search_data[chunk_no - 1]

    # TODO: maybe cache downloaded text
    scraped_text, _version_id = await download_scraped_text_from_s3_by_mfg_etld1(
        etld1=manufacturer.etld1,
        version_id=manufacturer.scraped_text_file_version_id,
    )

    start, end = int(chunk_bounds.split(":")[0]), int(chunk_bounds.split(":")[1])
    if start < 0 or end > len(scraped_text):
        # beg and pray this never happens
        raise HTTPException(
            status_code=400,
            detail=f"Chunk bounds {chunk_bounds} are out of range for the scraped text.",
        )

    concept_ground_truth = ConceptGroundTruth(
        mfg_etld1=manufacturer.etld1,
        concept_type=concept_type,
        scraped_text_file_version_id=manufacturer.scraped_text_file_version_id,
        map_prompt_version_id=concept_extraction_results.stats.map_prompt_version_id,
        extract_prompt_version_id=concept_extraction_results.stats.extract_prompt_version_id,
        ontology_version_id=concept_extraction_results.stats.ontology_version_id,
        chunk_bounds=chunk_bounds,
        last_chunk_no=last_chunk_no,
        chunk_no=chunk_no,
        chunk_extracted_at=concept_extraction_results.extracted_at,
        chunk_text=scraped_text[start:end],
        chunk_search_stats=chunk_search_stats,
        correction_logs=[],  # empty logs initially
    )

    response = concept_ground_truth.model_dump()

    response["your_correction"] = ConceptResultCorrection(
        author_email=author_email,
        add={},  # initially None, user will fill this
        remove=[],  # initially None, user will fill this
        source=GroundTruthSource.API_SURVEY,
    )

    response.pop("id", None)  # remove id from response
    return response


def get_human_correction_help_info() -> str:
    return (
        f"Also, please ensure `your_correction.add` is a map FROM:in-vocab known concepts TO:terms present in chunk text, "
        f"and `your_correction.remove` is a list of results to remove. "
        f"For ex: `add: {{'Healthcare': ['Medical', 'Hospital Industry']}}` and `remove: ['Defense', 'Military']`. "
        f"If you wish to add or remove nothing, set `add: {{}}` and `remove: []`"
    )


async def parse_concept_ground_truth_with_new_correction(
    request: Request,
) -> tuple[ConceptGroundTruth, ConceptResultCorrection]:
    """Parse request body and handle your_correction field"""
    body = await request.body()
    data = json.loads(body)

    # Extract your_correction
    new_correction_data = data.pop("your_correction", None)
    if not new_correction_data:
        raise ValueError(
            "your_correction must be provided in the request body. "
            f"{get_human_correction_help_info()}"
        )

    # Validate your_correction
    new_correction = ConceptResultCorrection(**new_correction_data)

    # Create ChunkconceptGroundTruth instance (validates all other fields)
    concept_gt = ConceptGroundTruth(**data)

    return concept_gt, new_correction


@router.post(
    "/ground_truth/extracted-concepts/correction",
    response_class=JSONResponse,
)
async def collect_concept_extraction_ground_truth(
    parsed_data: tuple[ConceptGroundTruth, ConceptResultCorrection] = Depends(
        parse_concept_ground_truth_with_new_correction
    ),
):
    """
    Endpoint to collect the ground truth results for a given concept ground truth.
    """
    concept_gt, new_correction = parsed_data
    current_time = get_current_time()

    user = await find_by_email(new_correction.author_email)
    if not user:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sorry, no registered user found with email: {new_correction.author_email}. "
                f"Please register on `sudokn.com` to submit corrections."
            ),
        )

    manufacturer = await find_manufacturer_by_etld1(concept_gt.mfg_etld1)
    if not manufacturer:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Manufacturer not found for etld1: {concept_gt.mfg_etld1}. "
                f"Cannot submit correction for non-existent manufacturer."
            ),
        )

    # decide if this is a new insert or update
    existing_concept_gt = await get_extracted_concept_ground_truth(
        linked_manufacturer=manufacturer,
        concept_type=concept_gt.concept_type,
        chunk_no=concept_gt.chunk_no,
    )

    if existing_concept_gt:
        if not existing_concept_gt.correction_logs:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Existing concept ground truth for mfg_url: {concept_gt.mfg_etld1}, "
                    f"concept_type: {concept_gt.concept_type}, chunk_no: {concept_gt.chunk_no} does not have any previous human corrections. "
                    f"Please contact the administrator."
                ),
            )

        # in case two people fetched the same concept ground truth, one submitted first
        if existing_concept_gt.correction_logs != concept_gt.correction_logs:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Result correction logs do not match the existing ground truth."
                    f"Please fetch the latest ground truth before making corrections.",
                ),
            )

        concept_gt = await add_correction_to_concept_ground_truth(
            timestamp=current_time,
            linked_manufacturer=manufacturer,
            existing_concept_gt=existing_concept_gt,
            new_correction=new_correction,
        )
    else:
        # this is a new concept ground truth, so we need to set the created_at and updated_at fields
        concept_gt = await save_new_concept_ground_truth(
            timestamp=current_time,
            manufacturer=manufacturer,
            new_concept_gt=concept_gt,
            new_correction=new_correction,
        )

    response = concept_gt.model_dump()

    response.pop("id", None)  # remove id from response
    return response
