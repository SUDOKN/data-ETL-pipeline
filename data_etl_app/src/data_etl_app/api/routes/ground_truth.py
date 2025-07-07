import random

from aiobotocore.session import get_session
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from shared.models.db.manufacturer import Batch, Manufacturer
from shared.models.db.extraction_results import ExtractionResults
from shared.models.to_scrape_item import ToScrapeItem

from shared.utils.url_util import canonical_host
from shared.utils.time_util import get_current_time
from shared.utils.aws.queue.sqs_scraper_client_util import make_sqs_scraper_client
from shared.utils.aws.queue.scrape_queue_util import push_item_to_scrape_queue
from shared.utils.aws.s3.s3_client_util import make_s3_client
from shared.utils.aws.s3.scraped_text_util import (
    get_file_name_from_mfg_url,
    download_scraped_text_from_s3_by_filename,
)

from data_etl_app.models.keyword_ground_truth import KeywordGroundTruth
from data_etl_app.services.keyword_ground_truth_service import (
    get_keyword_ground_truth,
)
from data_etl_app.models.types import ConceptTypeEnum

router = APIRouter()

# Initialize only once, TODO: move to deps
session = get_session()
sqs_scraper_client = make_sqs_scraper_client(session)
s3_client = make_s3_client(session)


@router.get("/keyword_gt/", response_class=JSONResponse)
async def fetch_ground_truth_template(
    mfg_url: str | None = Query(
        default=None, description="Manufacturer URL (optional, randomized otherwise)"
    ),
    concept_type: ConceptTypeEnum = Query(
        default=ConceptTypeEnum.industries,
        description=f"Any one of {[concept.value for concept in ConceptTypeEnum]}",
    ),
    chunk_no: int = Query(default=1, ge=1, description="Chunk number starting from 1."),
):
    current_timestamp = get_current_time()
    if not mfg_url:  # then find a random manufacturer and set mfg_url
        agg_cursor = await Manufacturer.aggregate(
            [
                {"$match": {"is_manufacturer.answer": True}},
                {"$sample": {"size": 1}},
                {"$project": {"url": 1}},
            ]
        ).to_list(length=1)
        mfg_url = agg_cursor[0]["url"] if agg_cursor else None

        if not mfg_url:  # then raise HTTPException
            raise HTTPException(
                status_code=404,
                detail="Something went wrong finding a random mfg_url. Please provide a valid mfg_url instead.",
            )

    mfg_url = canonical_host(mfg_url)  # VERY IMPORTANT
    if not mfg_url:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid URL: '{mfg_url}' has no valid hostname.",
        )

    # now that mfg_url is set and there is no existing_keyword_gt, fetch the manufacturer from the database
    manufacturer: Manufacturer | None = await Manufacturer.find_one({"url": mfg_url})

    if not manufacturer or not manufacturer.is_manufacturer:
        # this will only be the case with user provided mfg_url
        # or when for some reason the manufacturer was not extracted correctly
        # push this new potential manufacturer to scrape queue and ask user to try again in a few minutes
        await push_item_to_scrape_queue(
            sqs_scraper_client,
            ToScrapeItem(
                manufacturer_url=mfg_url,
                batch=Batch(
                    title=f"Ground Truth API",
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
            detail=f"The provided URL:`{mfg_url}` does not belong to a manufacturer, because {manufacturer.is_manufacturer.reason}.",
        )

    extracted_concept_data: ExtractionResults | None = None
    if manufacturer:
        match concept_type:
            case ConceptTypeEnum.industries:
                extracted_concept_data = manufacturer.industries
            case ConceptTypeEnum.certificates:
                extracted_concept_data = manufacturer.certificates
            case ConceptTypeEnum.material_caps:
                extracted_concept_data = manufacturer.material_caps
            case ConceptTypeEnum.process_caps:
                extracted_concept_data = manufacturer.process_caps
            case _:
                # randomly select one of the concept types
                concept_type = random.choice(list(ConceptTypeEnum))
                extracted_concept_data = getattr(manufacturer, concept_type.value, None)

    if not extracted_concept_data:
        await push_item_to_scrape_queue(
            sqs_scraper_client,
            ToScrapeItem(
                manufacturer_url=mfg_url,
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

    # check if keyword ground truth already exists for this mfg_url, concept_type, and chunk_no
    existing_keyword_gt = await get_keyword_ground_truth(
        mfg_url=mfg_url,
        concept_type=concept_type.value,
        chunk_no=chunk_no,
    )
    if existing_keyword_gt:
        # return existing keyword ground truth
        return JSONResponse(
            status_code=200,
            content=existing_keyword_gt.model_dump_json(),
        )

    # if not, then we need to create a new keyword ground truth
    # sort concept_data.stats.search by chunk_bounds
    sorted_search_data = [
        (key, value)
        for key, value in sorted(
            extracted_concept_data.stats.search.items(), key=lambda item: item[0]
        )
    ]
    last_chunk_no = len(sorted_search_data)
    if chunk_no > last_chunk_no:
        raise HTTPException(
            status_code=404,
            detail=f"Chunk number {chunk_no} exceeds last available chunk {last_chunk_no} for concept type: {concept_type.value}.",
        )
    chunk_bounds, chunk_search_stats = sorted_search_data[chunk_no - 1]

    # TODO: maybe cache downloaded text
    version_id, scraped_text = await download_scraped_text_from_s3_by_filename(
        s3_client, file_name=get_file_name_from_mfg_url(mfg_url)
    )
    if manufacturer.scraped_text_file_version_id != version_id:
        # pray this never happens
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
        mfg_url=mfg_url,
        concept_type=concept_type,
        chunk_bounds=chunk_bounds,
        chunk_text=scraped_text[start:end],
        chunk_no=chunk_no,
        last_chunk_no=last_chunk_no,
        chunk_search_stats=chunk_search_stats,
        result_correction=None,  # no corrections made yet
    )

    return keyword_ground_truth.model_dump_json()
