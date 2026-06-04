import json
import random
from typing import Literal
from core.models.queue_item import EmailUserErrand
from core.utils.aws.queue.gt_scrape_queue_util import push_item_to_gt_scrape_queue
from fastapi import APIRouter, HTTPException, Request, Query, Depends
from fastapi.responses import JSONResponse

from core.models.db.manufacturer import Batch
from core.models.to_scrape_item import ToScrapeItem
from core.models.concept_extraction_results import ConceptExtractionResults
from core.models.db.concept_ground_truth import (
    ConceptGroundTruth,
    EvidenceResultCorrection,
    HumanConceptCorrection,
    MappingResultCorrection,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum, GroundTruthSource

from core.services.manufacturer_service import (
    find_manufacturer_by_etld1,
    find_manufacturer_by_url,
)
from core.services.user_service import find_by_email
from data_etl_app.services.ground_truth.concept_ground_truth_service import (
    get_corrected_results,
    get_extracted_concept_ground_truth,
    save_new_concept_ground_truth,
    add_correction_to_concept_ground_truth,
)
from data_etl_app.services.knowledge.ontology_service import get_ontology_service


from core.utils.url_util import (
    get_normalized_url,
    get_complete_url_with_compatible_protocol,
)
from core.utils.time_util import get_current_time
from core.utils.aws.s3.scraped_text_util import (
    download_scraped_text_from_s3_by_mfg_etld1,
)

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
    mfg_url: str = Query(
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
        await push_item_to_gt_scrape_queue(
            ToScrapeItem(
                accessible_normalized_url=mfg_url,
                batch=Batch(
                    title="Ground Truth API: concept Extraction Result",
                    timestamp=current_timestamp,  # ISO format for timestamp
                ),
                email_errand=EmailUserErrand(user_email=author_email),
            ),
        )
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sorry, no manufacturer found for URL: {mfg_url}. "
                f"We have added this URL to our scrape queue. Please try again in a few minutes."
            ),
        )

    concept_extraction_results: ConceptExtractionResults | None = getattr(
        manufacturer, concept_type, None
    )
    if not concept_extraction_results:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for concept type: {concept_type.value} for manufacturer: {mfg_url}. Please add manufacturer first.",
        )

    # sort concept_data.stats.search by chunk_bounds
    sorted_search_data = [
        (key, value)
        for key, value in sorted(
            concept_extraction_results.chunk_stats.items(),
            key=lambda item: int(item[0].split(":")[0]),
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
        last_correction_log = existing_concept_gt.corrections[-1]
        response = existing_concept_gt.model_dump()
        response["your_correction"] = HumanConceptCorrection(
            author_email=author_email,
            source=GroundTruthSource.API_SURVEY,
            llm_evidence_correction=last_correction_log.human_correction.llm_evidence_correction,
            llm_mapping_correction=last_correction_log.human_correction.llm_mapping_correction,  # pre-fill with last correction
        )
        response.pop("id", None)  # remove id from response
        response["final_results"] = await get_corrected_results(existing_concept_gt)
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
        chunk_text=scraped_text[start:end],
        chunk_bounds=chunk_bounds,
        chunk_no=chunk_no,
        last_chunk_no=last_chunk_no,
        metadata=concept_extraction_results.metadata,
        extraction_stats=chunk_search_stats,
        corrections=[],  # empty logs initially
    )

    response = concept_ground_truth.model_dump()
    response["your_correction"] = (
        HumanConceptCorrection(  # begins as a pre-filled template for the user to fill in
            author_email=author_email,
            source=GroundTruthSource.API_SURVEY,
            llm_evidence_correction=EvidenceResultCorrection(
                upsert={
                    kw: reason for kw, reason in chunk_search_stats.llm_evidence.items()
                },
            ),
            llm_mapping_correction=MappingResultCorrection.from_raw_llm_mapping_result(
                original_mapping_result=chunk_search_stats.llm_mapping
            ),  # pre-fill with last correction
        )
    )
    response["final_results"] = await get_corrected_results(concept_ground_truth)

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
) -> tuple[ConceptGroundTruth, HumanConceptCorrection]:
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
    new_correction = HumanConceptCorrection(**new_correction_data)

    # Create ChunkconceptGroundTruth instance (validates all other fields)
    concept_gt = ConceptGroundTruth(**data)

    return concept_gt, new_correction


@router.post(
    "/ground_truth/extracted-concepts/correction",
    response_class=JSONResponse,
)
async def collect_concept_extraction_ground_truth(
    parsed_data: tuple[ConceptGroundTruth, HumanConceptCorrection] = Depends(
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
        if not existing_concept_gt.corrections:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Existing concept ground truth for mfg_url: {concept_gt.mfg_etld1}, "
                    f"concept_type: {concept_gt.concept_type}, chunk_no: {concept_gt.chunk_no} does not have any previous human corrections. "
                    f"Please contact the administrator."
                ),
            )

        # in case two people fetched the same concept ground truth, one submitted first
        if existing_concept_gt.corrections != concept_gt.corrections:
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
    response["final_results"] = await get_corrected_results(concept_gt)

    response.pop("id", None)  # remove id from response
    return response


@router.get("/ground_truth/concept-coverage", response_class=JSONResponse)
async def get_concept_coverage_stats(
    concept_type: ConceptTypeEnum = Query(
        description=f"Concept type to compute coverage for. One of {[c.value for c in ConceptTypeEnum]}.",
    ),
    ontology_version_id: str | None = Query(
        default=None,
        description="Ontology version ID to filter GT documents against. Defaults to the current live ontology version.",
    ),
    sort: Literal["asc", "desc"] = Query(
        default="asc",
        description="Sort order for the coverage list. 'asc' = lowest coverage first, 'desc' = highest coverage first.",
    ),
    min_count: int = Query(
        default=0,
        ge=0,
        description="Exclude concepts with a document count strictly below this value. Defaults to 0 (include all).",
    ),
    max_count: int | None = Query(
        default=None,
        ge=0,
        description="Exclude concepts with a document count strictly above this value. Defaults to None (no upper limit).",
    ),
):
    """
    Returns coverage statistics for all concepts of the given type.

    For each concept in the ontology, counts how many GT documents reference it.
    A document counts if the concept appears in either `chunk_search_stats.results`
    (raw extraction) OR `final_results` (human-corrected), i.e. the union.

    `total_documents_in_range`: count of distinct GT documents that contain at least
    one concept whose global coverage count falls within [min_count, max_count].

    Concepts with zero coverage are included unless filtered by min_count.
    """
    if max_count is not None and max_count < min_count:
        raise HTTPException(
            status_code=400,
            detail=f"max_count ({max_count}) must be >= min_count ({min_count}).",
        )

    ontology_svc = await get_ontology_service()

    _concept_type_to_map = {
        ConceptTypeEnum.process_caps: ontology_svc.process_cap_map,
        ConceptTypeEnum.material_caps: ontology_svc.material_cap_map,
        ConceptTypeEnum.industries: ontology_svc.industry_map,
        ConceptTypeEnum.certificates: ontology_svc.certificate_map,
    }

    live_version_id, concept_map = _concept_type_to_map[concept_type]

    effective_ontology_version_id: str = ontology_version_id or live_version_id

    gt_docs: list[ConceptGroundTruth] = await ConceptGroundTruth.find(
        ConceptGroundTruth.concept_type == concept_type,
        ConceptGroundTruth.metadata.ontology_version_id
        == effective_ontology_version_id,
    ).to_list()

    # Tally coverage across all docs (no range filter applied yet)
    coverage: dict[str, int] = {name: 0 for name in concept_map}
    doc_covered_sets: list[set[str]] = []

    per_doc_precision: list[float] = []
    per_doc_recall: list[float] = []
    per_doc_f1: list[float] = []

    for doc in gt_docs:
        results = set(doc.chunk_search_stats.results)
        final = set(doc.final_results or [])
        covered = results | final
        doc_covered_sets.append(covered)
        for concept_name in covered:
            if concept_name in coverage:
                coverage[concept_name] += 1

        # Accuracy metrics — only for validated (human-corrected) docs
        if doc.corrections:
            tp = len(results & final)
            fp = len(results - final)
            fn = len(final - results)
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
            f1 = (
                2 * precision * recall / (precision + recall)
                if (precision + recall) > 0
                else 0.0
            )
            per_doc_precision.append(precision)
            per_doc_recall.append(recall)
            per_doc_f1.append(f1)

    validated_count = len(per_doc_precision)
    accuracy = (
        {
            "precision": round(sum(per_doc_precision) / validated_count, 4),
            "recall": round(sum(per_doc_recall) / validated_count, 4),
            "f1": round(sum(per_doc_f1) / validated_count, 4),
        }
        if validated_count > 0
        else None
    )

    # Concepts whose global count falls within [min_count, max_count]
    in_range_concepts = {
        name
        for name, count in coverage.items()
        if count >= min_count and (max_count is None or count <= max_count)
    }

    # Distinct GT docs that touch at least one in-range concept
    total_gts_in_range = sum(
        1 for covered in doc_covered_sets if covered & in_range_concepts
    )

    sorted_coverage = sorted(
        [
            {"concept": name, "count": count}
            for name, count in coverage.items()
            if name in in_range_concepts
        ],
        key=lambda x: x["count"],
        reverse=(sort == "desc"),
    )

    return {
        "ontology_version_id": effective_ontology_version_id,
        "concept_type": concept_type.value,
        "total_ground_truths": len(gt_docs),
        "total_ground_truths_in_range": total_gts_in_range,
        "validated_ground_truths": validated_count,
        "total_concepts": len(concept_map),
        "total_concepts_in_range": len(in_range_concepts),
        "accuracy": accuracy,
        "coverage": sorted_coverage,
    }
