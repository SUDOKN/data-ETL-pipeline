import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from core.services.out_of_vocab_labels_service import (
    get_out_of_vocab_labels,
    get_all_out_of_vocab_labels_for_version,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.services.knowledge.ontology_service import get_ontology_service

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/out_of_vocab_labels", response_class=JSONResponse)
async def get_out_of_vocab_labels_route(
    concept_type: ConceptTypeEnum = Query(
        description=f"Any one of {[c.value for c in ConceptTypeEnum]}.",
    ),
    ontology_version_id: Optional[str] = Query(
        default=None,
        description=(
            "Ontology version ID to filter by. "
            "If omitted, defaults to the current live ontology version."
        ),
    ),
):
    """
    Returns the set of out-of-vocabulary keywords that have been accepted
    (submitted with a 'Correct, ' prefix) for the given concept_type.

    Scoped to a specific ontology version. If `ontology_version_id` is omitted,
    defaults to the current live ontology version.

    These keywords are candidates for future ontology expansion and are
    excluded from accuracy metrics.
    """
    try:
        if not ontology_version_id:
            ontology_svc = await get_ontology_service()
            ontology = await ontology_svc.get_latest_ontology()
            ontology_version_id = ontology.version_id

        doc = await get_out_of_vocab_labels(
            concept_type=concept_type,
            ontology_version_id=ontology_version_id,
        )
        return doc.model_dump(exclude={"id"}) if doc else {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/out_of_vocab_labels/all", response_class=JSONResponse)
async def get_all_out_of_vocab_labels_route(
    ontology_version_id: Optional[str] = Query(
        default=None,
        description=(
            "Ontology version ID to filter by. "
            "If omitted, defaults to the current live ontology version."
        ),
    ),
):
    """
    Returns all out-of-vocabulary keyword sets across every concept type
    for the given ontology version. If `ontology_version_id` is omitted,
    defaults to the current live ontology version.
    """
    try:
        if not ontology_version_id:
            ontology_svc = await get_ontology_service()
            ontology = await ontology_svc.get_latest_ontology()
            ontology_version_id = ontology.version_id

        docs = await get_all_out_of_vocab_labels_for_version(
            ontology_version_id=ontology_version_id,
        )
        return {
            "ontology_version_id": ontology_version_id,
            "count": len(docs),
            "entries": [doc.model_dump(exclude={"id"}) for doc in docs],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
