import json
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from data_etl_app.models.types import ConceptTypeEnum
from data_etl_app.models.skos_concept import ConceptJSONEncoder
from data_etl_app.services.ontology_service import ontology_service
from data_etl_app.utils.route_url_util import (
    ONTOLOGY_REFRESH_URL,
    get_full_ontology_concept_tree_url,
    get_full_ontology_concept_flat_url,
)

router = APIRouter()


@router.get(ONTOLOGY_REFRESH_URL, response_class=JSONResponse)
def refresh_ontology():
    try:
        ontology_service.refresh()
        return {
            "detail": f"Ontology refreshed successfully, version {ontology_service.ontology_version_id}."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Concept nodes endpoints
PROCESS_CONCEPT_TREE_ROUTE = get_full_ontology_concept_tree_url(
    ConceptTypeEnum.process_caps
)
print(PROCESS_CONCEPT_TREE_ROUTE)


@router.get(
    "/ontology/process_caps/tree",
    response_class=JSONResponse,
)
def get_process_concept_nodes():
    try:
        return ontology_service.process_capability_concept_nodes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/material_caps/tree",
    response_class=JSONResponse,
)
def get_material_concept_nodes():
    try:
        return ontology_service.material_capability_concept_nodes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/industries/tree",
    response_class=JSONResponse,
)
def get_industry_concept_nodes():
    try:
        return ontology_service.industry_concept_nodes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/certificates/tree",
    response_class=JSONResponse,
)
def get_certificate_concept_nodes():
    try:
        return ontology_service.certificate_concept_nodes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Processed (flat) endpoints
@router.get(
    "/ontology/process_caps/flat",
    response_class=JSONResponse,
)
def get_process_capabilities():
    try:
        concepts = ontology_service.process_caps
        return JSONResponse(
            content=json.loads(json.dumps(concepts, cls=ConceptJSONEncoder))
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/material_caps/flat",
    response_class=JSONResponse,
)
def get_material_capabilities():
    try:
        concepts = ontology_service.material_caps
        return JSONResponse(
            content=json.loads(json.dumps(concepts, cls=ConceptJSONEncoder))
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/industries/flat",
    response_class=JSONResponse,
)
def get_industries():
    try:
        concepts = ontology_service.industries
        return JSONResponse(
            content=json.loads(json.dumps(concepts, cls=ConceptJSONEncoder))
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/certificates/flat",
    response_class=JSONResponse,
)
def get_certificates():
    try:
        concepts = ontology_service.certificates
        return JSONResponse(
            content=json.loads(json.dumps(concepts, cls=ConceptJSONEncoder))
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
