import json
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
import logging

from data_etl_app.models.types import ConceptTypeEnum
from data_etl_app.models.skos_concept import ConceptJSONEncoder
from data_etl_app.services.ontology_service import ontology_service
from data_etl_app.utils.route_url_util import (
    ONTOLOGY_REFRESH_URL,
    get_full_ontology_concept_tree_url,
)

logger = logging.getLogger(__name__)
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
logger.debug(f"Process concept tree route: {PROCESS_CONCEPT_TREE_ROUTE}")


@router.get(
    "/ontology/process_caps/tree",
    response_class=JSONResponse,
)
def get_process_concept_nodes():
    try:
        concept_node_data = ontology_service.process_capability_concept_nodes
        return {
            "ontology_version_id": concept_node_data[0],
            "process_caps": concept_node_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/material_caps/tree",
    response_class=JSONResponse,
)
def get_material_concept_nodes():
    try:
        concept_node_data = ontology_service.material_capability_concept_nodes
        return {
            "ontology_version_id": concept_node_data[0],
            "material_caps": concept_node_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/industries/tree",
    response_class=JSONResponse,
)
def get_industry_concept_nodes():
    try:
        concept_node_data = ontology_service.industry_concept_nodes
        return {
            "ontology_version_id": concept_node_data[0],
            "industries": concept_node_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/certificates/tree",
    response_class=JSONResponse,
)
def get_certificate_concept_nodes():
    try:
        concept_node_data = ontology_service.certificate_concept_nodes
        return {
            "ontology_version_id": concept_node_data[0],
            "certificates": concept_node_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Processed (flat) endpoints
@router.get(
    "/ontology/process_caps/flat",
    response_class=JSONResponse,
)
def get_process_capabilities():
    try:
        concept_data = ontology_service.process_caps
        return {
            "ontology_version_id": concept_data[0],
            "process_caps": concept_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/material_caps/flat",
    response_class=JSONResponse,
)
def get_material_capabilities():
    try:
        concept_data = ontology_service.material_caps
        return {
            "ontology_version_id": concept_data[0],
            "material_caps": concept_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/industries/flat",
    response_class=JSONResponse,
)
def get_industries():
    try:
        concept_data = ontology_service.industries
        return {
            "ontology_version_id": concept_data[0],
            "industries": concept_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/certificates/flat",
    response_class=JSONResponse,
)
def get_certificates():
    try:
        concept_data = ontology_service.certificates
        return {
            "ontology_version_id": concept_data[0],
            "certificates": concept_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/service-info")
def get_service_info():
    """Debug endpoint to check ontology service singleton behavior."""
    try:
        service_info = ontology_service.get_service_info()
        return {
            "service_info": service_info,
            "message": "Service information retrieved successfully",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
