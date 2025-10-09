import logging
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from data_etl_app.services.knowledge.ontology_service import get_ontology_service
from data_etl_app.utils.route_url_util import (
    ONTOLOGY_REFRESH_URL,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(ONTOLOGY_REFRESH_URL, response_class=JSONResponse)
async def refresh_ontology():
    try:
        ontology_service = await get_ontology_service()
        await ontology_service.refresh()
        return {
            "detail": f"Ontology refreshed successfully, version {ontology_service.ontology.s3_version_id}."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/process_caps/tree",
    response_class=JSONResponse,
)
async def get_process_concept_nodes():
    try:
        ontology_service = await get_ontology_service()
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
async def get_material_concept_nodes():
    try:
        ontology_service = await get_ontology_service()
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
async def get_industry_concept_nodes():
    try:
        ontology_service = await get_ontology_service()
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
async def get_certificate_concept_nodes():
    try:
        ontology_service = await get_ontology_service()
        concept_node_data = ontology_service.certificate_concept_nodes
        return {
            "ontology_version_id": concept_node_data[0],
            "certificates": concept_node_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/naics/tree",
    response_class=JSONResponse,
)
async def get_naics_concept_nodes():
    try:
        ontology_service = await get_ontology_service()
        concept_node_data = ontology_service.naics_concept_nodes
        return {
            "ontology_version_id": concept_node_data[0],
            "naics": concept_node_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/ownership_statuses/tree",
    response_class=JSONResponse,
)
async def get_ownership_status_nodes():
    try:
        ontology_service = await get_ontology_service()
        concept_node_data = ontology_service.ownership_concept_nodes
        return {
            "ontology_version_id": concept_node_data[0],
            "ownership_statuses": concept_node_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Processed (flat) endpoints ----------------------------------------------------------- #
@router.get(
    "/ontology/process_caps/flat",
    response_class=JSONResponse,
)
async def get_process_capabilities():
    try:
        ontology_service = await get_ontology_service()
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
async def get_material_capabilities():
    try:
        ontology_service = await get_ontology_service()
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
async def get_industries():
    try:
        ontology_service = await get_ontology_service()
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
async def get_certificates():
    try:
        ontology_service = await get_ontology_service()
        concept_data = ontology_service.certificates
        return {
            "ontology_version_id": concept_data[0],
            "certificates": concept_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/ownership_statuses/flat",
    response_class=JSONResponse,
)
async def get_ownership_statuses():
    try:
        ontology_service = await get_ontology_service()
        concept_data = ontology_service.ownership_statuses
        return {
            "ontology_version_id": concept_data[0],
            "ownership_statuses": concept_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/naics/flat",
    response_class=JSONResponse,
)
async def get_naics_codes():
    try:
        ontology_service = await get_ontology_service()
        concept_data = ontology_service.naics_codes
        return {
            "ontology_version_id": concept_data[0],
            "naics": concept_data[1],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/service-info")
async def get_service_info():
    """Debug endpoint to check ontology service singleton behavior."""
    try:
        ontology_service = await get_ontology_service()
        service_info = ontology_service.get_service_info()
        return {
            "service_info": service_info,
            "message": "Service information retrieved successfully",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
