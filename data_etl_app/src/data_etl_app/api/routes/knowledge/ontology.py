import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
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
        ontology = await ontology_service.refresh()
        return {
            "detail": f"Ontology refreshed successfully, version {ontology.version_id}."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/process_caps/tree",
    response_class=JSONResponse,
)
async def get_process_concept_nodes(
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    )
):
    try:
        ontology_service = await get_ontology_service()
        ontology = (
            await ontology_service.get_ontology(version)
            if version
            else await ontology_service.get_latest_ontology()
        )
        process_caps = ontology.process_capability_concept_nodes
        return {
            "ontology_version_id": ontology.version_id,
            "count": len(process_caps),
            "process_caps": process_caps,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/material_caps/tree",
    response_class=JSONResponse,
)
async def get_material_concept_nodes(
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    )
):
    try:
        ontology_service = await get_ontology_service()
        ontology = (
            await ontology_service.get_ontology(version)
            if version
            else await ontology_service.get_latest_ontology()
        )
        material_caps = ontology.material_capability_concept_nodes
        return {
            "ontology_version_id": ontology.version_id,
            "count": len(material_caps),
            "material_caps": material_caps,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/industries/tree",
    response_class=JSONResponse,
)
async def get_industry_concept_nodes(
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    )
):
    try:
        ontology_service = await get_ontology_service()
        ontology = (
            await ontology_service.get_ontology(version)
            if version
            else await ontology_service.get_latest_ontology()
        )
        industries = ontology.industry_concept_nodes
        return {
            "ontology_version_id": ontology.version_id,
            "count": len(industries),
            "industries": industries,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/certificates/tree",
    response_class=JSONResponse,
)
async def get_certificate_concept_nodes(
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    )
):
    try:
        ontology_service = await get_ontology_service()
        ontology = (
            await ontology_service.get_ontology(version)
            if version
            else await ontology_service.get_latest_ontology()
        )
        certificates = ontology.certificate_concept_nodes
        return {
            "ontology_version_id": ontology.version_id,
            "count": len(certificates),
            "certificates": certificates,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/naics/tree",
    response_class=JSONResponse,
)
async def get_naics_concept_nodes(
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    )
):
    try:
        ontology_service = await get_ontology_service()
        ontology = (
            await ontology_service.get_ontology(version)
            if version
            else await ontology_service.get_latest_ontology()
        )
        naics = ontology.naics_concept_nodes
        return {
            "ontology_version_id": ontology.version_id,
            "count": len(naics),
            "naics": naics,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/ownership_statuses/tree",
    response_class=JSONResponse,
)
async def get_ownership_status_nodes(
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    )
):
    try:
        ontology_service = await get_ontology_service()
        ontology = (
            await ontology_service.get_ontology(version)
            if version
            else await ontology_service.get_latest_ontology()
        )
        ownership_statuses = ontology.ownership_concept_nodes
        return {
            "ontology_version_id": ontology.version_id,
            "count": len(ownership_statuses),
            "ownership_statuses": ownership_statuses,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Processed (flat) endpoints ----------------------------------------------------------- #
@router.get(
    "/ontology/process_caps/flat",
    response_class=JSONResponse,
)
async def get_process_capabilities(
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    )
):
    try:
        ontology_service = await get_ontology_service()
        ontology = (
            await ontology_service.get_ontology(version)
            if version
            else await ontology_service.get_latest_ontology()
        )
        process_caps = ontology.process_caps
        return {
            "ontology_version_id": ontology.version_id,
            "count": len(process_caps),
            "process_caps": process_caps,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/material_caps/flat",
    response_class=JSONResponse,
)
async def get_material_capabilities(
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    )
):
    try:
        ontology_service = await get_ontology_service()
        ontology = (
            await ontology_service.get_ontology(version)
            if version
            else await ontology_service.get_latest_ontology()
        )
        material_caps = ontology.material_caps
        return {
            "ontology_version_id": ontology.version_id,
            "count": len(material_caps),
            "material_caps": material_caps,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/industries/flat",
    response_class=JSONResponse,
)
async def get_industries(
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    )
):
    try:
        ontology_service = await get_ontology_service()
        ontology = (
            await ontology_service.get_ontology(version)
            if version
            else await ontology_service.get_latest_ontology()
        )
        industries = ontology.industries
        return {
            "ontology_version_id": ontology.version_id,
            "count": len(industries),
            "industries": industries,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/certificates/flat",
    response_class=JSONResponse,
)
async def get_certificates(
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    )
):
    try:
        ontology_service = await get_ontology_service()
        ontology = (
            await ontology_service.get_ontology(version)
            if version
            else await ontology_service.get_latest_ontology()
        )
        certificates = ontology.certificates
        return {
            "ontology_version_id": ontology.version_id,
            "count": len(certificates),
            "certificates": certificates,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/ownership_statuses/flat",
    response_class=JSONResponse,
)
async def get_ownership_statuses(
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    )
):
    try:
        ontology_service = await get_ontology_service()
        ontology = (
            await ontology_service.get_ontology(version)
            if version
            else await ontology_service.get_latest_ontology()
        )
        ownership_statuses = ontology.ownership_statuses
        return {
            "ontology_version_id": ontology.version_id,
            "count": len(ownership_statuses),
            "ownership_statuses": ownership_statuses,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/ontology/naics/flat",
    response_class=JSONResponse,
)
async def get_naics_codes(
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    )
):
    try:
        ontology_service = await get_ontology_service()
        ontology = (
            await ontology_service.get_ontology(version)
            if version
            else await ontology_service.get_latest_ontology()
        )
        naics = ontology.naics_codes
        return {
            "ontology_version_id": ontology.version_id,
            "count": len(naics),
            "naics": naics,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/service-info")
async def get_service_info():
    """Debug endpoint to check ontology service multi-version cache behavior."""
    try:
        ontology_service = await get_ontology_service()
        service_info = ontology_service.get_service_info()
        return {
            "service_info": service_info,
            "message": "Service information retrieved successfully",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
