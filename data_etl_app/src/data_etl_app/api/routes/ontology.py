import json
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from data_etl_app.services.ontology_service import ontology_service
from data_etl_app.models.skos_concept import ConceptJSONEncoder

router = APIRouter()


@router.get("/ontology/refresh", response_class=JSONResponse)
def refresh_ontology():
    try:
        ontology_service.refresh()
        return {"detail": "Ontology refreshed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Concept nodes endpoints
@router.get("/ontology/process/raw", response_class=JSONResponse)
def get_process_concept_nodes():
    try:
        return ontology_service.process_capability_concept_nodes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/material/raw", response_class=JSONResponse)
def get_material_concept_nodes():
    try:
        return ontology_service.material_capability_concept_nodes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/industry/raw", response_class=JSONResponse)
def get_industry_concept_nodes():
    try:
        return ontology_service.industry_concept_nodes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/certificate/raw", response_class=JSONResponse)
def get_certificate_concept_nodes():
    try:
        return ontology_service.certificate_concept_nodes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Processed (flat) endpoints
@router.get("/ontology/process", response_class=JSONResponse)
def get_process_capabilities():
    try:
        concepts = ontology_service.process_capabilities
        return JSONResponse(
            content=json.loads(json.dumps(concepts, cls=ConceptJSONEncoder))
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/material", response_class=JSONResponse)
def get_material_capabilities():
    try:
        concepts = ontology_service.material_capabilities
        return JSONResponse(
            content=json.loads(json.dumps(concepts, cls=ConceptJSONEncoder))
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/industry", response_class=JSONResponse)
def get_industries():
    try:
        concepts = ontology_service.industries
        return JSONResponse(
            content=json.loads(json.dumps(concepts, cls=ConceptJSONEncoder))
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/certificate", response_class=JSONResponse)
def get_certificates():
    try:
        concepts = ontology_service.certificates
        return JSONResponse(
            content=json.loads(json.dumps(concepts, cls=ConceptJSONEncoder))
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
