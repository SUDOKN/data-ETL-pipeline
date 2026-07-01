import logging
from typing import List, Optional
from typing_extensions import Literal
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from data_etl_app.models.skos_concept import ConceptNode
from data_etl_app.services.knowledge.ontology_service import (
    OntologyService,
    get_ontology_service,
)
from data_etl_app.models.ontology import Ontology
from data_etl_app.utils.rdf_to_graph_util import (
    find_concept_node_by_name,
    prune_tree_to_depth,
    tree_list_to_flat,
)
from data_etl_app.utils.route_url_util import (
    ONTOLOGY_REFRESH_URL,
)

logger = logging.getLogger(__name__)
router = APIRouter()

Mode = Literal["tree", "flat"]


async def _resolve_ontology(version: Optional[str]) -> Ontology:
    """Resolve the requested ontology version, defaulting to the latest."""
    ontology_service: OntologyService = await get_ontology_service()
    return (
        await ontology_service.get_ontology(version)
        if version
        else await ontology_service.get_latest_ontology()
    )


def _build_concept_response(
    ontology: Ontology,
    root_nodes: List[ConceptNode],
    key: str,
    mode: Mode,
    depth: Optional[int],
    node: Optional[str],
) -> dict:
    """Build a depth-limited concept response in either tree or flat mode.

    When `node` is provided it is matched (case-insensitively, against name or
    alt labels) and used as the starting point; otherwise the concept's root
    nodes are used. `depth` limits how many levels are included (None = full).
    """
    if node is not None:
        found = find_concept_node_by_name(root_nodes, node)
        if found is None:
            raise HTTPException(
                status_code=404,
                detail=f"'{node}' not found in ontology.",
            )
        start_nodes = [found]
    else:
        start_nodes = root_nodes

    pruned = prune_tree_to_depth(start_nodes, depth)
    data = pruned if mode == "tree" else tree_list_to_flat(pruned)

    return {
        "ontology_version_id": ontology.version_id,
        "mode": mode,
        "depth": depth,
        "count": len(data),
        key: data,
    }


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


@router.get("/ontology/process_caps", response_class=JSONResponse)
@router.get("/ontology/process_caps/{node}", response_class=JSONResponse)
async def get_process_caps(
    node: Optional[str] = None,
    mode: Mode = Query("tree", description="Response shape: 'tree' or 'flat'."),
    depth: Optional[int] = Query(
        None,
        ge=1,
        description="Levels to include from the root. Defaults to full depth.",
    ),
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    ),
):
    try:
        ontology = await _resolve_ontology(version)
        return _build_concept_response(
            ontology,
            ontology.process_capability_concept_nodes,
            "process_caps",
            mode,
            depth,
            node,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/material_caps", response_class=JSONResponse)
@router.get("/ontology/material_caps/{node}", response_class=JSONResponse)
async def get_material_caps(
    node: Optional[str] = None,
    mode: Mode = Query("tree", description="Response shape: 'tree' or 'flat'."),
    depth: Optional[int] = Query(
        None,
        ge=1,
        description="Levels to include from the root. Defaults to full depth.",
    ),
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    ),
):
    try:
        ontology = await _resolve_ontology(version)
        return _build_concept_response(
            ontology,
            ontology.material_capability_concept_nodes,
            "material_caps",
            mode,
            depth,
            node,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/industries", response_class=JSONResponse)
@router.get("/ontology/industries/{node}", response_class=JSONResponse)
async def get_industries(
    node: Optional[str] = None,
    mode: Mode = Query("tree", description="Response shape: 'tree' or 'flat'."),
    depth: Optional[int] = Query(
        None,
        ge=1,
        description="Levels to include from the root. Defaults to full depth.",
    ),
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    ),
):
    try:
        ontology = await _resolve_ontology(version)
        return _build_concept_response(
            ontology,
            ontology.industry_concept_nodes,
            "industries",
            mode,
            depth,
            node,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/certificates", response_class=JSONResponse)
@router.get("/ontology/certificates/{node}", response_class=JSONResponse)
async def get_certificates(
    node: Optional[str] = None,
    mode: Mode = Query("tree", description="Response shape: 'tree' or 'flat'."),
    depth: Optional[int] = Query(
        None,
        ge=1,
        description="Levels to include from the root. Defaults to full depth.",
    ),
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    ),
):
    try:
        ontology = await _resolve_ontology(version)
        return _build_concept_response(
            ontology,
            ontology.certificate_concept_nodes,
            "certificates",
            mode,
            depth,
            node,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/ownership_statuses", response_class=JSONResponse)
@router.get("/ontology/ownership_statuses/{node}", response_class=JSONResponse)
async def get_ownership_statuses(
    node: Optional[str] = None,
    mode: Mode = Query("tree", description="Response shape: 'tree' or 'flat'."),
    depth: Optional[int] = Query(
        None,
        ge=1,
        description="Levels to include from the root. Defaults to full depth.",
    ),
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    ),
):
    try:
        ontology = await _resolve_ontology(version)
        return _build_concept_response(
            ontology,
            ontology.ownership_concept_nodes,
            "ownership_statuses",
            mode,
            depth,
            node,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ontology/naics", response_class=JSONResponse)
@router.get("/ontology/naics/{node}", response_class=JSONResponse)
async def get_naics_codes(
    node: Optional[str] = None,
    mode: Mode = Query("tree", description="Response shape: 'tree' or 'flat'."),
    depth: Optional[int] = Query(
        None,
        ge=1,
        description="Levels to include from the root. Defaults to full depth.",
    ),
    version: Optional[str] = Query(
        None, description="S3 version ID. Defaults to latest."
    ),
):
    try:
        ontology = await _resolve_ontology(version)
        return _build_concept_response(
            ontology,
            ontology.naics_concept_nodes,
            "naics",
            mode,
            depth,
            node,
        )
    except HTTPException:
        raise
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
