import logging

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse

from data_etl_app.services.validation.rdf_validation_service import (
    validate_rdf_content,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/rdf/validate", response_class=JSONResponse)
async def validate_rdf_file(
    file: UploadFile = File(...),
    include_concept_roots: bool = Query(
        default=False,
        description="Include the full validated_concept_roots list in the response.",
    ),
):
    try:
        include_concept_roots = bool(include_concept_roots) if isinstance(include_concept_roots, bool) else False
        rdf_bytes = await file.read()
        rdf_text = rdf_bytes.decode("utf-8")
        result = validate_rdf_content(rdf_text)
        result["validated_concept_roots_count"] = len(result["validated_concept_roots"])
        if not include_concept_roots:
            result.pop("validated_concept_roots", None)
        return result
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"RDF file must be UTF-8 encoded: {exc}")
    except Exception as exc:
        logger.exception("Failed to validate RDF file")
        raise HTTPException(status_code=400, detail=str(exc))