from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
import logging

from data_etl_app.services.knowledge.prompt_service import get_prompt_service

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/prompts/refresh", response_class=JSONResponse)
async def refresh_prompts():
    """Refresh all prompts from S3."""
    try:
        prompt_service = await get_prompt_service()
        await prompt_service.refresh()
        return {"detail": "Prompts refreshed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/find_business_desc", response_class=JSONResponse)
async def get_find_business_name_prompt():
    """Get the find business name prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.find_business_desc_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/is_manufacturer", response_class=JSONResponse)
async def get_is_manufacturer_prompt():
    """Get the is manufacturer prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.is_manufacturer_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/is_product_manufacturer", response_class=JSONResponse)
async def get_is_product_manufacturer_prompt():
    """Get the is product manufacturer prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.is_product_manufacturer_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/is_contract_manufacturer", response_class=JSONResponse)
async def get_is_contract_manufacturer_prompt():
    """Get the is contract manufacturer prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.is_contract_manufacturer_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/extract_any_address", response_class=JSONResponse)
async def get_extract_any_address_prompt():
    """Get the extract any address prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.extract_any_address
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/extract_any_product", response_class=JSONResponse)
async def get_extract_any_product_prompt():
    """Get the extract any product prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.extract_any_product_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/extract_any_certificate", response_class=JSONResponse)
async def get_extract_any_certificate_prompt():
    """Get the extract any certificate prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.extract_any_certificate_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/extract_any_industry", response_class=JSONResponse)
async def get_extract_any_industry_prompt():
    """Get the extract any industry prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.extract_any_industry_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/extract_any_material_cap", response_class=JSONResponse)
async def get_extract_any_material_cap_prompt():
    """Get the extract any material capability prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.extract_any_material_cap_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/extract_any_process_cap", response_class=JSONResponse)
async def get_extract_any_process_cap_prompt():
    """Get the extract any process capability prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.extract_any_process_cap_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/unknown_to_known_certificate", response_class=JSONResponse)
async def get_unknown_to_known_certificate_prompt():
    """Get the unknown to known certificate prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.unknown_to_known_certificate_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/unknown_to_known_industry", response_class=JSONResponse)
async def get_unknown_to_known_industry_prompt():
    """Get the unknown to known industry prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.unknown_to_known_industry_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/unknown_to_known_material_cap", response_class=JSONResponse)
async def get_unknown_to_known_material_cap_prompt():
    """Get the unknown to known material capability prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.unknown_to_known_material_cap_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/unknown_to_known_process_cap", response_class=JSONResponse)
async def get_unknown_to_known_process_cap_prompt():
    """Get the unknown to known process capability prompt."""
    try:
        prompt_service = await get_prompt_service()
        prompt = prompt_service.unknown_to_known_process_cap_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/all", response_class=JSONResponse)
async def get_all_prompts():
    """Get all available prompts."""
    try:
        prompt_service = await get_prompt_service()
        prompts = {}
        for prompt_name in [
            "find_business_desc",
            "is_manufacturer",
            "is_product_manufacturer",
            "is_contract_manufacturer",
            "extract_any_certificate",
            "extract_any_industry",
            "extract_any_material_cap",
            "extract_any_process_cap",
            "unknown_to_known_certificate",
            "unknown_to_known_industry",
            "unknown_to_known_material_cap",
            "unknown_to_known_process_cap",
        ]:
            prompt = getattr(prompt_service, f"{prompt_name}_prompt")
            prompts[prompt_name] = prompt.model_dump()

        return {
            "prompts": prompts,
            "count": len(prompts),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/service-info")
async def get_service_info():
    """Debug endpoint to check prompt service singleton behavior."""
    try:
        prompt_service = await get_prompt_service()
        service_info = {
            "instance_id": id(prompt_service),
            "cached_prompts": (
                list(prompt_service._prompt_cache.keys())
                if hasattr(prompt_service, "_prompt_cache")
                else []
            ),
            "prompt_count": (
                len(prompt_service._prompt_cache)
                if hasattr(prompt_service, "_prompt_cache")
                else 0
            ),
        }
        return {
            "service_info": service_info,
            "message": "Service information retrieved successfully",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
