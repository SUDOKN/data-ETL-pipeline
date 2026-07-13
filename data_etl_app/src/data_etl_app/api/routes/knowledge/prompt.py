from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
import logging

from core.models.llm_model import get_llm_model_by_name, LLM_Model
from data_etl_app.services.knowledge.prompt_service import get_prompt_service

logger = logging.getLogger(__name__)
router = APIRouter()


def _resolve_llm_model(model_name: str) -> LLM_Model:
    """Resolve a model name to an LLM_Model, raising 400 on unknown names."""
    try:
        return get_llm_model_by_name(model_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/prompts/refresh", response_class=JSONResponse)
async def refresh_prompts(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Refresh all prompts from S3."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        await prompt_service.refresh()
        return {"detail": "Prompts refreshed successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/find_business_desc", response_class=JSONResponse)
async def get_find_business_name_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the find business name prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.find_business_desc_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/is_manufacturer", response_class=JSONResponse)
async def get_is_manufacturer_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the is manufacturer prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.is_manufacturer_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/is_product_manufacturer", response_class=JSONResponse)
async def get_is_product_manufacturer_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the is product manufacturer prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.is_product_manufacturer_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/is_contract_manufacturer", response_class=JSONResponse)
async def get_is_contract_manufacturer_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the is contract manufacturer prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.is_contract_manufacturer_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/extract_any_address", response_class=JSONResponse)
async def get_extract_any_address_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the extract any address prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.extract_any_address_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/product_phrase_search", response_class=JSONResponse)
async def get_product_phrase_search_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the product phrase search prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.product_phrase_search_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/certificate_phrase_search", response_class=JSONResponse)
async def get_certificate_phrase_search_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the certificate phrase search prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.certificate_phrase_search_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/industry_phrase_search", response_class=JSONResponse)
async def get_industry_phrase_search_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the industry phrase search prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.industry_phrase_search_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/material_cap_phrase_search", response_class=JSONResponse)
async def get_material_cap_phrase_search_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the material capability phrase search prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.material_cap_phrase_search_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/process_cap_phrase_search", response_class=JSONResponse)
async def get_process_cap_phrase_search_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the process capability phrase search prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.process_cap_phrase_search_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/prompts/certificate_phrase_initial_grounding", response_class=JSONResponse
)
async def get_certificate_phrase_initial_grounding_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the certificate phrase initial grounding prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.certificate_phrase_initial_grounding_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/industry_phrase_initial_grounding", response_class=JSONResponse)
async def get_industry_phrase_initial_grounding_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the industry phrase initial grounding prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.industry_phrase_initial_grounding_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/prompts/material_cap_phrase_initial_grounding", response_class=JSONResponse
)
async def get_material_cap_phrase_initial_grounding_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the material capability phrase initial grounding prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.material_cap_phrase_initial_grounding_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/prompts/process_cap_phrase_initial_grounding", response_class=JSONResponse
)
async def get_process_cap_phrase_initial_grounding_prompt(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get the process capability phrase initial grounding prompt."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompt = prompt_service.process_cap_phrase_initial_grounding_prompt
        return prompt.model_dump()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prompts/all", response_class=JSONResponse)
async def get_all_prompts(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Get all available prompts."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
        prompts = {}
        for prompt_name in [
            "find_business_desc",
            "is_manufacturer",
            "is_product_manufacturer",
            "is_contract_manufacturer",
            "certificate_phrase_search",
            "industry_phrase_search",
            "material_cap_phrase_search",
            "process_cap_phrase_search",
            "certificate_phrase_initial_grounding",
            "industry_phrase_initial_grounding",
            "material_cap_phrase_initial_grounding",
            "process_cap_phrase_initial_grounding",
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
async def get_service_info(
    model_name: str = Query(description="LLM model name used for token counting"),
):
    """Debug endpoint to check prompt service singleton behavior."""
    try:
        llm_model = _resolve_llm_model(model_name)
        prompt_service = await get_prompt_service(llm_model)
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
            "model_name": (
                prompt_service.llm_model.name if prompt_service.llm_model else None
            ),
        }
        return {
            "service_info": service_info,
            "message": "Service information retrieved successfully",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
