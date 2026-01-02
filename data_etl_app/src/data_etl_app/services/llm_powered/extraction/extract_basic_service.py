from datetime import datetime
import asyncio
import json
import logging
from typing import Optional

from core.models.db.manufacturer import Address
from core.models.prompt import Prompt

from core.models.db.manufacturer import BusinessDescriptionResult

from core.utils.str_util import make_json_array_parse_safe
from open_ai_key_app.utils.token_util import num_tokens_from_string
from open_ai_key_app.utils.ask_gpt_util import (
    ask_gpt_async,
)
from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)

from data_etl_app.services.knowledge.prompt_service import get_prompt_service
from data_etl_app.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
)

logger = logging.getLogger(__name__)


async def extract_address_from_n_chunks(
    extraction_timestamp: datetime,
    mfg_etld1: str,
    mfg_text: str,
    n: int = 1,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> list[Address]:
    addresses_extracted = []
    prompt_service = await get_prompt_service()
    extract_address_prompt = prompt_service.extract_any_address

    chunks_map = await get_chunks_respecting_line_boundaries(
        text=mfg_text,
        soft_limit_tokens=(
            gpt_model.max_context_tokens - extract_address_prompt.num_tokens - 10_000
        ),  # subtracting 10000 to leave room for last line in each chunk, otherwise _binary_classify_chunk gets > GPT_4o_mini.max_context_tokens
        overlap_ratio=0,
        max_chunks=n,
    )
    tasks = []

    for i, (_bounds, chunk_text) in enumerate(chunks_map.items()):
        if i >= n:
            break
        tasks.append(
            _extract_address_from_chunk(
                extraction_timestamp=extraction_timestamp,
                keyword_label="extract_address",
                mfg_etld1=mfg_etld1,
                chunk_text=chunk_text,
                extract_prompt=extract_address_prompt,
                gpt_model=gpt_model,
                model_params=model_params,
            )
        )
    answers: list[list[Address]] = await asyncio.gather(*tasks)
    for chunk_addresses in answers:
        addresses_extracted.extend(chunk_addresses)

    return addresses_extracted


async def _extract_address_from_chunk(
    extraction_timestamp: datetime,
    keyword_label: str,
    mfg_etld1: str,
    chunk_text: str,
    extract_prompt: Prompt,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> list[Address]:
    chunk_tokens = num_tokens_from_string(chunk_text)
    logger.debug(
        f"_extract_address_from_chunk: extraction_timestamp({extraction_timestamp}), chunk_tokens({chunk_tokens}) for {mfg_etld1}:{keyword_label}"
    )

    gpt_response = await ask_gpt_async(
        chunk_text, extract_prompt.text, gpt_model, model_params
    )

    return parse_address_list_from_gpt_response(gpt_response)


def parse_address_list_from_gpt_response(
    gpt_response: Optional[str],
) -> list[Address]:
    addresses = []
    if not gpt_response:
        logger.error(
            f"parse_address_list_from_gpt_response: Invalid gpt_response:{gpt_response}, returning empty list"
        )
        return []

    try:
        cleaned_response = make_json_array_parse_safe(gpt_response)
    except Exception as e:
        logger.error(
            (
                f"parse_address_list_from_gpt_response: Failed to make_json_parse_safe GPT response: {e}\n",
                f"cleaned_response={gpt_response}, returning empty list",
            ),
            exc_info=True,
        )
        return []

    try:
        json_response = json.loads(cleaned_response)
    except Exception as e:
        logger.error(
            (
                f"parse_address_list_from_gpt_response: Failed to json.loads(cleaned_response): {e}\n",
                f"gpt_response={gpt_response}\n" f"cleaned_response={cleaned_response}",
            ),
            exc_info=True,
        )
        return []

    if isinstance(json_response, list):
        for addr in json_response:
            try:
                country = addr.get("country")
                if not country:
                    addr["country"] = "US"
                else:
                    addr["country"] = country.upper()
                addresses.append(Address(**addr))
            except Exception as e:
                logger.error(
                    f"parse_address_list_from_gpt_response: Skipping failed parsed address from GPT response addr:{addr}\n"
                    f"error={e}",
                    exc_info=True,
                )
    else:
        logger.info(
            f"parse_address_list_from_gpt_response: extracted non-list {json_response}, returning empty list"
        )
        return []

    # dedupe_addresses(addresses=addresses)  # modifies in place, commented out to keep integrity of what was exactly extracted

    return addresses


async def find_business_desc_using_only_first_chunk(
    mfg_etld1: str,
    mfg_text: str,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> BusinessDescriptionResult:
    logger.info(f"Finding business desc for {mfg_etld1} using only first chunk...")
    prompt_service = await get_prompt_service()
    prompt = prompt_service.find_business_desc_prompt
    chunks_map = await get_chunks_respecting_line_boundaries(
        text=mfg_text,
        soft_limit_tokens=(
            gpt_model.max_context_tokens - prompt.num_tokens - 10_000
        ),  # subtracting 10000 to leave room for last line in each chunk, otherwise _binary_classify_chunk gets > GPT_4o_mini.max_context_tokens
        overlap_ratio=0,
        max_chunks=1,  # Only generate the first chunk
    )
    first_chunk_key = min(chunks_map.keys(), key=lambda k: int(k.split(":")[0]))
    first_chunk_text = chunks_map[first_chunk_key]
    gpt_response = await ask_gpt_async(
        first_chunk_text, prompt.text, gpt_model, model_params
    )

    return parse_business_desc_result_from_gpt_response(gpt_response)


def parse_business_desc_result_from_gpt_response(
    gpt_response: Optional[str],
) -> BusinessDescriptionResult:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_business_desc_result: Empty or invalid response from GPT"
        )

    try:
        gpt_response = gpt_response.replace("```", "").replace("json", "")
        json_response = json.loads(gpt_response)
        business_name = json_response.get("name")
        business_desc = json_response.get("description")
    except:
        raise ValueError(
            f"parse_business_desc_result: Invalid response from GPT:{gpt_response}"
        )

    logger.debug(f"parse_business_desc_result:`{business_name}`\n`{business_desc}`")

    return BusinessDescriptionResult(name=business_name, description=business_desc)
