import logging
from enum import Enum

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse, Response

from core.services.manufacturer_service import find_manufacturer_by_etld1
from core.services.user_service import find_by_email
from core.utils.aws.s3.scraped_text_util import (
    download_scraped_text_from_s3_by_mfg_etld1,
    get_file_name_from_mfg_etld,
    get_scraped_text_file_exist_last_modified_on,
)
from open_ai_key_app.utils.token_util import num_tokens_from_string

router = APIRouter()
logger = logging.getLogger(__name__)


class ScrapedTextResponseFormat(str, Enum):
    text = "text"
    json = "json"


@router.get(
    "/scraped_text",
)
async def get_scraped_text(
    author_email: str = Query(
        description=(
            "Author email query param is required. This is used to verify you are a registered user. "
            "To add your email as query param, simply append the URL with `?author_email=your_email@example.com`. "
            "If you are using postman, you can use the `Params` tab to add a query param."
        ),
    ),
    mfg_etld1: str = Query(
        description="Manufacturer effective top-level domain (e.g. 'example.com')"
    ),
    format: ScrapedTextResponseFormat = Query(
        default=ScrapedTextResponseFormat.text,
        description="Response format: 'text' returns plain text with a metadata header; 'json' returns {mfg_etld1, scraped_text, version_id, num_tokens, file_created_on}.",
    ),
) -> Response:
    user = await find_by_email(author_email)
    if not user:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sorry, no registered user found with email: {author_email}. "
                f"Please register on `sudokn.com` to fetch scraped text."
            ),
        )

    manufacturer = await find_manufacturer_by_etld1(mfg_etld1)
    if not manufacturer:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sorry, no manufacturer found with effective top-level domain: {mfg_etld1}. "
                f"Please provide a valid mfg_etld1."
            ),
        )

    version_id = manufacturer.scraped_text_file_version_id
    if not version_id:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No scraped file found for manufacturer with etld1: {mfg_etld1}. "
                f"The manufacturer may not have been scraped yet."
            ),
        )

    scraped_text, resolved_version_id = (
        await download_scraped_text_from_s3_by_mfg_etld1(
            etld1=mfg_etld1,
            version_id=version_id,
        )
    )

    num_tokens = num_tokens_from_string(scraped_text)

    file_name = get_file_name_from_mfg_etld(mfg_etld1)
    last_modified = await get_scraped_text_file_exist_last_modified_on(
        file_name=file_name, version_id=resolved_version_id
    )
    file_created_on = last_modified.isoformat() if last_modified else None

    logger.debug(
        f"Returning scraped text for mfg_etld1: {mfg_etld1}, version_id: {resolved_version_id}, "
        f"num_tokens: {num_tokens}, file_created_on: {file_created_on}, format: {format}"
    )

    if format == ScrapedTextResponseFormat.json:
        return JSONResponse(
            content={
                "mfg_etld1": mfg_etld1,
                "version_id": resolved_version_id,
                "num_tokens": num_tokens,
                "file_created_on": file_created_on,
                "scraped_text": scraped_text,
            }
        )

    return PlainTextResponse(
        content=(
            f"mfg_etld1: {mfg_etld1}\n"
            f"version_id: {resolved_version_id}\n"
            f"num_tokens: {num_tokens}\n"
            f"file_created_on: {file_created_on}\n\n"
            f"{scraped_text}"
        )
    )
