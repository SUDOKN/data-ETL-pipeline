from core.services.graph_db_manufacturer_service import replace_manufacturer_in_graph
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
import logging
from pymongo.errors import DuplicateKeyError

from core.services.manufacturer_service import find_manufacturer_by_etld1
from core.utils.url_util import get_etld1_from_host
from core.utils.graph_db_client import SPARQLQueryError

from core.models.db.manufacturer_user_form import ManufacturerUserForm
from data_etl_app.services.manufacturer_user_form_service import (
    create_blank_manufacturer_user_form,
    enqueue_manufacturer_for_priority_scrape,
    validate_and_create_from_manufacturer,
    get_manufacturer_user_form_by_mfg_etld1,
    save_manufacturer_user_form,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/manufacturer_user_form", response_class=JSONResponse)
async def fetch_manufacturer_user_form(
    author_email: str = Query(
        description=(
            f"Author email query param is required. This is used to generate customized template for you. "
            f"To add your email as query param, simply append the URL with `?author_email=your_email@example.com`. "
            f"If you are using postman, you can use the `Params` tab to add a query param."
        ),
    ),
    mfg_url: str = Query(description="Manufacturer URL to fetch the form for"),
):
    mfg_etld1 = get_etld1_from_host(mfg_url)
    if not mfg_etld1:
        raise HTTPException(
            status_code=400,
            detail=f"Could not extract etld1 from the provided URL: {mfg_url}",
        )

    logger.info(f"Fetching ManufacturerUserForm for etld1: {mfg_etld1}")
    form = await get_manufacturer_user_form_by_mfg_etld1(mfg_etld1)
    if not form:
        existing_manufacturer = await find_manufacturer_by_etld1(mfg_etld1)
        if not existing_manufacturer:
            logger.info(
                f"Manufacturer with etld1 {mfg_etld1} not found, pushing to scrape queue."
            )
            await enqueue_manufacturer_for_priority_scrape(
                author_email=author_email,
                mfg_url=mfg_url,
                title="Userform Binary Classification",
            )
            raise HTTPException(
                status_code=404,
                detail=f"Manufacturer with etld1 {mfg_etld1} not found. Manufacturer has been pushed to the extract queue for processing. Please try again later.",
            )
        else:
            logger.info(
                f"Manufacturer with etld1 {mfg_etld1} found, but no ManufacturerUserForm exists. Validating/extracting..."
            )
            try:
                form = await validate_and_create_from_manufacturer(
                    existing_manufacturer
                )
                form.author_email = author_email
            except Exception as e:
                # some of the fields weren't extracted successfully, push to pipeline again
                logger.error(
                    f"Error validating/extracting ManufacturerUserForm for etld1 {mfg_etld1}: {e}"
                )
                await enqueue_manufacturer_for_priority_scrape(
                    author_email=author_email,
                    mfg_url=mfg_url,
                    title="Userform Binary Classification, Repush",
                )
                raise HTTPException(
                    status_code=208,
                    detail=f"Manufacturer with etld1 {mfg_etld1} is being processed right now, please try again later.",
                )

    return form


@router.post("/manufacturer_user_form/extract", response_class=JSONResponse)
async def start_manufacturer_extraction(
    author_email: str = Query(
        description="Author email query param is required to queue manufacturer extraction.",
    ),
    mfg_url: str = Query(description="Manufacturer URL to extract"),
):
    mfg_etld1 = get_etld1_from_host(mfg_url)
    if not mfg_etld1:
        raise HTTPException(
            status_code=400,
            detail=f"Could not extract etld1 from the provided URL: {mfg_url}",
        )

    existing_form = await get_manufacturer_user_form_by_mfg_etld1(mfg_etld1)
    if existing_form:
        return existing_form

    existing_manufacturer = await find_manufacturer_by_etld1(mfg_etld1)
    if existing_manufacturer:
        try:
            form = await validate_and_create_from_manufacturer(existing_manufacturer)
            form.author_email = author_email
            await save_manufacturer_user_form(form)
            return form
        except DuplicateKeyError:
            existing_form = await get_manufacturer_user_form_by_mfg_etld1(mfg_etld1)
            if existing_form:
                return existing_form
            raise
        except Exception as e:
            logger.error(
                f"Error validating/extracting ManufacturerUserForm for etld1 {mfg_etld1}: {e}"
            )

    logger.info(
        f"Manufacturer with etld1 {mfg_etld1} not found, pushing to scrape queue."
    )
    await enqueue_manufacturer_for_priority_scrape(
        author_email=author_email,
        mfg_url=mfg_url,
        title="Userform Binary Classification",
    )

    return JSONResponse(
        status_code=202,
        content={
            "detail": (
                f"Manufacturer with etld1 {mfg_etld1} has been pushed to the extract queue for processing."
            )
        },
    )


@router.post("/manufacturer_user_form/draft", response_class=JSONResponse)
async def create_manufacturer_user_form_draft(
    author_email: str = Query(
        description="Author email query param is required to seed the draft form.",
    ),
    mfg_url: str = Query(description="Manufacturer URL to seed the draft for"),
):
    mfg_etld1 = get_etld1_from_host(mfg_url)
    if not mfg_etld1:
        raise HTTPException(
            status_code=400,
            detail=f"Could not extract etld1 from the provided URL: {mfg_url}",
        )

    existing_form = await get_manufacturer_user_form_by_mfg_etld1(mfg_etld1)
    if existing_form:
        return existing_form

    existing_manufacturer = await find_manufacturer_by_etld1(mfg_etld1)
    if existing_manufacturer:
        try:
            form = await validate_and_create_from_manufacturer(existing_manufacturer)
            form.author_email = author_email
            await save_manufacturer_user_form(form)
            return form
        except DuplicateKeyError:
            existing_form = await get_manufacturer_user_form_by_mfg_etld1(mfg_etld1)
            if existing_form:
                return existing_form
            raise
        except Exception as e:
            logger.error(
                f"Error validating/extracting ManufacturerUserForm for etld1 {mfg_etld1}: {e}"
            )

    draft = await create_blank_manufacturer_user_form(
        author_email=author_email,
        mfg_etld1=mfg_etld1,
    )
    return draft


@router.post("/manufacturer_user_form", response_class=JSONResponse)
async def upsert_manufacturer_user_form(
    form: ManufacturerUserForm,
):
    existing_form = await get_manufacturer_user_form_by_mfg_etld1(form.mfg_etld1)
    if existing_form and not form.id:
        raise ValueError(
            f"ManufacturerUserForm for etld1 {form.mfg_etld1} already exists. To update, "
            f"please fetch the existing form and then include the id field."
        )
    await save_manufacturer_user_form(form)
    try:
        await replace_manufacturer_in_graph(form.mfg_etld1)
    except SPARQLQueryError as exc:
        logger.exception(
            "Failed to sync ManufacturerUserForm for etld1 %s to GraphDB",
            form.mfg_etld1,
        )
        raise HTTPException(
            status_code=503,
            detail=(
                f"ManufacturerUserForm was saved, but GraphDB sync failed for etld1 {form.mfg_etld1}. "
                "Please retry shortly."
            ),
        ) from exc
    return form
