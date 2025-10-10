from core.services.graph_db_manufacturer_service import replace_manufacturer_in_graph
from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
import logging

from core.models.db.manufacturer import Batch
from core.models.queue_item import EmailUserErrand
from core.models.to_scrape_item import ToScrapeItem
from core.services.manufacturer_service import find_manufacturer_by_etld1
from core.utils.time_util import get_current_time
from core.utils.url_util import (
    get_etld1_from_host,
)
from core.utils.aws.queue.priority_scrape_queue_util import (
    push_item_to_priority_scrape_queue,
)

from data_etl_app.models.db.manufacturer_user_form import ManufacturerUserForm
from data_etl_app.services.manufacturer_user_form_service import (
    create_from_manufacturer,
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
    current_timestamp = get_current_time()
    mfg_etld1 = get_etld1_from_host(mfg_url)
    if not mfg_etld1:
        raise HTTPException(
            status_code=400,
            detail=f"Could not extract etld1 from the provided URL: {mfg_url}",
        )

    form = await get_manufacturer_user_form_by_mfg_etld1(mfg_etld1)
    if not form:
        existing_manufacturer = await find_manufacturer_by_etld1(mfg_etld1)
        if not existing_manufacturer:
            await push_item_to_priority_scrape_queue(
                ToScrapeItem(
                    accessible_normalized_url=mfg_url,
                    batch=Batch(
                        title="Ground Truth API: Binary Classification",
                        timestamp=current_timestamp,  # ISO format for timestamp
                    ),
                    email_errand=EmailUserErrand(user_email=author_email),
                ),
            )
            raise HTTPException(
                status_code=404,
                detail=f"Manufacturer with etld1 {mfg_etld1} not found. Manufacturer has been pushed to the extract queue for processing. Please try again later.",
            )

        form = await create_from_manufacturer(existing_manufacturer)
        form.author_email = author_email

    return form


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
    await replace_manufacturer_in_graph(form.mfg_etld1)
    return form
