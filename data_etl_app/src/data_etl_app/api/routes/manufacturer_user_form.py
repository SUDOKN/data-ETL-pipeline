from data_etl_app.dependencies.aws_deps import get_sqs_scraper_client
from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
import logging

from shared.models.db.manufacturer import Batch
from shared.models.queue_item import EmailUserErrand
from shared.models.to_scrape_item import ToScrapeItem
from shared.services.manufacturer_service import find_manufacturer_by_etld1
from shared.utils.time_util import get_current_time
from shared.utils.url_util import (
    get_complete_url_with_compatible_protocol,
    get_etld1_from_host,
    get_normalized_url,
)
from shared.utils.aws.queue.priority_scrape_queue_util import (
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
    mfg_url: str = Query(
        default=None, description="Manufacturer URL to fetch the form for"
    ),
    sqs_client=Depends(get_sqs_scraper_client),
):
    current_timestamp = get_current_time()

    try:
        _, mfg_url = get_normalized_url(
            get_complete_url_with_compatible_protocol(mfg_url)
        )
        mfg_etld1 = get_etld1_from_host(mfg_url)
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid URL: '{mfg_url}' is not accessible. Error: {str(e)}",
        )

    form = await get_manufacturer_user_form_by_mfg_etld1(mfg_etld1)
    if not form:
        existing_manufacturer = await find_manufacturer_by_etld1(mfg_etld1)
        if not existing_manufacturer:
            await push_item_to_priority_scrape_queue(
                sqs_client,
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
    return form
