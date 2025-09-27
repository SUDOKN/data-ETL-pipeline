import logging

from requests import session
from core.models.field_types import MfgETLDType
from core.utils.aws.s3.s3_client_util import make_s3_client
from core.services.manufacturer_service import find_manufacturer_by_etld1
from core.utils.aws.s3.scraped_text_util import (
    download_scraped_text_from_s3_by_mfg_etld1,
)

from data_etl_app.utils.find_email_addresses import get_validated_emails_from_text

logger = logging.getLogger(__name__)


async def get_validated_emails(mfg_etld1: MfgETLDType) -> list[str]:
    existing_manufacturer = await find_manufacturer_by_etld1(mfg_etld1)
    if not existing_manufacturer:
        raise ValueError(
            f"extract_and_validate_emails: Manufacturer with ETLD1 {mfg_etld1} not found."
        )

    async with make_s3_client(session) as s3_client:
        mfg_text, _version_id = await download_scraped_text_from_s3_by_mfg_etld1(
            s3_client, mfg_etld1, existing_manufacturer.scraped_text_file_version_id
        )
        return get_validated_emails_from_text(mfg_etld1, mfg_text)
