from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time
import logging
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, computed_field

from data_etl_app.services.ground_truth.concept_ground_truth_service import (
    does_a_cgt_exist_with_scraped_file_version,
)
from data_etl_app.services.ground_truth.keyword_ground_truth_service import (
    does_a_kgt_exist_with_scraped_file_version,
)
from data_etl_app.services.ground_truth.binary_ground_truth_service import (
    does_a_bgt_exist_with_scraped_file_version,
)

from open_ai_key_app.utils.ask_gpt_util import num_tokens_from_string
from scraper_app.services.url_scraper_service import ScrapingResult
from core.models.db.manufacturer import Batch
from core.utils.aws.s3.scraped_text_util import (
    delete_scraped_text_from_s3_by_etld1,
    get_file_name_from_mfg_etld,
    get_scraped_text_file_exist_last_modified_on,
    get_scraped_text_object_tags_by_mfg_etld1,
    download_scraped_text_from_s3_by_mfg_etld1,
    upload_scraped_text_to_s3,
)

logger = logging.getLogger(__name__)


class ScrapedTextFile(BaseModel):
    # 1) Instances are immutable after creation
    model_config = ConfigDict(frozen=True, extra="forbid")

    s3_version_id: str
    last_modified_on: datetime
    etld1: str
    num_tokens: int
    text: str = Field(repr=False, exclude=True)
    urls_scraped: int
    urls_failed: int
    success_rate: float
    is_valid: bool

    @computed_field  # included in dumps; safe for logs
    @property
    def text_preview(self) -> str:
        # Truncate to 100 words; tweak as needed
        return " ".join(self.text.split()[:100]) + (
            "..." if len(self.text.split()) > 100 else ""
        )

    def __repr__(self) -> str:
        # Ensure repr is safe
        return (
            f"ScrapedTextFile(etld1={self.etld1!r}, s3_version_id={self.s3_version_id!r}, "
            f"num_tokens={self.num_tokens}, urls_scraped={self.urls_scraped}, "
            f"urls_failed={self.urls_failed}, success_rate={self.success_rate}, "
            f"is_valid={self.is_valid}, text_preview={self.text_preview!r})"
        )

    # 3) Factory that computes and sets all values at creation time
    # specially needed because pydantic validators/computed fields can't await
    @classmethod
    async def download_from_s3_and_create(
        cls, mfg_etld1: str, s3_version_id: str
    ) -> tuple[ScrapedTextFile, None] | tuple[None, Exception]:
        try:
            scraped_text, _version_id = (
                await download_scraped_text_from_s3_by_mfg_etld1(
                    mfg_etld1, s3_version_id
                )
            )
            last_modified_on = await get_scraped_text_file_exist_last_modified_on(
                get_file_name_from_mfg_etld(mfg_etld1), s3_version_id
            )
            assert (
                last_modified_on is not None
            ), "Last modified date should not be None if file exists."
            num_tokens = num_tokens_from_string(scraped_text)
            tags = await get_scraped_text_object_tags_by_mfg_etld1(
                mfg_etld1, s3_version_id
            )

            urls_scraped = int(tags.get("urls_scraped", 0)) if tags else 0
            urls_failed = int(tags.get("urls_failed", 0)) if tags else 0
            success_rate = ScrapingResult.get_success_rate(urls_scraped, urls_failed)

            is_valid = ScrapingResult.is_scrape_valid(
                scraped_text, urls_scraped, urls_failed
            )

            return (
                cls(
                    etld1=mfg_etld1,
                    s3_version_id=s3_version_id,
                    num_tokens=num_tokens,
                    text=scraped_text,
                    urls_scraped=urls_scraped,
                    urls_failed=urls_failed,
                    success_rate=success_rate,
                    is_valid=is_valid,
                    last_modified_on=last_modified_on,
                ),
                None,
            )
        except Exception as e:
            logger.error(
                f"Error creating ScrapedTextFile for {mfg_etld1} with version ID {s3_version_id}: {e}"
            )
            return None, e

    @classmethod
    async def can_delete_version(
        cls, s3_version_id: str
    ) -> bool:  # basically check if there is no ground truth using this file version
        if (
            await does_a_kgt_exist_with_scraped_file_version(s3_version_id)
            or await does_a_cgt_exist_with_scraped_file_version(s3_version_id)
            or await does_a_bgt_exist_with_scraped_file_version(s3_version_id)
        ):
            return False
        return True

    async def delete_permanently_if_possible(self) -> None:
        if not await self.can_delete_version(s3_version_id=self.s3_version_id):
            logger.error(
                f"Cannot delete scraped text file for {self.etld1} with version ID {self.s3_version_id} as it is referenced by existing ground truths."
            )
            return

        await delete_scraped_text_from_s3_by_etld1(self.etld1, self.s3_version_id)

    @classmethod
    async def upload_to_s3_and_create(
        cls, batch: Batch, scrape_result: ScrapingResult, mfg_etld1: str
    ) -> ScrapedTextFile:
        is_valid = ScrapingResult.is_scrape_valid(
            scrape_result.content, scrape_result.urls_scraped, scrape_result.urls_failed
        )

        if not is_valid:
            raise ValueError(
                f"Upload cancelled. Scraping result is not valid for {mfg_etld1}: "
                f"Urls scraped: {scrape_result.urls_scraped}, "
                f"Urls failed: {scrape_result.urls_failed}, "
                f"success_rate: {scrape_result.success_rate}, "
                f"num_tokens: {scrape_result.num_tokens}."
            )

        version_id, s3_text_file_full_url = await upload_scraped_text_to_s3(
            scrape_result.content,
            get_file_name_from_mfg_etld(mfg_etld1),
            {
                "batch_title": batch.title,
                "batch_timestamp": batch.timestamp.isoformat(),
                "urls_scraped": str(scrape_result.urls_scraped),
                "urls_failed": str(scrape_result.urls_failed),
                "success_rate": f"{scrape_result.success_rate:.2}",
                "num_tokens": str(scrape_result.num_tokens),
            },
        )
        logger.info(f"Uploaded to S3: {s3_text_file_full_url}")
        last_modified_on = await get_scraped_text_file_exist_last_modified_on(
            get_file_name_from_mfg_etld(mfg_etld1), version_id
        )
        assert (
            last_modified_on is not None
        ), "Last modified date should not be None if file exists."

        return cls(
            etld1=mfg_etld1,
            s3_version_id=version_id,
            num_tokens=scrape_result.num_tokens,
            text=scrape_result.content,
            urls_scraped=scrape_result.urls_scraped,
            urls_failed=scrape_result.urls_failed,
            success_rate=scrape_result.success_rate,
            is_valid=is_valid,
            last_modified_on=last_modified_on,
        )
