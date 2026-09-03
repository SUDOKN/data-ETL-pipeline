from __future__ import (
    annotations,
)  # This allows you to write self-referential types without quotes, because type annotations are no longer evaluated at function/class definition time
from abc import ABC, abstractmethod
import json
import litellm
import logging
from datetime import datetime
from typing import Optional, Self
from pydantic import BaseModel, ConfigDict, Field, computed_field

from llm_providers.models.llm_model import LLM_Model
from pure_utils.text_normalize import normalize_scraped_text

from infra.models.queue_items.to_scrape_item import Batch
from infra.utils.aws.s3.scraped_text_file_util import (
    delete_scraped_text_from_s3_by_subject_unique_id,
    get_file_name_from_subject_unique_id,
    get_scraped_text_file_exist_last_modified_on,
    get_scraped_text_object_tags_by_subject_unique_id,
    download_scraped_text_from_s3_by_subject_unique_id,
    upload_scraped_text_to_s3,
)

from scraper.services.url_scraper_service import ScrapingResult

logger = logging.getLogger(__name__)


class ScrapedTextFile(BaseModel, ABC):
    """Abstract, subject-agnostic scraped text file. Concrete subjects (e.g. manufacturers)
    must implement `can_delete_version`; see data_etl_app's ScrapedMfgFile."""

    # 1) Instances are immutable after creation
    model_config = ConfigDict(frozen=True, extra="forbid")

    s3_version_id: str
    last_modified_on: datetime
    subject_unique_id: str
    text: str = Field(repr=False, exclude=True)

    # tags
    num_tokens: int
    urls_scraped: int
    urls_failed: int
    etld1_accessible_at: str  # eTLD+1 of the final landing URL reached during scraping
    # The rendering that produced `text` (2026-08-28; a format-version string
    # from scraper.utils.html_to_markdown, e.g. "markdown_v1"). None on objects
    # uploaded before the tag existed — those are legacy innerText.
    text_format: Optional[str] = None

    # meta
    success_rate: float  # TODO: computed property
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
            f"ScrapedTextFile(subject_unique_id={self.subject_unique_id!r}, s3_version_id={self.s3_version_id!r}, "
            f"num_tokens={self.num_tokens}, urls_scraped={self.urls_scraped}, "
            f"urls_failed={self.urls_failed}, success_rate={self.success_rate}, "
            f"is_valid={self.is_valid}, text_preview={self.text_preview!r})"
        )

    # 3) Factory that computes and sets all values at creation time
    # specially needed because pydantic validators/computed fields can't await
    @classmethod
    async def download_from_s3_and_create(
        cls, subject_unique_id: str, s3_version_id: str, llm_model: LLM_Model
    ) -> Self:
        try:
            scraped_text, _version_id = (
                await download_scraped_text_from_s3_by_subject_unique_id(
                    subject_unique_id, s3_version_id
                )
            )
            # Canonicalize once, before tokenizing/chunking, so extracted phrase keys are stable.
            scraped_text = normalize_scraped_text(scraped_text)
            last_modified_on = await get_scraped_text_file_exist_last_modified_on(
                get_file_name_from_subject_unique_id(subject_unique_id), s3_version_id
            )
            assert (
                last_modified_on is not None
            ), "Last modified date should not be None if file exists."
            num_tokens = litellm.token_counter(model=llm_model.name, text=scraped_text)
            tags = await get_scraped_text_object_tags_by_subject_unique_id(
                subject_unique_id, s3_version_id
            )

            urls_scraped = int(tags.get("urls_scraped", 0)) if tags else 0
            urls_failed = int(tags.get("urls_failed", 0)) if tags else 0
            # Tag is absent on objects uploaded before it was introduced.
            etld1_accessible_at = (
                tags.get("etld1_accessible_at") if tags else None
            ) or subject_unique_id
            # Absent on pre-2026-08-28 objects (legacy innerText renderings).
            text_format = tags.get("text_format") if tags else None
            success_rate = ScrapingResult.get_success_rate(urls_scraped, urls_failed)

            is_valid = ScrapingResult.is_scrape_valid(
                scraped_text, urls_scraped, urls_failed, llm_model
            )

            return cls(
                subject_unique_id=subject_unique_id,
                s3_version_id=s3_version_id,
                num_tokens=num_tokens,
                text=scraped_text,
                urls_scraped=urls_scraped,
                urls_failed=urls_failed,
                etld1_accessible_at=etld1_accessible_at,
                success_rate=success_rate,
                is_valid=is_valid,
                last_modified_on=last_modified_on,
                text_format=text_format,
            )
        except Exception as e:
            logger.error(
                f"Error creating ScrapedTextFile for {subject_unique_id} with version ID {s3_version_id}: {e}"
            )
            raise e

    @classmethod
    @abstractmethod
    async def can_delete_version(cls, s3_version_id: str) -> bool:
        """Whether this version is safe to delete; concrete subjects define the deletion-safety policy."""
        ...

    async def delete_permanently_if_possible(self) -> None:
        if not await self.can_delete_version(s3_version_id=self.s3_version_id):
            logger.error(
                f"Cannot delete scraped text file for {self.subject_unique_id} with version ID {self.s3_version_id} as it is referenced by existing ground truths."
            )
            return

        await delete_scraped_text_from_s3_by_subject_unique_id(
            self.subject_unique_id, self.s3_version_id
        )

    @classmethod
    async def upload_to_s3_and_create(
        cls,
        batch: Batch,
        scrape_result: ScrapingResult,
        subject_unique_id: str,
    ) -> Self:
        is_valid = scrape_result.is_valid()

        if not is_valid:
            raise ValueError(
                f"Upload cancelled. Scraping result is not valid for {subject_unique_id}: "
                f"Urls scraped: {scrape_result.urls_scraped}, "
                f"Urls failed: {scrape_result.urls_failed}, "
                f"Timed out: {scrape_result.timed_out}, "
                f"Urls discovered: {scrape_result.urls_discovered}, "
                f"Total time taken: {scrape_result.total_time_taken}, "
                f"success_rate: {scrape_result.success_rate}, "
                f"num_tokens: {scrape_result.num_tokens}."
            )

        text_file_name = get_file_name_from_subject_unique_id(subject_unique_id)
        version_id, s3_text_file_full_url = await upload_scraped_text_to_s3(
            scrape_result.content,
            text_file_name,
            {
                "batch_title": batch.title,
                "batch_timestamp": batch.timestamp.isoformat(),
                "urls_scraped": str(scrape_result.urls_scraped),
                "urls_failed": str(scrape_result.urls_failed),
                "success_rate": f"{scrape_result.success_rate:.2}",
                "num_tokens": str(scrape_result.num_tokens),
                "etld1_accessible_at": scrape_result.final_landing_etld1,
                # which rendering produced this text (2026-08-28; see
                # scraper.utils.html_to_markdown) — stored texts outlive code
                "text_format": scrape_result.text_format,
            },
        )
        logger.info(f"Uploaded to S3: {s3_text_file_full_url}")

        # The provenance sidecar (2026-08-29; scraper.models.scrape_manifest):
        # `<etld1>.manifest.json` next to the text, its `s3_text_version_id`
        # naming the text version it describes. Best-effort — a manifest
        # failure must never lose a valid scrape.
        if scrape_result.manifest is not None:
            try:
                manifest = dict(scrape_result.manifest)
                manifest["s3_text_version_id"] = version_id
                manifest_version_id, manifest_url = await upload_scraped_text_to_s3(
                    json.dumps(manifest, indent=1, sort_keys=True),
                    f"{subject_unique_id}.manifest.json",
                    {
                        "text_version_id": version_id,
                        "manifest_version": str(manifest.get("manifest_version", "")),
                        "text_format": scrape_result.text_format,
                    },
                )
                logger.info(f"Uploaded manifest sidecar: {manifest_url}")
            except Exception:
                logger.error(
                    f"Manifest sidecar upload failed for {subject_unique_id}; text version {version_id} has no manifest.",
                    exc_info=True,
                )

        last_modified_on = await get_scraped_text_file_exist_last_modified_on(
            text_file_name, version_id
        )
        assert (
            last_modified_on is not None
        ), "Last modified date should not be None if file exists."

        return cls(
            subject_unique_id=subject_unique_id,
            s3_version_id=version_id,
            num_tokens=scrape_result.num_tokens,
            text=scrape_result.content,
            urls_scraped=scrape_result.urls_scraped,
            urls_failed=scrape_result.urls_failed,
            etld1_accessible_at=scrape_result.final_landing_etld1,
            success_rate=scrape_result.success_rate,
            is_valid=is_valid,
            last_modified_on=last_modified_on,
            text_format=scrape_result.text_format,
        )
