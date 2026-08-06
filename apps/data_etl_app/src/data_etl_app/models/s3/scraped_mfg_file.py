from scraper.models.s3.scraped_text_file import ScrapedTextFile
from data_etl_app.utils.s3.scraped_mfg_file_util import (
    is_scraped_mfg_file_version_deletable,
)


class ScrapedMfgFile(ScrapedTextFile):
    """Concrete manufacturer-scoped scraped text file; deletion safety is gated on ground truths."""

    @classmethod
    async def can_delete_version(cls, s3_version_id: str) -> bool:
        return await is_scraped_mfg_file_version_deletable(s3_version_id)
