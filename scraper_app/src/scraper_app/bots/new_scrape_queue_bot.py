import asyncio
from datetime import datetime
import argparse

import logging

from typing import Callable, Awaitable

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()


from core.dependencies.aws_clients import (
    cleanup_core_aws_clients,
    initialize_core_aws_clients,
)
from data_etl_app.dependencies.aws_clients import (
    cleanup_data_etl_aws_clients,
    initialize_data_etl_aws_clients,
)

from core.models.db.scraping_error import ScrapingError
from core.models.db.manufacturer import Manufacturer
from core.models.to_extract_item import ToExtractItem
from core.models.to_scrape_item import ToScrapeItem

from core.utils.aws.s3.scraped_text_util import (
    delete_scraped_text_from_s3_by_etld1,
    get_latest_version_id_by_mfg_etld,
)
from core.utils.mongo_client import init_db
from core.utils.time_util import get_current_time

from core.services.manufacturer_service import (
    reset_llm_extracted_fields,
    update_manufacturer,
)

from scraper_app.models.scraped_text_file import ScrapedTextFile
from scraper_app.services.url_scraper_service import (
    ScraperService,
)

from core.utils.url_util import get_etld1_from_host

logger = logging.getLogger(__name__)

# Configuration constants
DEFAULT_SLEEP_AFTER_RETRIES = 12 * 60 * 60  # 12 hours in seconds
RETRY_SLEEP_INTERVAL = 5  # seconds
CONCURRENCY_CHECK_INTERVAL = 0.1  # second


class ScrapingStats:
    """Track scraping timing statistics."""

    def __init__(self):
        self.total_time = 0.0
        self.completed_count = 0
        self.average_time = 0.0

    def add_timing(self, duration: float):
        """Add a new timing measurement and update average."""
        self.total_time += duration
        self.completed_count += 1
        self.average_time = self.total_time / self.completed_count

    def get_stats(self) -> dict:
        """Get current statistics."""
        return {
            "completed_count": self.completed_count,
            "total_time": self.total_time,
            "average_time": self.average_time,
        }

    def print_stats(self):
        """Print the current statistics."""
        logger.info(f"   ✅ Completed scraping: {self.completed_count} manufacturers")
        logger.info(f"   ⏱️  Total time: {self.total_time/60:.1f} minutes")
        logger.info(f"   📈 Average time per manufacturer: {self.average_time:.2f}s")


"""
ScrapedTextFile:
    - automatically downloads the passed version
    - throws error if file does not exist


1. check if manufacturer exists
        Yes? Try creating ScrapedTextFile(existing_manufacturer.scraped_file_version), success?
            Yes? check if file is valid
                Yes? check if redo flag is passed
                    Yes? delete this valid file, set manufacturer.scraped_file_version = None
                    No? fast track to extraction
                No? delete this invalid file, set manufacturer.scraped_file_version = None
            No? file doesn't exist, set manufacturer.scraped_file_version = None
        No? proceed


Now, either, manufacturer is None or manufacturer.scraped_file_version is None, both of which mean scraping must be done
set valid_file: ScrapedTextFile = None
2. check for some existing_scraped_file_version
        Yes? create ScrapedTextFile(existing_scraped_file_version), but is valid?
            Yes? set valid_file = ScrapedTextFile(existing_scraped_file_version)
            No: delete from s3, set valid_file = None (This will clean any old invalid files)
        No: proceed


3. check if valid_file is None
        Yes? Go scrape, check if result is valid
            Yes? upload, valid_file = ScrapedTextFile(new_scraped_file_version)
            No: throw error, let finally block catch this and delete from queue
        No: proceed

Here, valid_file must be set
4. manufacturer exists?
        Yes? manufacturer.scraped_file_version = valid_file.version, upate
        No: Create and save
"""


async def process_queue(
    scraper: ScraperService,
    push_item_to_e_queue: Callable[[ToExtractItem], Awaitable[None]],
    poll_item_from_s_queue: Callable[
        [], Awaitable[tuple[ToScrapeItem, str] | tuple[None, None]]
    ],
    delete_item_from_s_queue: Callable[[str], Awaitable[None]],
):
    scraping_stats = ScrapingStats()  # Initialize timing stats
    try:
        while True:
            item, receipt_handle = (
                await poll_item_from_s_queue()
            )  # 10 second long poll, doesn't block, yields control back to event loop

            if item is None:
                continue
            elif not receipt_handle:
                logger.error("No receipt handle found, skipping this message.")
                continue

            # Create single timestamp for this polled item - all errors will use this timestamp
            polled_at = get_current_time()
            mfg_etld = get_etld1_from_host(item.accessible_normalized_url)

            logger.info(
                f"Processing item: {item.accessible_normalized_url} (Batch: {item.batch.title})"
            )
            try:
                manufacturer = await Manufacturer.find_one({"etld1": mfg_etld})
                scraped_file = await get_valid_scraped_file(
                    polled_at,
                    item,
                    manufacturer,
                    redo_extraction_flag=item.redo_extraction,
                    scraper=scraper,
                )

                if manufacturer:
                    logger.info(f"existing manufacturer found: {manufacturer.etld1}.")
                    manufacturer.scraped_text_file_num_tokens = scraped_file.num_tokens
                    manufacturer.scraped_text_file_version_id = (
                        scraped_file.s3_version_id
                    )
                else:
                    logger.info(f"Creating new manufacturer for mfg_etld:{mfg_etld}.")
                    manufacturer = Manufacturer(
                        created_at=polled_at,
                        etld1=mfg_etld,
                        url_accessible_at=item.accessible_normalized_url,
                        scraped_text_file_num_tokens=scraped_file.num_tokens,
                        scraped_text_file_version_id=scraped_file.s3_version_id,
                        batches=[item.batch],
                        # Following fields will be set later during extraction
                        name=None,
                        is_manufacturer=None,
                        is_contract_manufacturer=None,
                        is_product_manufacturer=None,
                        founded_in=None,
                        email_addresses=None,
                        num_employees=None,
                        business_desc=None,
                        business_statuses=None,
                        primary_naics=None,
                        secondary_naics=None,
                        addresses=None,
                        products=None,
                        certificates=None,
                        industries=None,
                        process_caps=None,
                        material_caps=None,
                    )
                    logger.info(
                        f"Done creating new manufacturer for mfg_etld:{manufacturer.etld1}."
                    )

                await update_manufacturer(polled_at, manufacturer)
                # await push_item_to_e_queue(
                #     ToExtractItem.from_to_scrape_item(item),
                # )
                logger.info(f"Saved manufacturer: {manufacturer.etld1}")
                # Calculate and log timing
                if scraped_file.last_modified_on > polled_at:
                    # Only calculate stats if the file was modified after polling
                    end_time = get_current_time()
                    duration = end_time - polled_at
                    scraping_stats.add_timing(duration.total_seconds())
                    logger.info(
                        f"   ⏱️  Individual time: {duration.total_seconds():.2f}s"
                    )
                    scraping_stats.print_stats()
                else:
                    logger.info(
                        f"Skipped stats calculation: scraped_file.last_modified_on ({scraped_file.last_modified_on}) "
                        f"is not newer than polled_at ({polled_at})"
                    )
            except Exception as e:
                logger.error(
                    f"Error processing manufacturer {item.accessible_normalized_url}: {e}"
                )
                await ScrapingError.insert_one(
                    ScrapingError(
                        created_at=polled_at,
                        error=str(e),
                        url=item.accessible_normalized_url,
                        batch=item.batch,
                    )
                )
            finally:
                await delete_item_from_s_queue(receipt_handle)
    except Exception as e:
        logger.error(f"Error processing SQS message: {e}")
    finally:
        # Print final statistics
        if scraping_stats.completed_count > 0:
            logger.info(f"\n🏁 Final Session Statistics:")
            scraping_stats.print_stats()


async def get_valid_scraped_file(
    polled_at: datetime,
    item: ToScrapeItem,
    manufacturer: Manufacturer | None,
    redo_extraction_flag: bool,
    scraper: ScraperService,
) -> ScrapedTextFile:
    mfg_etld = get_etld1_from_host(item.accessible_normalized_url)
    existing_scraped_file: ScrapedTextFile | None = None
    if manufacturer:
        logger.info(
            f"Manufacturer found for {mfg_etld}. Checking if linked scraped file exists."
        )
        existing_scraped_file, exception = (
            await ScrapedTextFile.download_from_s3_and_create(
                mfg_etld,
                manufacturer.scraped_text_file_version_id,
            )
        )
        if exception:
            subject = f"Error downloading existing scraped file for {mfg_etld}, version {manufacturer.scraped_text_file_version_id}."
            await ScrapingError.insert_one(
                ScrapingError(
                    created_at=polled_at,
                    error=(
                        f"{subject} details={str(exception)}" if exception else subject
                    ),
                    url=item.accessible_normalized_url,
                    batch=item.batch,
                )
            )
        if existing_scraped_file:
            logger.info(
                f"Existing scraped file found for {mfg_etld} with version ID {manufacturer.scraped_text_file_version_id}."
            )
            if existing_scraped_file.is_valid:
                if redo_extraction_flag:
                    reset_llm_extracted_fields(manufacturer)
                    logger.info(
                        f"Redo extraction flag is set. Reset llm extracted fields for {mfg_etld}."
                    )
            else:  # invalid file found
                logger.info(
                    f"Existing scraped file for {mfg_etld} is invalid. Deleting and setting existing_scraped_file = None."
                )
                # await existing_scraped_file.delete_permanently_if_possible()
                existing_scraped_file = None
        else:
            logger.info(f"No valid existing scraped file for {mfg_etld}.")
    else:
        logger.info(f"No existing manufacturer found for {mfg_etld}.")

    logger.info(
        f"after existing mfg check, existing_scraped_file = {existing_scraped_file.model_dump() if existing_scraped_file else None}."
    )

    # Find any latest version on S3 if a linked version wasn't found above
    if not existing_scraped_file:  # try to find any version on S3
        logger.info(
            f"No existing scraped file found for {mfg_etld}. Checking S3 for any version."
        )
        latest_version_id = await get_latest_version_id_by_mfg_etld(
            mfg_etld,
        )
        if latest_version_id:
            logger.info(
                f"Found existing scraped file version {latest_version_id} for {mfg_etld} on S3. Attempting to download and create."
            )
            existing_scraped_file, exception = (
                await ScrapedTextFile.download_from_s3_and_create(
                    mfg_etld,
                    latest_version_id,
                )
            )
            if exception:
                subject = f"Error downloading existing scraped file for {mfg_etld}, version {latest_version_id}."
                await ScrapingError.insert_one(
                    ScrapingError(
                        created_at=polled_at,
                        error=(
                            f"{subject} details={str(exception)}"
                            if exception
                            else subject
                        ),
                        url=item.accessible_normalized_url,
                        batch=item.batch,
                    )
                )
            if (
                not existing_scraped_file
            ):  # something went wrong, delete this version from S3
                logger.info(
                    f"Failed to download and create scraped file for {mfg_etld} with version ID {latest_version_id}. Deleting this version from S3."
                    f" Setting existing_scraped_file = None."
                )
                (
                    await delete_scraped_text_from_s3_by_etld1(
                        mfg_etld, latest_version_id
                    )
                    if await ScrapedTextFile.can_delete_version(latest_version_id)
                    else None
                )
                existing_scraped_file = None
            elif not existing_scraped_file.is_valid:
                logger.info(
                    f"Downloaded scraped file for {mfg_etld} with version ID {latest_version_id} is invalid. Deleting this version from S3."
                    f" Setting existing_scraped_file = None.\n"
                    f"{existing_scraped_file}"
                )
                # await existing_scraped_file.delete_permanently_if_possible()
                existing_scraped_file = None

    logger.info(f"after s3 check, existing_scraped_file = {existing_scraped_file}.")

    if not existing_scraped_file:
        logger.info(f"No valid scraped file found for {mfg_etld}. Starting new scrape.")
        # now we must scrape and upload a new file
        scraping_result = scraper.scrape(item.accessible_normalized_url)
        # Save individual URL errors to database (using consistent timestamp)
        if scraping_result.has_errors:
            logger.warning(
                f"⚠️  Saving {len(scraping_result.errors)} individual URL errors to database"
            )
            for error_info in scraping_result.errors:
                await ScrapingError.insert_one(
                    ScrapingError(
                        created_at=polled_at,  # Use consistent timestamp from main loop
                        error=f"URL: {error_info['url']} (depth {error_info['depth']}) - {error_info['error_type']}: {error_info['error']}",
                        url=item.accessible_normalized_url,  # Main manufacturer URL for grouping
                        batch=item.batch,
                    )
                )
        # Log scraping statistics
        logger.info(f"📊 Scraping stats for {item.accessible_normalized_url}:")
        scraping_result.print_stats()

        existing_scraped_file = await ScrapedTextFile.upload_to_s3_and_create(  # throws error if not valid or scraping_result.timed_out
            item.batch, scraping_result, mfg_etld
        )
        logger.info(
            f"Uploaded new scraped text file for {item.accessible_normalized_url} to S3."
        )

    return existing_scraped_file


def parse_args():
    parser = argparse.ArgumentParser(description="SQS Scraper Bot")
    parser.add_argument(
        "--priority",
        type=int,
        default=0,
        help="Is this for priority queue? 0 for no, 1 for yes",
    )
    parser.add_argument(
        "--debug",
        type=str,
        default="INFO",
        help="Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL",
    )
    parser.add_argument(
        "--max_concurrent_browsers",
        type=int,
        default=5,
        help="Max concurrent browser tabs that can be active at once",
    )
    parser.add_argument(
        "--max_depth", type=int, default=5, help="Max depth for scraping"
    )
    parser.add_argument(
        "--scrape_timeout",
        type=int,
        default=60,
        help="Max timeout for scraping in minutes",
    )
    return parser.parse_args()


async def async_main():
    from core.utils.aws.queue.extract_queue_util import push_item_to_extract_queue
    from core.utils.aws.queue.priority_extract_queue_util import (
        push_item_to_priority_extract_queue,
    )
    from core.utils.aws.queue.scrape_queue_util import (
        poll_item_from_scrape_queue,
        delete_item_from_scrape_queue,
    )
    from core.utils.aws.queue.priority_scrape_queue_util import (
        poll_item_from_priority_scrape_queue,
        delete_item_from_priority_scrape_queue,
    )

    await init_db()

    # Initialize AWS clients
    await initialize_core_aws_clients()
    await initialize_data_etl_aws_clients()

    args = parse_args()

    log_level = args.debug.upper()

    # Validate log level
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if log_level not in valid_levels:
        log_level = "INFO"

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Starting scrape bot with log level: {log_level}")

    if args.priority:
        logger.info("Running scraping in priority mode")
        poll_item_from_s_queue = poll_item_from_priority_scrape_queue
        delete_item_from_s_queue = delete_item_from_priority_scrape_queue
        push_item_to_e_queue = push_item_to_priority_extract_queue
    else:
        logger.info("Running scraping in normal mode")
        poll_item_from_s_queue = poll_item_from_scrape_queue
        delete_item_from_s_queue = delete_item_from_scrape_queue
        push_item_to_e_queue = push_item_to_extract_queue

    try:
        scraper = ScraperService(
            max_concurrent_browsers=args.max_concurrent_browsers,
            max_depth=args.max_depth,
            scrape_timeout=args.scrape_timeout,  # in minutes
        )
        await process_queue(
            scraper,
            push_item_to_e_queue,
            poll_item_from_s_queue,
            delete_item_from_s_queue,
        )
    finally:
        # Clean up AWS clients
        await cleanup_data_etl_aws_clients()
        await cleanup_core_aws_clients()


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
