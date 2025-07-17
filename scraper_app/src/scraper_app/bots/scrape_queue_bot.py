import asyncio
from datetime import datetime
import argparse
from aiobotocore.session import get_session
import logging

from typing import Callable, Awaitable, Any, Optional

from shared.constants import REDO_EXTRACTION_KEYWORD
from shared.models.db.scraping_error import ScrapingError
from shared.models.db.manufacturer import Manufacturer
from shared.models.to_extract_item import ToExtractItem
from shared.models.to_scrape_item import ToScrapeItem
from shared.utils.aws.queue.sqs_scraper_client_util import make_sqs_scraper_client
from shared.utils.aws.queue.extract_queue_util import push_item_to_extract_queue
from shared.utils.aws.queue.scrape_queue_util import (
    poll_item_from_scrape_queue,
    delete_item_from_scrape_queue,
)
from shared.utils.aws.queue.priority_extract_queue_util import (
    push_item_to_priority_extract_queue,
)
from shared.utils.aws.queue.priority_scrape_queue_util import (
    poll_item_from_priority_scrape_queue,
    delete_item_from_priority_scrape_queue,
)
from shared.utils.aws.s3.s3_client_util import make_s3_client
from shared.utils.aws.s3.scraped_text_util import (
    get_file_name_from_mfg_url,
    does_scraped_text_file_exist,
    upload_scraped_text_to_s3,
)
from shared.utils.mongo_client import init_db
from shared.utils.time_util import get_current_time

from shared.services.manufacturer_service import (
    reset_llm_aided_fields,
    find_manufacturer_by_url,
    update_manufacturer,
)

from scraper_app.services.async_url_scraper_service import (
    AsyncScraperService,
    ScrapingResult,
)
from open_ai_key_app.utils.ask_gpt_util import num_tokens_from_string

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
        "--max_concurrency", type=int, default=5, help="Max concurrency for scraping"
    )
    parser.add_argument(
        "--max_concurrent_manufacturers",
        type=int,
        default=5,
        help="Queue would not be polled if there are more than this many manufacturers are being scraped concurrently.",
    )
    parser.add_argument(
        "--max_depth", type=int, default=5, help="Max depth for scraping"
    )
    return parser.parse_args()


"""
Special Notes:
If a manufacturer already exists in the database, 
but the scraped_text_file_version_id field is not present, 
or if the scraped_text_file_version_id does not match the existing/non-existing text file in S3,
we will proceed to scrape the URL and upload the scraped text to S3 and mark the manufacturer for redo.

Timestamp Consistency:
All errors for a single polled item share the same creation timestamp (current_timestamp) 
to maintain consistency and enable better error tracking and debugging.
"""


async def process_queue(
    sqs_client,
    s3_client,
    scraper: AsyncScraperService,
    push_item_to_e_queue: Callable[[Any, ToExtractItem], Awaitable[None]],
    poll_item_from_s_queue: Callable[
        [Any], Awaitable[tuple[ToScrapeItem, str] | tuple[None, None]]
    ],
    delete_item_from_s_queue: Callable[[Any, str], Awaitable[None]],
    max_concurrent_manufacturers: int = 5,
):
    concurrent_manufacturers = set()
    scraping_stats = ScrapingStats()  # Initialize timing stats
    await scraper.start()
    try:
        while True:
            # Check if we are at the concurrency threshold
            if len(concurrent_manufacturers) >= max_concurrent_manufacturers:
                await asyncio.sleep(CONCURRENCY_CHECK_INTERVAL)
                """
                    The sleeping coroutine only resumes when:
                    - The minimum sleep time has elapsed AND
                    - The currently running task yields control back to the event loop
                    - So if you have a long-running task that takes 10 seconds to complete, 
                    and it starts running during your 1-second sleep, your sleep will 
                    effectively last ~10 seconds total.
                """
                continue

            item, receipt_handle = await poll_item_from_s_queue(
                sqs_client
            )  # 10 second long poll, doesn't block, yields control back to event loop

            if item is None:
                continue
            elif not receipt_handle:
                logger.error("No receipt handle found, skipping this message.")
                continue

            # Create single timestamp for this polled item - all errors will use this timestamp
            current_timestamp = get_current_time()

            logger.info(
                f"Processing item: {item.manufacturer_url} (Batch: {item.batch.title})"
            )
            try:
                # Validate manufacturer before processing
                existing_manufacturer, should_continue = (
                    await validate_manufacturer_for_scraping(
                        current_timestamp,
                        item,
                        sqs_client,
                        s3_client,
                        push_item_to_e_queue,
                    )
                )

                if not should_continue:
                    await delete_item_from_s_queue(sqs_client, receipt_handle)
                    continue

                task = asyncio.create_task(
                    scrape_and_cleanup(
                        current_timestamp,
                        s3_client,
                        scraper,
                        push_item_to_e_queue,
                        delete_item_from_s_queue,
                        item,
                        existing_manufacturer,
                        sqs_client,
                        receipt_handle,
                        concurrent_manufacturers,
                        scraping_stats,
                    )
                )
                concurrent_manufacturers.add(task)

            except Exception as e:
                logger.error(
                    f"Error processing manufacturer {item.manufacturer_url}: {e}"
                )
                await ScrapingError.insert_one(
                    ScrapingError(
                        created_at=current_timestamp,
                        error=str(e),
                        url=item.manufacturer_url,
                        batch=item.batch,
                    )
                )
                await delete_item_from_s_queue(sqs_client, receipt_handle)

    except Exception as e:
        logger.error(f"Error processing SQS message: {e}")
    finally:
        # Print final statistics
        if scraping_stats.completed_count > 0:
            logger.info(f"\n🏁 Final Session Statistics:")
            scraping_stats.print_stats()
        await scraper.stop()


async def validate_manufacturer_for_scraping(
    timestamp: datetime,
    item: ToScrapeItem,
    sqs_client,
    s3_client,
    push_item_to_e_queue: Callable[[Any, ToExtractItem], Awaitable[None]],
) -> tuple[Optional[Manufacturer], bool]:
    """
    Validate manufacturer for scraping.
    Returns (manufacturer, should_continue) where should_continue indicates
    if processing should continue.
    """
    logger.info(f"Validating manufacturer for scraping: {item.manufacturer_url}")
    manufacturer = await find_manufacturer_by_url(
        item.manufacturer_url
    )  # Fetch existing manufacturer by URL

    if manufacturer:
        logger.info(f"Found existing manufacturer for {item.manufacturer_url}.")
        if (
            not manufacturer.scraped_text_file_version_id
            or manufacturer.scraped_text_file_version_id == REDO_EXTRACTION_KEYWORD
        ):
            # NOTE: this block can (and should) be utilized only when we want the manufacturer extraction to be:
            # 1. done for the first time
            # 2. redone by setting its scraped_text_file_version_id as None beforehand.
            logger.info(
                f"Manufacturer {manufacturer.url} exists but has no scraped_text_file_version_id. Adding batch {item.batch} and proceeding to scrape."
            )
            reset_llm_aided_fields(manufacturer)
            manufacturer.batches.append(item.batch)
            await update_manufacturer(timestamp, manufacturer)
        elif manufacturer.scraped_text_file_version_id:
            existing_txt_file = await does_scraped_text_file_exist(
                s3_client,
                get_file_name_from_mfg_url(item.manufacturer_url),
                manufacturer.scraped_text_file_version_id,
            )
            if existing_txt_file:
                logger.info(
                    f"Found existing scraped text file for {manufacturer.url} with version {manufacturer.scraped_text_file_version_id}. Re-extraction request noted. No need to scrape, fast tracking to extraction."
                )

                await push_item_to_e_queue(
                    sqs_client,
                    ToExtractItem(manufacturer_url=manufacturer.url),
                )
                return manufacturer, False
            else:
                logger.warning(
                    f"Scraped text file for {manufacturer.url} with version {manufacturer.scraped_text_file_version_id} does not exist in S3. Please check manufacturer records."
                )
                return manufacturer, False

    return manufacturer, True


async def scrape_and_save_manufacturer(
    timestamp: datetime,
    s3_client,
    scraper: AsyncScraperService,
    item: ToScrapeItem,
    manufacturer: Optional[Manufacturer],  # if existing
) -> ScrapingResult:
    """Scrape manufacturer content and save to database."""
    logger.info(f"🔄 Starting scraping for {item.manufacturer_url}")

    # Use the updated scraper that returns ScrapingResult
    scraping_result = await scraper.scrape(item.manufacturer_url, item.manufacturer_url)

    # Save individual URL errors to database (using consistent timestamp)
    if scraping_result.has_errors:
        logger.warning(
            f"⚠️  Saving {len(scraping_result.errors)} individual URL errors to database"
        )
        for error_info in scraping_result.errors:
            await ScrapingError.insert_one(
                ScrapingError(
                    created_at=timestamp,  # Use consistent timestamp from main loop
                    error=f"URL: {error_info['url']} (depth {error_info['depth']}) - {error_info['error_type']}: {error_info['error']}",
                    url=item.manufacturer_url,  # Main manufacturer URL for grouping
                    batch=item.batch,
                )
            )

    # Log scraping statistics
    logger.info(f"📊 Scraping stats for {item.manufacturer_url}:")
    scraping_result.print_stats()

    # Validate scraped content
    num_tokens = num_tokens_from_string(scraping_result.content)
    logger.info(f"Number of tokens in scraped text: {num_tokens}")

    if not num_tokens or num_tokens < 30:
        raise ValueError(
            f"Scraped text for {item.manufacturer_url} is empty or too short ({num_tokens} tokens). Success rate: {scraping_result.success_rate:.1%}"
        )

    # Upload to S3
    version_id, s3_text_file_full_url = await upload_scraped_text_to_s3(
        s3_client,
        scraping_result.content,
        get_file_name_from_mfg_url(item.manufacturer_url),
        {
            "batch_title": item.batch.title,
            "batch_timestamp": item.batch.timestamp.isoformat(),
            "urls_scraped": str(scraping_result.urls_scraped),
            "urls_failed": str(scraping_result.urls_failed),
            "success_rate": f"{scraping_result.success_rate:.2}",
        },  # tags for S3 object
    )
    logger.info(f"Uploaded to S3: {s3_text_file_full_url}")

    if manufacturer:
        manufacturer.scraped_text_file_num_tokens = num_tokens
        manufacturer.scraped_text_file_version_id = version_id
    else:
        manufacturer = Manufacturer(
            created_at=timestamp,
            url=item.manufacturer_url,
            scraped_text_file_num_tokens=num_tokens,
            scraped_text_file_version_id=version_id,
            batches=[item.batch],
            # Following fields will be set later during extraction
            name=None,
            is_manufacturer=None,
            is_contract_manufacturer=None,
            is_product_manufacturer=None,
            founded_in=None,
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

    await update_manufacturer(timestamp, manufacturer)
    logger.info(f"Saved manufacturer: {manufacturer.url}")

    return scraping_result


async def scrape_and_cleanup(
    timestamp: datetime,
    s3_client,
    scraper: AsyncScraperService,
    push_item_to_e_queue: Callable[[Any, ToExtractItem], Awaitable[None]],
    delete_item_from_s_queue: Callable[[Any, str], Awaitable[None]],
    item: ToScrapeItem,
    manufacturer: Optional[Manufacturer],
    sqs_client,
    receipt_handle: str,
    concurrent_manufacturers: set,
    scraping_stats: ScrapingStats,
):
    """Scrape manufacturer and handle cleanup tasks with comprehensive error handling."""
    start_time = get_current_time()
    scraping_result = None

    try:
        # Perform scraping and get result with errors
        scraping_result = await scrape_and_save_manufacturer(
            timestamp, s3_client, scraper, item, manufacturer
        )

        # Only push to extract queue if scraping was mostly successful
        if scraping_result.success_rate > 0.8:  # At least 80% success rate
            await push_item_to_e_queue(
                sqs_client,
                ToExtractItem(manufacturer_url=item.manufacturer_url),
            )
        else:
            logger.warning(
                f"⚠️  Skipping extract queue due to low success rate: {scraping_result.success_rate:.1%}"
            )
            await ScrapingError.insert_one(
                ScrapingError(
                    created_at=timestamp,  # Use consistent timestamp from main loop
                    error=f"Low success rate ({scraping_result.success_rate:.1%}) for {item.manufacturer_url}. Not added to extract queue.",
                    url=item.manufacturer_url,
                    batch=item.batch,
                )
            )

        # Calculate and log timing
        end_time = get_current_time()
        duration = end_time - start_time
        scraping_stats.add_timing(duration.total_seconds())

        logger.info(f"✅ Scraping completed for {item.manufacturer_url}")
        logger.info(f"   ⏱️  Individual time: {duration:.2f}s")
        scraping_stats.print_stats()

    except Exception as e:
        end_time = get_current_time()
        duration = end_time - start_time
        logger.error(
            f"❌ Error scraping manufacturer {item.manufacturer_url} after {duration:.2f}s: {e}"
        )

        # Save the main scraping error
        await ScrapingError.insert_one(
            ScrapingError(
                created_at=timestamp,  # Use consistent timestamp from main loop
                error=str(e),
                url=item.manufacturer_url,
                batch=item.batch,
            )
        )

        # If we have partial results, still save individual URL errors
        if scraping_result and scraping_result.has_errors:
            logger.warning(
                f"⚠️  Also saving {len(scraping_result.errors)} individual URL errors from failed scraping"
            )
            for error_info in scraping_result.errors:
                await ScrapingError.insert_one(
                    ScrapingError(
                        created_at=timestamp,  # Use consistent timestamp
                        error=f"URL: {error_info['url']} (depth {error_info['depth']}) - {error_info['error_type']}: {error_info['error']}",
                        url=item.manufacturer_url,
                        batch=item.batch,  # Include batch for context
                    )
                )

    finally:
        # Always clean up
        concurrent_manufacturers.discard(asyncio.current_task())
        await delete_item_from_s_queue(sqs_client, receipt_handle)


async def async_main():
    await init_db()
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

    session = get_session()

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

    async with make_sqs_scraper_client(session) as sqs_scraper_client, make_s3_client(
        session
    ) as s3_client:
        scraper = AsyncScraperService(
            max_concurrency=args.max_concurrency,
            max_depth=args.max_depth,
        )
        await process_queue(
            sqs_scraper_client,
            s3_client,
            scraper,
            push_item_to_e_queue,
            poll_item_from_s_queue,
            delete_item_from_s_queue,
            args.max_concurrent_manufacturers,
        )


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
