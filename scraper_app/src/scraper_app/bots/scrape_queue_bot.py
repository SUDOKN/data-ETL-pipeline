from aiobotocore.session import get_session
import argparse
import asyncio
from datetime import datetime
import time
from typing import Optional

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
from shared.utils.aws.s3.s3_client_util import make_s3_client
from shared.utils.aws.s3.scraped_text_util import (
    get_file_name_from_mfg_url,
    does_scraped_text_file_exist,
    upload_scraped_text_to_s3,
)
from shared.utils.time_util import get_current_time

from shared.services.manufacturer_service import update_manufacturer
from scraper_app.services.async_url_scraper_service import AsyncScraperService

# Configuration constants
DEFAULT_SLEEP_AFTER_RETRIES = 12 * 60 * 60  # 12 hours in seconds
RETRY_SLEEP_INTERVAL = 5  # seconds
CONCURRENCY_CHECK_INTERVAL = 1  # second


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


def parse_args():
    parser = argparse.ArgumentParser(description="SQS Scraper Bot")
    parser.add_argument(
        "--max_poll_retries", type=int, default=2, help="Max polling retries for SQS"
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
        "--max_depth", type=int, default=2, help="Max depth for scraping"
    )
    return parser.parse_args()


"""
Special Notes:
If a manufacturer already exists in the database, 
but the scraped_text_file_version_id field is not present, 
or if the scraped_text_file_version_id does not match the existing/non-existing text file in S3,
we will proceed to scrape the URL and upload the scraped text to S3 and mark the manufacturer for redo.

"""


async def process_queue(
    sqs_client,
    s3_client,
    scraper: AsyncScraperService,
    max_poll_retries: int = 2,
    max_concurrent_manufacturers: int = 5,
):
    concurrent_manufacturers = set()
    retries = 0
    scraping_stats = ScrapingStats()  # Initialize timing stats
    await scraper.start()
    try:
        while True:
            # Check if we are at the concurrency threshold
            if len(concurrent_manufacturers) >= max_concurrent_manufacturers:
                await asyncio.sleep(CONCURRENCY_CHECK_INTERVAL)  # lets other tasks run
                continue
            item, receipt_handle = await poll_item_from_scrape_queue(sqs_client)
            if item is None:
                if retries < max_poll_retries:
                    print(
                        f"No messages received. Retrying {retries + 1}/{max_poll_retries}..."
                    )
                    retries += 1
                    await asyncio.sleep(RETRY_SLEEP_INTERVAL)
                    continue
                else:
                    print(
                        f"No messages received after {retries} retries. Sleeping for {DEFAULT_SLEEP_AFTER_RETRIES // 3600} hours."
                    )
                    # Print summary statistics before long sleep
                    stats = scraping_stats.get_stats()
                    if stats["completed_count"] > 0:
                        print(f"📊 Session Summary:")
                        print(
                            f"   ✅ Completed scraping: {stats['completed_count']} manufacturers"
                        )
                        print(f"   ⏱️  Total time: {stats['total_time']:.2f}s")
                        print(
                            f"   📈 Average time per manufacturer: {stats['average_time']:.2f}s"
                        )
                    await asyncio.sleep(DEFAULT_SLEEP_AFTER_RETRIES)
                    retries = 0  # Reset retries after sleeping
                    continue

            if not receipt_handle:
                print("No receipt handle found, skipping this message.")
                continue

            retries = 0  # Reset retries on successful message retrieval

            try:
                # Validate manufacturer before processing
                existing_manufacturer, should_continue = (
                    await validate_manufacturer_for_scraping(
                        item, sqs_client, s3_client
                    )
                )

                if not should_continue:
                    await delete_item_from_scrape_queue(sqs_client, receipt_handle)
                    continue

                current_timestamp = get_current_time()
                task = asyncio.create_task(
                    scrape_and_cleanup(
                        current_timestamp,
                        s3_client,
                        scraper,
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
                print(f"Error processing manufacturer {item.manufacturer_url}: {e}")
                current_timestamp = get_current_time()
                await ScrapingError.insert_one(
                    ScrapingError(
                        created_at=current_timestamp,
                        error=str(e),
                        url=item.manufacturer_url,
                    )
                )
                await delete_item_from_scrape_queue(sqs_client, receipt_handle)

    except Exception as e:
        print(f"Error processing SQS message: {e}")
    finally:
        # Print final statistics
        stats = scraping_stats.get_stats()
        if stats["completed_count"] > 0:
            print(f"\n🏁 Final Session Statistics:")
            print(f"   ✅ Total completed: {stats['completed_count']} manufacturers")
            print(
                f"   ⏱️  Total time: {stats['total_time']:.2f}s ({stats['total_time']/60:.1f} minutes)"
            )
            print(f"   📈 Average time per manufacturer: {stats['average_time']:.2f}s")
        await scraper.stop()


async def validate_manufacturer_for_scraping(
    item: ToScrapeItem, sqs_client, s3_client
) -> tuple[Optional[Manufacturer], bool]:
    """
    Validate manufacturer for scraping.
    Returns (manufacturer, should_continue) where should_continue indicates
    if processing should continue.
    """
    manufacturer = await Manufacturer.find_one({"url": item.manufacturer_url})

    if manufacturer:
        print(f"Found existing manufacturer for {item.manufacturer_url}.")
        if manufacturer.scraped_text_file_version_id:
            existing_txt_file = await does_scraped_text_file_exist(
                s3_client,
                get_file_name_from_mfg_url(item.manufacturer_url),
                manufacturer.scraped_text_file_version_id,
            )
            if existing_txt_file:
                print(
                    f"Found existing scraped text file for {manufacturer.url} with version {manufacturer.scraped_text_file_version_id}. No need to scrape, fast tracking to extraction."
                )
                await push_item_to_extract_queue(
                    sqs_client,
                    ToExtractItem(manufacturer_url=manufacturer.url),
                )
                return manufacturer, False
            else:
                print(
                    f"WARNING: Scraped text file for {manufacturer.url} with version {manufacturer.scraped_text_file_version_id} does not exist in S3. Please check manufacturer records."
                )
                return manufacturer, False
        else:
            # NOTE: this block can (and should) be utilized only when we want the manufacturer extraction to be:
            # 1. done for the first time
            # 2. redone by setting its scraped_text_file_version_id as None beforehand.
            print(
                f"Manufacturer {manufacturer.url} exists but has no scraped_text_file_version_id. Adding batch {item.batch} and proceeding to scrape."
            )
            manufacturer.batches.insert(0, item.batch)

    return manufacturer, True


async def scrape_manufacturer(
    timestamp: datetime,
    s3_client,
    scraper: AsyncScraperService,
    item: ToScrapeItem,
    manufacturer: Optional[Manufacturer],  # if existing
):
    """Scrape manufacturer content and save to database."""
    scraped_text = await scraper.scrape(item.manufacturer_url, item.manufacturer_url)
    print(
        f"Scraped content for {item.manufacturer_url} (truncated):\n{scraped_text[:50]}"
    )

    # Upload to S3
    version_id, s3_text_file_full_url = await upload_scraped_text_to_s3(
        s3_client,
        scraped_text,
        get_file_name_from_mfg_url(item.manufacturer_url),
        {
            "batch_title": item.batch.title,
            "batch_timestamp": item.batch.timestamp.isoformat(),
        },  # tags for S3 object
    )
    print(f"Uploaded to S3: {s3_text_file_full_url}")

    if manufacturer:
        manufacturer.scraped_text_file_version_id = version_id
    else:
        manufacturer = Manufacturer(
            created_at=timestamp,
            url=item.manufacturer_url,
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

    # Absolutely necessary to ensure the model is validated and saved correctly
    # especially if the model has been modified or new fields have been added.
    await update_manufacturer(timestamp, manufacturer)
    print(f"Saved manufacturer: {manufacturer.url}")


async def scrape_and_cleanup(
    timestamp: datetime,
    s3_client,
    scraper: AsyncScraperService,
    item: ToScrapeItem,
    manufacturer: Optional[Manufacturer],
    sqs_client,
    receipt_handle: str,
    concurrent_manufacturers: set,
    scraping_stats: ScrapingStats,
):
    """Scrape manufacturer and handle cleanup tasks."""
    start_time = time.time()
    try:
        await scrape_manufacturer(timestamp, s3_client, scraper, item, manufacturer)

        # Push to extract queue on success
        await push_item_to_extract_queue(
            sqs_client,
            ToExtractItem(manufacturer_url=item.manufacturer_url),
        )

        # Calculate and log timing
        end_time = time.time()
        duration = end_time - start_time
        scraping_stats.add_timing(duration)

        stats = scraping_stats.get_stats()
        print(f"✅ Scraping completed for {item.manufacturer_url}")
        print(f"   ⏱️  Individual time: {duration:.2f}s")
        print(
            f"   📊 Average time: {stats['average_time']:.2f}s (from {stats['completed_count']} completed)"
        )

    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        print(
            f"❌ Error scraping manufacturer {item.manufacturer_url} after {duration:.2f}s: {e}"
        )
        await ScrapingError.insert_one(
            ScrapingError(
                created_at=timestamp,
                error=str(e),
                url=item.manufacturer_url,
            )
        )
    finally:
        # Always clean up
        concurrent_manufacturers.discard(asyncio.current_task())
        await delete_item_from_scrape_queue(sqs_client, receipt_handle)


async def async_main():
    args = parse_args()
    session = get_session()
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
            args.max_poll_retries,
            args.max_concurrent_manufacturers,
        )


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
