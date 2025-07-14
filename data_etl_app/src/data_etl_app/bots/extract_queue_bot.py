from aiobotocore.session import get_session
import argparse
import asyncio
from datetime import datetime
from typing import Callable, Awaitable, Any

from shared.models.db.manufacturer import Manufacturer
from shared.models.db.extraction_error import ExtractionError
from shared.models.to_extract_item import ToExtractItem

from shared.utils.aws.queue.sqs_extractor_client_util import make_sqs_extractor_client
from shared.utils.aws.queue.extract_queue_util import (
    poll_item_from_extract_queue,
    delete_item_from_extract_queue,
)
from shared.utils.aws.queue.priority_extract_queue_util import (
    poll_item_from_priority_extract_queue,
    delete_item_from_priority_extract_queue,
)
from shared.utils.aws.s3.scraped_text_util import (
    get_file_name_from_mfg_url,
    does_scraped_text_file_exist,
    download_scraped_text_from_s3_by_filename,
)
from shared.utils.mongo_client import init_db
from shared.utils.aws.s3.s3_client_util import make_s3_client
from shared.utils.time_util import get_current_time

from shared.services.manufacturer_service import (
    find_manufacturer_by_url,
    update_manufacturer,
    is_company_a_manufacturer,
)

from open_ai_key_app.utils.ask_gpt_util import num_tokens_from_string
from data_etl_app.services.extract_concept_service import (
    extract_industries,
    extract_certificates,
    extract_materials,
    extract_processes,
)
from data_etl_app.services.binary_classifier_service import (
    is_product_manufacturer,
    is_contract_manufacturer,
)

"""
Extract Queue Bot - Data ETL Pipeline

This bot processes items from the extraction queue to extract manufacturing data
from scraped text using various AI services.

"""

INVALID_ITEM_TAG = "INVALID_ITEM"
TOO_SHORT_THRESHOLD = 50  # Minimum number of tokens for scraped text
TOO_LONG_THRESHOLD = 125000  # Maximum number of tokens for scraped text

# Configuration constants
DEFAULT_SLEEP_AFTER_RETRIES = 12 * 60 * 60  # 12 hours in seconds
RETRY_SLEEP_INTERVAL = 5  # seconds
CONCURRENCY_CHECK_INTERVAL = 0.1  # second


class ExtractionStats:
    """Track extraction timing statistics."""

    def __init__(self):
        self.total_time = 0.0
        self.completed_count = 0
        self.average_time = 0.0

    def add_timing(self, duration: float):
        """Add a new timing measurement and update average."""
        self.total_time += duration
        self.completed_count += 1
        self.average_time = self.total_time / self.completed_count

    def print_stats(self):
        """Print the current statistics."""
        print(f"   ✅ Total completed: {self.completed_count} manufacturers")
        print(
            f"   ⏱️  Total time: {self.total_time:.2f}s ({self.total_time/60:.1f} minutes)"
        )
        print(f"   📈 Average time per manufacturer: {self.average_time:.2f}s")


def parse_args():
    parser = argparse.ArgumentParser(description="SQS Extractor Bot")
    parser.add_argument(
        "--priority",
        type=int,
        default=0,
        help="Is this for priority queue? 0 for no, 1 for yes",
    )
    parser.add_argument(
        "--debug",
        default=0,
        help="Enable debug mode: 0 for no, 1 for yes",
    )
    parser.add_argument(
        "--max_concurrent_manufacturers",
        type=int,
        default=25,
        help="Queue would not be polled if there are more than this many manufacturers are being processed concurrently.",
    )
    return parser.parse_args()


# TODO: fix Any
async def process_queue(
    poll_item_from_queue: Callable[
        [Any], Awaitable[tuple[ToExtractItem, str] | tuple[None, None]]
    ],
    delete_item_from_queue: Callable[[Any, str], Awaitable[None]],
    sqs_client,
    s3_client,
    max_concurrent_manufacturers: int = 25,
    debug: bool = False,
):

    concurrent_manufacturers = set()
    extraction_stats = ExtractionStats()  # Initialize timing stats
    try:
        while True:
            # Check if we are at the concurrency threshold
            if len(concurrent_manufacturers) >= max_concurrent_manufacturers:
                await asyncio.sleep(CONCURRENCY_CHECK_INTERVAL)  # lets other tasks run
                continue

            item, receipt_handle = await poll_item_from_queue(
                sqs_client
            )  # 10 second long poll, doesn't block, yields control back to event loop

            if item is None:
                continue
            elif not receipt_handle:
                print("No receipt handle found, skipping this message.")
                continue

            current_timestamp = get_current_time()

            # Validate manufacturer before processing
            manufacturer, mfg_txt, _txt_version_id, should_continue = (
                await validate_manufacturer_for_extraction(
                    current_timestamp, item, s3_client
                )
            )

            if not should_continue:
                # Delete invalid item from queue and continue
                await delete_item_from_queue(sqs_client, receipt_handle)
                continue

            # Help Pylance know we are now certain manufacturer is not None
            assert manufacturer is not None
            assert mfg_txt is not None

            task = asyncio.create_task(
                extract_and_cleanup(
                    current_timestamp,
                    mfg_txt,
                    manufacturer,
                    sqs_client,
                    receipt_handle,
                    delete_item_from_queue,
                    concurrent_manufacturers,
                    extraction_stats,
                    debug=debug,
                )
            )
            concurrent_manufacturers.add(task)

    except Exception as e:
        print(f"Error processing Extract Queue message: {e}")
    finally:
        # Print final statistics
        if extraction_stats.completed_count > 0:
            print(f"\n🏁 Final Session Statistics:")
            extraction_stats.print_stats()


async def validate_manufacturer_for_extraction(
    timestamp: datetime,
    item: ToExtractItem,
    s3_client,
) -> tuple[Manufacturer, str, str, bool] | tuple[None, None, None, bool]:
    """
    Validate manufacturer for extraction.
    Returns (manufacturer, mfg_txt, version_id, should_continue) where should_continue
    indicates if processing should continue.
    Note: This function does NOT delete from queue - cleanup is handled by caller.
    """

    manufacturer = await find_manufacturer_by_url(item.manufacturer_url)

    if not manufacturer:
        print(
            f"{INVALID_ITEM_TAG}: Manufacturer {item.manufacturer_url} does not exist. Skipping extraction."
        )
        await ExtractionError.insert_one(
            ExtractionError(
                created_at=timestamp,
                error=f"Manufacturer {item.manufacturer_url} does not exist.",
                field="manufacturer",
                url=item.manufacturer_url,
            )
        )
        return None, None, None, False

    if not manufacturer.scraped_text_file_version_id:
        print(
            f"{INVALID_ITEM_TAG}: Manufacturer {item.manufacturer_url} has no scraped_text_file_version_id, probably needs scraping. Skipping extraction."
        )
        await ExtractionError.insert_one(
            ExtractionError(
                created_at=timestamp,
                error=f"Manufacturer {item.manufacturer_url} has no scraped_text_file_version_id.",
                field="scraped_text_file_version_id",
                url=item.manufacturer_url,
            )
        )
        return None, None, None, False

    if not (
        await does_scraped_text_file_exist(
            s3_client,
            get_file_name_from_mfg_url(manufacturer.url),
            manufacturer.scraped_text_file_version_id,
        )
    ):
        print(
            f"{INVALID_ITEM_TAG}: Scraped text file for {item.manufacturer_url} does not exist, probably needs rescraping. Skipping extraction."
        )
        await ExtractionError.insert_one(
            ExtractionError(
                created_at=timestamp,
                error=f"Scraped text file for {item.manufacturer_url}:{manufacturer.scraped_text_file_version_id} does not exist.",
                field="scraped_text_file",
                url=item.manufacturer_url,
            )
        )
        return None, None, None, False

    # Download and validate scraped text
    mfg_txt, version_id = await download_scraped_text_from_s3_by_filename(
        s3_client,
        file_name=get_file_name_from_mfg_url(manufacturer.url),
    )

    num_tokens = num_tokens_from_string(mfg_txt)
    if num_tokens < TOO_SHORT_THRESHOLD:
        print(
            f"{INVALID_ITEM_TAG}: Scraped text for {item.manufacturer_url} is too short ({num_tokens}), probably needs rescraping. Skipping extraction."
        )
        await ExtractionError.insert_one(
            ExtractionError(
                created_at=timestamp,
                error=f"Scraped text for {item.manufacturer_url} is shorter than {TOO_SHORT_THRESHOLD} tokens.",
                field="scraped_text",
                url=item.manufacturer_url,
            )
        )
        return None, None, None, False

    if num_tokens > TOO_LONG_THRESHOLD:
        print(
            f"{INVALID_ITEM_TAG}: Scraped text for {item.manufacturer_url} is too long ({num_tokens}), skipping extraction."
        )
        await ExtractionError.insert_one(
            ExtractionError(
                created_at=timestamp,
                error=f"Scraped text for {item.manufacturer_url} is longer than {TOO_LONG_THRESHOLD} tokens.",
                field="scraped_text",
                url=item.manufacturer_url,
            )
        )
        return None, None, None, False

    if version_id != manufacturer.scraped_text_file_version_id:
        print(
            f"{INVALID_ITEM_TAG}: Scraped text version ID mismatch for {item.manufacturer_url}. Expected: {manufacturer.scraped_text_file_version_id}, got: {version_id}. Skipping extraction."
        )
        await ExtractionError.insert_one(
            ExtractionError(
                created_at=timestamp,
                error=f"Scraped text version ID mismatch for {item.manufacturer_url}. Expected: {manufacturer.scraped_text_file_version_id}, got: {version_id}.",
                field="scraped_text_file_version_id",
                url=item.manufacturer_url,
            )
        )
        return None, None, None, False

    return manufacturer, mfg_txt, version_id, True


async def process_manufacturer(
    timestamp: datetime,
    mfg_txt: str,
    manufacturer: Manufacturer,
    debug: bool = False,
):

    if debug:
        print(f"Debug mode enabled. Processing manufacturer: {manufacturer.url}")

    if not manufacturer.is_manufacturer:
        try:
            print(f"Finding out if company is a manufacturer: {manufacturer.url}")
            manufacturer.is_manufacturer = await is_company_a_manufacturer(
                timestamp,
                manufacturer.url,
                mfg_txt,
                debug=debug,
            )

            if (
                manufacturer.is_manufacturer
                and manufacturer.is_manufacturer.answer is False
            ):
                await update_manufacturer(
                    updated_at=timestamp,
                    manufacturer=manufacturer,
                )
                return  # if not a manufacturer, skip further processing
        except Exception as e:
            print(f"{manufacturer.url}.is_manufacturer errored:{e}")
            await ExtractionError.insert_one(
                ExtractionError(
                    created_at=timestamp,
                    error=str(e),
                    field="is_manufacturer",
                    url=manufacturer.url,
                )
            )
            return  # if is_manufacturer check fails, skip further processing

    if not manufacturer.is_product_manufacturer:
        try:
            print(
                f"Finding out if company is a product manufacturer: {manufacturer.url}"
            )
            manufacturer.is_product_manufacturer = await is_product_manufacturer(
                timestamp, manufacturer.url, mfg_txt, debug=debug
            )
            await update_manufacturer(
                updated_at=timestamp,
                manufacturer=manufacturer,
            )
        except Exception as e:
            print(f"{manufacturer.url}.is_product_manufacturer errored:{e}")
            await ExtractionError.insert_one(
                ExtractionError(
                    created_at=timestamp,
                    error=str(e),
                    field="is_product_manufacturer",
                    url=manufacturer.url,
                )
            )

    if not manufacturer.is_contract_manufacturer:
        try:
            print(
                f"Finding out if company is a contract manufacturer: {manufacturer.url}"
            )
            manufacturer.is_contract_manufacturer = await is_contract_manufacturer(
                timestamp, manufacturer.url, mfg_txt, debug=debug
            )
            await update_manufacturer(
                updated_at=timestamp,
                manufacturer=manufacturer,
            )
        except Exception as e:
            print(f"{manufacturer.url}.is_contract_manufacturer errored:{e}")
            await ExtractionError.insert_one(
                ExtractionError(
                    created_at=timestamp,
                    error=str(e),
                    field="is_contract_manufacturer",
                    url=manufacturer.url,
                )
            )
            return

    if debug:
        print(f"industries if present: {manufacturer.industries}")

    if not manufacturer.industries or manufacturer.industries.results is None:
        try:
            print(f"Extracting industries for {manufacturer.url}")
            manufacturer.industries = await extract_industries(
                timestamp, manufacturer.url, mfg_txt, debug=debug
            )
            await update_manufacturer(
                updated_at=timestamp,
                manufacturer=manufacturer,
            )

        except Exception as e:
            print(f"{manufacturer.name}.industries errored:{e}")
            await ExtractionError.insert_one(
                ExtractionError(
                    created_at=timestamp,
                    error=str(e),
                    field="industries",
                    url=manufacturer.url,
                )
            )

    if debug:
        print(f"certificates if present: {manufacturer.certificates}")

    if not manufacturer.certificates or manufacturer.certificates.results is None:
        try:
            print(f"Extracting certificates for {manufacturer.url}")
            manufacturer.certificates = await extract_certificates(
                timestamp, manufacturer.url, mfg_txt, debug=debug
            )
            await update_manufacturer(
                updated_at=timestamp,
                manufacturer=manufacturer,
            )

        except Exception as e:
            print(f"{manufacturer.name}.certificates errored:{e}")
            await ExtractionError.insert_one(
                ExtractionError(
                    created_at=timestamp,
                    error=str(e),
                    field="certificates",
                    url=manufacturer.url,
                )
            )

    if debug:
        print(f"material_caps if present: {manufacturer.material_caps}")

    if not manufacturer.material_caps or manufacturer.material_caps.results is None:
        try:
            print(f"Extracting materials for {manufacturer.url}")
            manufacturer.material_caps = await extract_materials(
                timestamp, manufacturer.url, mfg_txt, debug=debug
            )
            await update_manufacturer(
                updated_at=timestamp,
                manufacturer=manufacturer,
            )

        except Exception as e:
            print(f"{manufacturer.name}.material_caps errored:{e}")
            await ExtractionError.insert_one(
                ExtractionError(
                    created_at=timestamp,
                    error=str(e),
                    field="material_caps",
                    url=manufacturer.url,
                )
            )

    if debug:
        print(f"process_caps if present: {manufacturer.process_caps}")

    if not manufacturer.process_caps or manufacturer.process_caps.results is None:
        try:
            print(f"Extracting processes for {manufacturer.url}")
            manufacturer.process_caps = await extract_processes(
                timestamp, manufacturer.url, mfg_txt, debug=debug
            )
            await update_manufacturer(
                updated_at=timestamp,
                manufacturer=manufacturer,
            )

        except Exception as e:
            print(f"{manufacturer.name}.process_caps errored:{e}")
            await ExtractionError.insert_one(
                ExtractionError(
                    created_at=timestamp,
                    error=str(e),
                    field="process_caps",
                    url=manufacturer.url,
                )
            )


async def extract_and_cleanup(
    timestamp: datetime,
    mfg_txt: str,
    manufacturer: Manufacturer,
    sqs_client,
    receipt_handle: str,
    delete_item_from_queue,
    concurrent_manufacturers: set,
    extraction_stats: ExtractionStats,
    debug: bool = False,
):
    """Extract manufacturer data and handle cleanup tasks."""
    start_time = get_current_time()
    try:
        await process_manufacturer(timestamp, mfg_txt, manufacturer, debug)

        # Calculate and log timing
        end_time = get_current_time()
        duration = end_time - start_time
        extraction_stats.add_timing(duration.total_seconds())

        print(f"✅ Extraction completed for {manufacturer.url}")
        print(f"   ⏱️  Individual time: {duration.total_seconds():.2f}s")
        extraction_stats.print_stats()

    except Exception as e:
        end_time = get_current_time()
        duration = end_time - start_time
        print(
            f"❌ Error processing manufacturer {manufacturer.url} after {duration.total_seconds():.2f}s: {e}"
        )
        await ExtractionError.insert_one(
            ExtractionError(
                created_at=timestamp,
                error=str(e),
                field="general_processing",
                url=manufacturer.url,
            )
        )
    finally:
        # Always clean up
        concurrent_manufacturers.discard(asyncio.current_task())
        await delete_item_from_queue(sqs_client, receipt_handle)


async def async_main():
    await init_db()
    args = parse_args()
    session = get_session()

    if args.priority:
        print("Running extraction in priority mode")
        poll_item_from_queue = poll_item_from_priority_extract_queue
        delete_item_from_queue = delete_item_from_priority_extract_queue
    else:
        print("Running extraction in normal mode")
        poll_item_from_queue = poll_item_from_extract_queue
        delete_item_from_queue = delete_item_from_extract_queue

    async with make_sqs_extractor_client(
        session
    ) as sqs_extractor_client, make_s3_client(session) as s3_client:
        await process_queue(
            poll_item_from_queue,
            delete_item_from_queue,
            sqs_extractor_client,
            s3_client,
            args.max_concurrent_manufacturers,
            debug=args.debug,
        )


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
