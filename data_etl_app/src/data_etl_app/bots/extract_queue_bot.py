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
from shared.utils.aws.s3.s3_client_util import make_s3_client
from shared.utils.time_util import get_current_time

from shared.services.manufacturer_service import (
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

INVALID_ITEM_TAG = "INVALID_ITEM"
TOO_SHORT_THRESHOLD = 50  # Minimum number of tokens for scraped text
TOO_LONG_THRESHOLD = 120000  # Maximum number of tokens for scraped text


def parse_args():
    parser = argparse.ArgumentParser(description="SQS Extractor Bot")
    parser.add_argument(
        "--priority",
        type=int,
        default=0,
        help="Is this for priority queue? 0 for no, 1 for yes",
    )
    parser.add_argument(
        "--max_poll_retries", type=int, default=2, help="Max polling retries for SQS"
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
    max_poll_retries: int = 2,
    max_concurrent_manufacturers: int = 25,
):
    concurrent_manufacturers = set()
    try:
        while True:
            # Check if we are at the concurrency threshold
            if len(concurrent_manufacturers) >= max_concurrent_manufacturers:
                await asyncio.sleep(1)  # lets other tasks run
                continue

            item, receipt_handle = await poll_item_from_queue(sqs_client)
            if item is None:
                if retries < max_poll_retries:
                    print(
                        f"No messages received. Retrying {retries + 1}/{max_poll_retries}..."
                    )
                    retries += 1
                    await asyncio.sleep(5)
                    continue
                else:
                    print(
                        f"No messages received after {retries} retries. Sleeping for 12 hours."
                    )
                    await asyncio.sleep(12 * 60 * 60)  # Sleep for 12 hours
                    retries = 0  # Reset retries after sleeping
                    continue

            if not receipt_handle:
                print("No receipt handle found, skipping this message.")
                continue

            retries = 0  # Reset retries on successful message retrieval
            current_timestamp = get_current_time()
            manufacturer = await Manufacturer.find_one({"url": item.manufacturer_url})

            invalid_item = False
            if not manufacturer:
                print(
                    f"{INVALID_ITEM_TAG}: Manufacturer {item.manufacturer_url} does not exist. Skipping extraction."
                )
                await ExtractionError.insert_one(
                    ExtractionError(
                        created_at=current_timestamp,
                        error=f"Manufacturer {item.manufacturer_url} does not exist.",
                        field="manufacturer",
                        url=item.manufacturer_url,
                    )
                )
                invalid_item = True
            elif not manufacturer.scraped_text_file_version_id:
                print(
                    f"{INVALID_ITEM_TAG}: Manufacturer {item.manufacturer_url} has no scraped_text_file_version_id, probably needs scraping. Skipping extraction."
                )
                await ExtractionError.insert_one(
                    ExtractionError(
                        created_at=current_timestamp,
                        error=f"Manufacturer {item.manufacturer_url} has no scraped_text_file_version_id.",
                        field="scraped_text_file_version_id",
                        url=item.manufacturer_url,
                    )
                )
                invalid_item = True
            elif not (
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
                        created_at=current_timestamp,
                        error=f"Scraped text file for {item.manufacturer_url}:{manufacturer.scraped_text_file_version_id} does not exist.",
                        field="scraped_text_file",
                        url=item.manufacturer_url,
                    )
                )
                invalid_item = True

            # Help Pylance know we are now certain manufacturer is not None
            assert manufacturer is not None

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
                        created_at=current_timestamp,
                        error=f"Scraped text for {item.manufacturer_url} is shorter than {TOO_SHORT_THRESHOLD} tokens.",
                        field="scraped_text",
                        url=item.manufacturer_url,
                    )
                )
                invalid_item = True
            elif num_tokens > TOO_LONG_THRESHOLD:
                print(
                    f"{INVALID_ITEM_TAG}: Scraped text for {item.manufacturer_url} is too long ({num_tokens}), skipping extraction."
                )
                await ExtractionError.insert_one(
                    ExtractionError(
                        created_at=current_timestamp,
                        error=f"Scraped text for {item.manufacturer_url} is longer than {TOO_LONG_THRESHOLD} tokens.",
                        field="scraped_text",
                        url=item.manufacturer_url,
                    )
                )
                invalid_item = True
            elif version_id != manufacturer.scraped_text_file_version_id:
                print(
                    f"{INVALID_ITEM_TAG}: Scraped text version ID mismatch for {item.manufacturer_url}. Expected: {manufacturer.scraped_text_file_version_id}, got: {version_id}. Skipping extraction."
                )
                await ExtractionError.insert_one(
                    ExtractionError(
                        created_at=current_timestamp,
                        error=f"Scraped text version ID mismatch for {item.manufacturer_url}. Expected: {manufacturer.scraped_text_file_version_id}, got: {version_id}.",
                        field="scraped_text_file_version_id",
                        url=item.manufacturer_url,
                    )
                )
                invalid_item = True

            if invalid_item:
                await delete_item_from_queue(sqs_client, receipt_handle)
            else:
                # Instead of awaiting process_manufacturer directly, create a task
                task = asyncio.create_task(
                    process_manufacturer(current_timestamp, mfg_txt, manufacturer)
                )
                concurrent_manufacturers.add(task)

                def make_on_task_done(receipt_handle, sqs_client):
                    async def on_task_done_async():
                        concurrent_manufacturers.discard(task)
                        await delete_item_from_queue(sqs_client, receipt_handle)

                    def on_task_done(task):
                        asyncio.create_task(on_task_done_async())

                    return on_task_done

                task.add_done_callback(make_on_task_done(receipt_handle, sqs_client))

    except Exception as e:
        print(f"Error processing SQS message: {e}")


async def process_manufacturer(
    timestamp: datetime,
    mfg_txt: str,
    manufacturer: Manufacturer,
):

    try:
        print(f"Processing manufacturer: {manufacturer.url}")
        manufacturer.is_manufacturer = await is_company_a_manufacturer(
            timestamp,
            manufacturer.url,
            mfg_txt,
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

    # TODO: add is_product_manufacturer, is_contract_manufacturer check

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
        return

    if not manufacturer.industries or manufacturer.industries.results is None:
        try:
            manufacturer.industries = await extract_industries(
                timestamp, manufacturer.url, mfg_txt
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

    if not manufacturer.certificates or manufacturer.certificates.results is None:
        try:
            manufacturer.certificates = await extract_certificates(
                timestamp,
                manufacturer.url,
                mfg_txt,
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

    if not manufacturer.material_caps or manufacturer.material_caps.results is None:
        try:
            manufacturer.material_caps = await extract_materials(
                timestamp,
                manufacturer.url,
                mfg_txt,
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

    if not manufacturer.process_caps or manufacturer.process_caps.results is None:
        try:
            manufacturer.process_caps = await extract_processes(
                timestamp,
                manufacturer.url,
                mfg_txt,
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


async def async_main():
    args = parse_args()
    session = get_session()
    if args.priority:
        poll_item_from_queue = poll_item_from_priority_extract_queue
        delete_item_from_queue = delete_item_from_priority_extract_queue
    else:
        poll_item_from_queue = poll_item_from_extract_queue
        delete_item_from_queue = delete_item_from_extract_queue
    async with make_sqs_extractor_client(
        session
    ) as sqs_extractor_client, make_s3_client(session) as s3_client:
        asyncio.run(
            process_queue(
                poll_item_from_queue,
                delete_item_from_queue,
                sqs_extractor_client,
                s3_client,
                args.max_poll_retries,
            )
        )


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
