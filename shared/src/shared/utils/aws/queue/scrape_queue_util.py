import os
import json
import logging

from shared.models.to_scrape_item import ToScrapeItem
from shared.constants import LONG_POLL_INTERVAL

SCRAPE_QUEUE_URL = os.getenv("SCRAPE_QUEUE_URL")
logger = logging.getLogger(__name__)


if not SCRAPE_QUEUE_URL:
    raise ValueError(
        "AWS SCRAPE_QUEUE_URL is not set. Please set it in your .env file."
    )


async def push_item_to_scrape_queue(sqs_client, item: ToScrapeItem):
    """
    Sends an item to the SQS queue for scraping.

    :param sqs_client: The SQS client to use for sending messages.
    :param item: The item to send to the SQS queue.
    """
    await sqs_client.send_message(
        QueueUrl=SCRAPE_QUEUE_URL,
        MessageBody=item.model_dump_json(),
    )
    logger.info(
        f"Sent ToScrapeItem for {item.manufacturer_url} to scrape queue: {SCRAPE_QUEUE_URL}"
    )


async def poll_item_from_scrape_queue(
    sqs_client,
) -> tuple[ToScrapeItem, str] | tuple[None, None]:
    """
    Receives a single item from the SQS queue.

    :param sqs_client: The SQS client to use for receiving messages.
    """
    logger.info(f"Polling Scrape queue: {SCRAPE_QUEUE_URL}")
    response = await sqs_client.receive_message(
        QueueUrl=SCRAPE_QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=LONG_POLL_INTERVAL,
    )
    messages = response.get("Messages", [])
    if not messages:
        return None, None
    body = messages[0]["Body"]  # Return the body of the first message
    logger.info(f"Received message body: {body.strip()}")
    receipt_handle = messages[0]["ReceiptHandle"]

    try:
        item_dict = json.loads(body.strip())
        item = ToScrapeItem(**item_dict)
    except Exception as e:
        logger.error(
            f"Error decoding ToScrapeItem JSON from message body: {e}, deleting message from Scrape queue."
        )
        # Optionally delete the message if it's malformed
        await sqs_client.delete_message(
            QueueUrl=SCRAPE_QUEUE_URL,
            ReceiptHandle=receipt_handle,
        )
        return None, None

    return item, receipt_handle


async def delete_item_from_scrape_queue(sqs_client, receipt_handle: str):
    """
    Deletes an item from the SQS queue.

    :param sqs_client: The SQS client to use for deleting messages.
    :param receipt_handle: The receipt handle of the message to delete.
    """
    await sqs_client.delete_message(
        QueueUrl=SCRAPE_QUEUE_URL,
        ReceiptHandle=receipt_handle,
    )
    logger.info(f"Deleted item from Scrape queue with receipt handle: {receipt_handle}")
