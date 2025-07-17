import os
import json
import logging

from shared.models.to_scrape_item import ToScrapeItem
from shared.constants import LONG_POLL_INTERVAL

logger = logging.getLogger(__name__)
PRIORITY_SCRAPE_QUEUE_URL = os.getenv("PRIORITY_SCRAPE_QUEUE_URL")

if not PRIORITY_SCRAPE_QUEUE_URL:
    raise ValueError(
        "AWS PRIORITY_SCRAPE_QUEUE_URL is not set. Please set it in your .env file."
    )


async def push_item_to_priority_scrape_queue(priority_sqs_client, item: ToScrapeItem):
    """
    Sends an item to the Priority Scrape queue for scraping.

    :param priority_sqs_client: The SQS client to use for sending messages.
    :param item: The item to send to the SQS queue.
    """
    await priority_sqs_client.send_message(
        QueueUrl=PRIORITY_SCRAPE_QUEUE_URL,
        MessageBody=item.model_dump_json(),
    )
    logger.info(
        f"Sent ToScrapeItem for {item.manufacturer_url} to priority scrape queue: {PRIORITY_SCRAPE_QUEUE_URL}"
    )


async def poll_item_from_priority_scrape_queue(priority_sqs_client):
    """
    Receives a single item from the Priority Scrape queue.

    :param priority_sqs_client: The SQS client to use for receiving messages.
    """
    logger.info(f"Polling Priority Scrape queue: {PRIORITY_SCRAPE_QUEUE_URL}")
    response = await priority_sqs_client.receive_message(
        QueueUrl=PRIORITY_SCRAPE_QUEUE_URL,
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
        logger.error(f"Error decoding ToScrapeItem JSON from message body: {e}")
        # Optionally delete the message if it's malformed
        await priority_sqs_client.delete_message(
            QueueUrl=PRIORITY_SCRAPE_QUEUE_URL,
            ReceiptHandle=receipt_handle,
        )
        return None, None

    return item, receipt_handle


async def delete_item_from_priority_scrape_queue(priority_sqs_client, receipt_handle):
    """
    Deletes an item from the Priority Scrape queue.

    :param priority_sqs_client: The SQS client to use for deleting messages.
    :param receipt_handle: The receipt handle of the message to delete.
    """
    await priority_sqs_client.delete_message(
        QueueUrl=PRIORITY_SCRAPE_QUEUE_URL,
        ReceiptHandle=receipt_handle,
    )
    logger.info(
        f"Deleted item from Priority Scrape queue with receipt handle: {receipt_handle}"
    )
