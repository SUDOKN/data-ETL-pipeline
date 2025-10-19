import os
import json
import logging

from core.models.to_extract_item import ToExtractItem
from core.constants import LONG_POLL_INTERVAL
from core.dependencies.aws_clients import (
    get_extract_queue_client,
    get_scrape_queue_client,
)

logger = logging.getLogger(__name__)
PRIORITY_EXTRACT_QUEUE_URL = os.getenv("PRIORITY_EXTRACT_QUEUE_URL")


if not PRIORITY_EXTRACT_QUEUE_URL:
    raise ValueError(
        "AWS PRIORITY_EXTRACT_QUEUE_URL is not set. Please set it in your .env file."
    )


async def push_item_to_priority_extract_queue(item: ToExtractItem):
    """
    Sends an item to the Extract queue for extraction.

    :param item: The item to send to the SQS queue.
    """
    assert PRIORITY_EXTRACT_QUEUE_URL, "PRIORITY_EXTRACT_QUEUE_URL is not set"
    sqs_client = get_scrape_queue_client()
    await sqs_client.send_message(
        QueueUrl=PRIORITY_EXTRACT_QUEUE_URL,
        MessageBody=item.model_dump_json(),
    )
    logger.info(
        f"Sent ToExtractItem for {item} to priority extract queue: {PRIORITY_EXTRACT_QUEUE_URL}"
    )


async def poll_item_from_priority_extract_queue() -> (
    tuple[ToExtractItem, str] | tuple[None, None]
):
    """
    Receives a single item from the Extract queue.
    """
    assert PRIORITY_EXTRACT_QUEUE_URL, "PRIORITY_EXTRACT_QUEUE_URL is not set"
    sqs_client = get_extract_queue_client()
    logger.info(f"Polling SQS Priority Extract queue: {PRIORITY_EXTRACT_QUEUE_URL}")
    response = await sqs_client.receive_message(
        QueueUrl=PRIORITY_EXTRACT_QUEUE_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=LONG_POLL_INTERVAL,
    )
    messages = response.get("Messages", [])
    if not messages:
        return None, None
    body = messages[0].get("Body")
    if not body:
        logger.error("Message missing Body field")
        return None, None
    logger.info(f"Received message body: {body.strip()}")
    receipt_handle = messages[0].get("ReceiptHandle")
    if not receipt_handle:
        logger.error("Message missing ReceiptHandle field")
        return None, None

    try:
        item_dict = json.loads(body.strip())
        item = ToExtractItem(**item_dict)
    except Exception as e:
        logger.error(f"Error decoding ToExtractItem JSON from message body: {e}")
        # Optionally delete the message if it's malformed
        await sqs_client.delete_message(
            QueueUrl=PRIORITY_EXTRACT_QUEUE_URL,
            ReceiptHandle=receipt_handle,
        )
        return None, None

    return item, receipt_handle


async def delete_item_from_priority_extract_queue(receipt_handle: str) -> None:
    """
    Deletes an item from the Extract queue.

    :param receipt_handle: The receipt handle of the message to delete.
    """
    assert PRIORITY_EXTRACT_QUEUE_URL, "PRIORITY_EXTRACT_QUEUE_URL is not set"
    sqs_client = get_extract_queue_client()
    await sqs_client.delete_message(
        QueueUrl=PRIORITY_EXTRACT_QUEUE_URL,
        ReceiptHandle=receipt_handle,
    )
    logger.info(
        f"Deleted item from priority extract queue with receipt handle: {receipt_handle}"
    )
