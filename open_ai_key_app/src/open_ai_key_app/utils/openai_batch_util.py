import logging
from openai import OpenAI
from openai.types import Batch


logger = logging.getLogger(__name__)


def fetch_all_batches(client: OpenAI, limit: int = 100) -> list[Batch]:
    """
    Fetch all batches by iterating through the cursor-based pagination.

    Args:
        client: OpenAI client instance
        limit: Number of batches per page (max 100)

    Returns:
        List of all batch objects
    """
    all_batches = []

    # The SDK's list() returns a cursor that handles pagination
    for batch in client.batches.list(limit=limit):
        all_batches.append(batch)

    logger.info(f"Fetched {len(all_batches)} batches total")
    return all_batches
