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
    all_batches: list[Batch] = []
    after: str | None = None

    while True:
        if after is None:
            page = client.batches.list(limit=limit)
        else:
            page = client.batches.list(limit=limit, after=after)

        batches = page.data or []
        all_batches.extend(batches)

        if not page.has_more or not batches:
            break

        after = batches[-1].id

    logger.info(f"Fetched {len(all_batches)} batches total")
    return all_batches
