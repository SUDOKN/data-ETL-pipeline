from core.models.db.api_key_bundle import APIKeyBundle
from core.models.db.gpt_batch import GPTBatch
from core.utils.time_util import get_current_time


async def get_all_api_key_bundles() -> list[APIKeyBundle]:
    """
    Find all available API keys.
    """
    return await APIKeyBundle.find({"exhausted": False}).to_list()


async def find_inactive_api_keys() -> list[APIKeyBundle]:
    """
    Find all available API keys.
    """
    return await APIKeyBundle.find({"latest_external_batch_id": None}).to_list()


async def find_all_api_keys_by_gpt_batches(
    gpt_batches: list[GPTBatch],
) -> dict[str, APIKeyBundle]:
    """
    Find available or unavailable API keys for the given purpose.
    """

    return await _find_all_api_keys_by_names(
        [gpt_batch.api_key_label for gpt_batch in gpt_batches]
    )


async def _find_all_api_keys_by_names(names: list[str]) -> dict[str, APIKeyBundle]:
    """
    Find available or unavailable API keys for the given purpose.
    """

    keys = await APIKeyBundle.find({"name": {"$in": names}}).to_list()
    return {k.label: k for k in keys}
