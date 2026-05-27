import logging
from beanie.operators import In
from typing import Optional

from core.models.db.gpt_batch import GPTBatch
from core.models.db.gpt_batch_request import GPTBatchRequest
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID

logger = logging.getLogger(__name__)


async def find_gpt_batch_request_by_custom_id(
    mfg_etld1: str,
    gpt_batch_request_custom_id: GPTBatchRequestCustomID,
) -> Optional[GPTBatchRequest]:
    gpt_batch_request = await GPTBatchRequest.find_one(
        GPTBatchRequest.etld1 == mfg_etld1,
        GPTBatchRequest.request.custom_id == gpt_batch_request_custom_id,
    )

    return gpt_batch_request


async def find_completed_gpt_batch_request_by_custom_id(
    mfg_etld1: str,
    gpt_batch_request_custom_id: GPTBatchRequestCustomID,
) -> Optional[GPTBatchRequest]:
    if gpt_batch_request_custom_id is None:
        raise ValueError("gpt_batch_request_custom_id cannot be None")

    gpt_batch_request = await GPTBatchRequest.find_one(
        GPTBatchRequest.etld1 == mfg_etld1,
        GPTBatchRequest.request.custom_id == gpt_batch_request_custom_id,
        GPTBatchRequest.batch_id != None,
        GPTBatchRequest.response != None,
    )

    return gpt_batch_request


async def find_gpt_batch_requests_by_custom_ids(
    mfg_etld1: str,
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID],
) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest] = dict()
    async for gpt_req in GPTBatchRequest.find(
        GPTBatchRequest.etld1 == mfg_etld1,
        In(GPTBatchRequest.request.custom_id, gpt_batch_request_custom_ids),
    ):
        request_map[gpt_req.request.custom_id] = gpt_req

    return request_map


async def find_gpt_batch_request_ids_only(
    mfg_etld1: str,
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID],
) -> set[GPTBatchRequestCustomID]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    gpt_req_ids_found = set()
    async for gpt_req in GPTBatchRequest.find(
        GPTBatchRequest.etld1 == mfg_etld1,
        In(GPTBatchRequest.request.custom_id, gpt_batch_request_custom_ids),
    ):
        gpt_req_ids_found.add(gpt_req.request.custom_id)

    return gpt_req_ids_found


async def find_completed_gpt_batch_requests_by_custom_ids(
    mfg_etld1: str,
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID],
) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest] = dict()
    async for gpt_req in GPTBatchRequest.find(
        GPTBatchRequest.etld1 == mfg_etld1,
        In(GPTBatchRequest.request.custom_id, gpt_batch_request_custom_ids),
        GPTBatchRequest.batch_id != None,
        GPTBatchRequest.response != None,
    ):
        request_map[gpt_req.request.custom_id] = gpt_req

    return request_map


async def find_completed_gpt_batch_request_ids_only(
    mfg_etld1: str,
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID | None],
) -> set[GPTBatchRequestCustomID]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    gpt_req_ids_found = set()
    async for gpt_req in GPTBatchRequest.find(
        GPTBatchRequest.etld1 == mfg_etld1,
        In(GPTBatchRequest.request.custom_id, gpt_batch_request_custom_ids),
        GPTBatchRequest.batch_id != None,
        GPTBatchRequest.response != None,
    ):
        gpt_req_ids_found.add(gpt_req.request.custom_id)

    return gpt_req_ids_found


async def find_incomplete_gpt_batch_requests_by_custom_ids(
    mfg_etld1: str,
    gpt_batch_request_custom_ids: list[GPTBatchRequestCustomID],
) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
    if gpt_batch_request_custom_ids is None:
        raise ValueError("gpt_batch_request_custom_ids cannot be None")
    elif len(gpt_batch_request_custom_ids) == 0:
        raise ValueError("gpt_batch_request_custom_ids cannot be empty")
    elif any(cid is None for cid in gpt_batch_request_custom_ids):
        raise ValueError("gpt_batch_request_custom_ids cannot contain None values")

    request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest] = dict()
    async for gpt_req in GPTBatchRequest.find(
        GPTBatchRequest.etld1 == mfg_etld1,
        In(GPTBatchRequest.request.custom_id, gpt_batch_request_custom_ids),
        # "batch_id": {"$in": [None, "Eager"]},
        GPTBatchRequest.response == None,
    ):
        request_map[gpt_req.request.custom_id] = gpt_req

    return request_map


async def get_custom_ids_for_batch(
    gpt_batch: GPTBatch,
) -> set[GPTBatchRequestCustomID]:
    logger.info(f"Getting custom IDs for GPT batch {gpt_batch.external_batch_id}")

    custom_ids: set[GPTBatchRequestCustomID] = set()
    async for gpt_req in GPTBatchRequest.find(
        GPTBatchRequest.batch_id == gpt_batch.external_batch_id,
    ):
        custom_ids.add(gpt_req.request.custom_id)

    logger.info(
        f"Found {len(custom_ids):,} batch requests for batch {gpt_batch.external_batch_id}"
    )
    return custom_ids
