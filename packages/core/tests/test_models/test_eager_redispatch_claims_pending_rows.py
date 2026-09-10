"""The eager path CLAIMS a parse-nulled row before sending it — through the
real dispatch guard, not a stub.

``record_response_parse_error`` nulls ``batch_id`` so the next pass re-asks a
request; ``dispatch_gpt_batch_request`` refuses a row whose ``batch_id`` is
None, because that is the batch-file path's "upload me next" state. Until
2026-09-05 nothing bridged the two: the loop found the nulled row, dispatch
refused it, three passes shrank nothing, and the subject died — run
20260904T184906, one truncated synthesis answer, retries over in 235 ms
without a network call. The loop tests in test_recursive_node_eager_dispatch
stub ``dispatch_batch_request`` and so stayed green through it; this file
sends the row through the real guard.
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.models.llm_model import GPT_4_1
from llm_providers.models.open_ai.gpt_batch_request_blob import GPTBatchRequestBlob
from llm_providers.models.open_ai.gpt_request_body import GPTRequestBody
from llm_providers.services.gpt_batch_request import gpt_batch_request_service
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)

import core.models.pipeline_nodes.base.base_llm_extraction_node as base_module
from core.models.pipeline_nodes.base.base_llm_recursive_extraction_node import (
    BaseLLMRecursiveExtractionNode,
)
from core.models.pipeline_nodes.base.pipeline_stage import PipelineStage

T0 = datetime(2026, 9, 5, tzinfo=UTC)
CUSTOM_ID = "acme.example>products>llm_phrase_synthesis>chunk>0:100>group>13>gpt-4.1|pv=x"


class _Field:
    name = "products"

    def __hash__(self) -> int:
        return hash(self.name)


class _Node(BaseLLMRecursiveExtractionNode):
    """Only ``dispatch_batch_request`` matters here, and it is the REAL one."""

    stage = PipelineStage.synthesis

    def __init__(self) -> None:
        super().__init__(field_type=cast(Any, _Field()), next_node=cast(Any, None))

    async def embed_request_ids(self, *args, **kwargs) -> None: ...

    def get_embedded_request_ids(self, *args, **kwargs):
        return set()

    def get_request_custom_id(self, *args, **kwargs) -> str:
        return CUSTOM_ID

    async def get_result(self, *args, **kwargs):
        return None

    async def create_batch_requests(self, *args, **kwargs):  # type: ignore[override]
        return []

    async def dispatch_batch_request(self, gpt_batch_request, metadata):  # type: ignore[override]
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request, gpt_model=GPT_4_1
        )


def _parse_nulled_row(batch_id: str | None) -> GPTBatchRequest:
    """A stored row as ``record_response_parse_error`` leaves it (``batch_id``
    None, no response, one error on record) — ``model_construct`` because
    Beanie's ``__init__`` wants a live database."""
    return GPTBatchRequest.model_construct(
        created_at=T0,
        updated_at=T0,
        subject_unique_id="acme.example",
        batch_id=batch_id,
        num_batches_paired_with=0,
        response=None,
        response_parse_errors=[{"timestamp": T0.isoformat(), "error_message": "truncated"}],
        request=GPTBatchRequestBlob(
            custom_id=CUSTOM_ID,
            body=GPTRequestBody(
                model=GPT_4_1.name,
                messages=[
                    {"role": "system", "content": "synthesize"},
                    {"role": "user", "content": "records: ..."},
                ],
                max_completion_tokens=20_000,
                temperature=0.0,
                top_p=1.0,
                presence_penalty=0.0,
                frequency_penalty=0.0,
                seed=12345,
                response_format={"type": "json_object"},
            ),
            input_tokens=11,
        ),
    )


@pytest.fixture
def events(monkeypatch) -> list[tuple[str, Any]]:
    """The claim, the wire call and the recording, in the order they happen."""
    log: list[tuple[str, Any]] = []

    async def _mark(timestamp, custom_ids):
        log.append(("mark", sorted(custom_ids)))
        return len(custom_ids)

    async def _fetch(**kwargs):
        log.append(("fetch", kwargs["context"]))
        return SimpleNamespace(request_custom_id=CUSTOM_ID)

    async def _record(batch_requests, response_blobs, timestamp):
        log.append(("record", [r.request.custom_id for r in batch_requests]))
        return len(batch_requests), 0

    monkeypatch.setattr(base_module, "mark_gpt_batch_requests_eager", _mark)
    monkeypatch.setattr(gpt_batch_request_service, "fetch_gpt_batch_response", _fetch)
    monkeypatch.setattr(base_module, "bulk_record_gpt_batch_responses", _record)
    return log


@pytest.mark.asyncio
async def test_a_parse_nulled_row_is_claimed_then_passes_the_real_guard(events):
    row = _parse_nulled_row(batch_id=None)

    failures = await _Node().dispatch_and_record_eagerly(
        subject_unique_id="acme.example",
        requests_to_dispatch=[row],
        metadata=cast(Any, None),
        timestamp=T0,
    )

    assert failures == []
    assert [kind for kind, _ in events] == ["mark", "fetch", "record"]
    assert events[0] == ("mark", [CUSTOM_ID])  # persisted BEFORE the wire call
    assert row.batch_id == "Eager"


@pytest.mark.asyncio
async def test_a_row_that_is_already_eager_is_not_claimed_again(events):
    row = _parse_nulled_row(batch_id="Eager")

    failures = await _Node().dispatch_and_record_eagerly(
        subject_unique_id="acme.example",
        requests_to_dispatch=[row],
        metadata=cast(Any, None),
        timestamp=T0,
    )

    assert failures == []
    assert [kind for kind, _ in events] == ["fetch", "record"]


@pytest.mark.asyncio
async def test_without_the_claim_the_real_guard_refuses_the_row(events, monkeypatch):
    """The control: the same row, the same guard, no claim step — this is the
    2026-09-04 failure, kept as a test so the guard's meaning stays explicit."""
    with pytest.raises(ValueError, match="if batch_id is not 'Eager'"):
        await dispatch_gpt_batch_request(
            gpt_batch_request=_parse_nulled_row(batch_id=None), gpt_model=GPT_4_1
        )
    assert events == []
