"""The recursive base's eager loop dispatches only requests that have no answer
yet. A node may create pre-answered requests — the mention stage's single dummy
for a window with nothing to locate (built for NO_MODEL, response filled in) —
and until 2026-08-23 those were sent too: run 20260823T034518 lost steelcraft's
seven phrase fields when dispatch_gpt_batch_request's model guard met one."""

from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

import core.models.pipeline_nodes.base.base_llm_recursive_extraction_node as recursive_module
from core.models.pipeline_nodes.base.base_llm_recursive_extraction_node import (
    BaseLLMRecursiveExtractionNode,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.pipeline_stage import PipelineStage

T0 = datetime(2026, 8, 23, 4, 0, 0)


class _Field:
    name = "products"

    def __hash__(self) -> int:
        return hash(self.name)


def _request(custom_id: str, answered: bool) -> Any:
    return SimpleNamespace(
        request=SimpleNamespace(custom_id=custom_id),
        response=SimpleNamespace(result="{}") if answered else None,
    )


class _Node(BaseLLMRecursiveExtractionNode):
    stage = PipelineStage.mention_collection

    def __init__(self) -> None:
        super().__init__(field_type=cast(Any, _Field()), next_node=cast(Any, None))
        self.dispatched: list[str] = []
        self._missing_calls = 0

    async def embed_request_ids(self, *args, **kwargs) -> None: ...

    def get_embedded_request_ids(self, *args, **kwargs):
        return {"dummy", "real"}

    def get_request_custom_id(self, *args, **kwargs) -> str:
        return "id"

    async def get_result(self, *args, **kwargs):
        return None

    async def get_missing_req_ids(self, *args, **kwargs):  # type: ignore[override]
        self._missing_calls += 1
        return {"dummy", "real"} if self._missing_calls == 1 else set()

    async def create_batch_requests(self, *args, **kwargs):  # type: ignore[override]
        return [_request("dummy", answered=True), _request("real", answered=False)]

    async def dispatch_batch_request(self, gpt_batch_request, metadata):  # type: ignore[override]
        self.dispatched.append(gpt_batch_request.request.custom_id)
        return SimpleNamespace(request_custom_id=gpt_batch_request.request.custom_id)

    async def are_all_requests_complete(self, *args, **kwargs):  # type: ignore[override]
        return True

    async def get_completed_request_map(self, *args, **kwargs):  # type: ignore[override]
        return {}


@pytest.mark.asyncio
async def test_pre_answered_requests_are_created_and_recorded_but_never_dispatched(monkeypatch):
    upserted: list[list[str]] = []
    recorded: list[list[str]] = []

    async def fake_upsert(batch_requests, subject_unique_id):
        upserted.append([r.request.custom_id for r in batch_requests])

    async def fake_record(batch_requests, response_blobs, timestamp):
        recorded.append([r.request.custom_id for r in batch_requests])
        assert len(batch_requests) == len(response_blobs)
        return len(batch_requests), 0

    monkeypatch.setattr(
        recursive_module, "bulk_upsert_gpt_batch_requests_with_only_req_bodies", fake_upsert
    )
    monkeypatch.setattr(recursive_module, "bulk_record_gpt_batch_responses", fake_record)

    async def save() -> None: ...

    deferred = SimpleNamespace(
        subject_unique_id="acme.example",
        products=SimpleNamespace(metadata=None, chunked_request_map={"0:10": object()}),
        save=save,
    )
    node = _Node()
    await node.execute(
        subject=cast(Any, SimpleNamespace(subject_unique_id="acme.example")),
        deferred_subject=cast(Any, deferred),
        scraped_text_file=cast(Any, SimpleNamespace(text="")),
        timestamp=T0,
        pipeline_context=PipelineContext(),
        eager=True,
    )
    assert upserted == [["dummy", "real"]]  # the dummy is stored, answered
    assert node.dispatched == ["real"]  # and never sent
    assert recorded == [["real"]]
