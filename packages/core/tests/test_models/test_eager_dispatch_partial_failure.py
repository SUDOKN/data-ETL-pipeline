"""The non-recursive eager path records what came back, then raises.

Its sibling case lives in ``test_recursive_node_eager_dispatch.py``: a recursive
node feeds its failures back into its own convergence loop, which re-asks them.
This node has no loop. If it swallowed the failure it would fall through to
``are_all_requests_complete``, find the field incomplete, log at debug and
return — no next_node, no exception, no dump. So it records first (the answers
that arrived are paid for and must not be discarded), then raises, which is what
puts an ExtractionError row naming the field on the record.
"""

from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

import core.models.pipeline_nodes.base.base_llm_extraction_node as base_module
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
    EagerDispatchFailure,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.base.pipeline_stage import PipelineStage

T0 = datetime(2026, 8, 29, 12, 0, 0)


class _Field:
    name = "products"

    def __hash__(self) -> int:
        return hash(self.name)


def _request(custom_id: str) -> Any:
    return SimpleNamespace(
        request=SimpleNamespace(custom_id=custom_id),
        response=None,
    )


class _Node(BaseLLMExtractionNode):
    stage = PipelineStage.phrase_search

    def __init__(self) -> None:
        super().__init__(field_type=cast(Any, _Field()), next_node=cast(Any, None))
        self.dispatched: list[str] = []
        self.fail_ids: set[str] = set()
        self.next_node_ran = False

    async def embed_request_ids(self, *args, **kwargs) -> None: ...

    def get_embedded_request_ids(self, *args, **kwargs):
        return {"a", "b"}

    def get_request_custom_id(self, *args, **kwargs) -> str:
        return "id"

    async def get_result(self, *args, **kwargs):
        return None

    async def get_missing_req_ids(self, *args, **kwargs):  # type: ignore[override]
        return set()

    async def create_batch_requests(self, *args, **kwargs):  # type: ignore[override]
        return []

    async def dispatch_batch_request(self, gpt_batch_request, metadata):  # type: ignore[override]
        custom_id = gpt_batch_request.request.custom_id
        self.dispatched.append(custom_id)
        if custom_id in self.fail_ids:
            raise RuntimeError(f"Error code: 429 - rate limit reached for {custom_id}")
        return SimpleNamespace(request_custom_id=custom_id)

    async def are_all_requests_complete(self, *args, **kwargs):  # type: ignore[override]
        return True

    async def get_completed_request_map(self, *args, **kwargs):  # type: ignore[override]
        return {}

    async def validate_own_responses(self, *args, **kwargs) -> None:  # type: ignore[override]
        return None


def _install(monkeypatch, recorded: list[list[str]]) -> None:
    async def record(batch_requests, response_blobs, timestamp):
        assert len(batch_requests) == len(response_blobs)
        recorded.append([r.request.custom_id for r in batch_requests])
        return len(batch_requests), 0

    async def find_incomplete(subject_unique_id, custom_ids):
        return {cid: _request(cid) for cid in sorted(custom_ids)}

    monkeypatch.setattr(base_module, "bulk_record_gpt_batch_responses", record)
    monkeypatch.setattr(
        base_module,
        "find_incomplete_gpt_batch_requests_by_custom_ids",
        find_incomplete,
    )


async def _run(node: _Node) -> None:
    async def save() -> None: ...

    deferred = SimpleNamespace(
        subject_unique_id="acme.example",
        products=SimpleNamespace(metadata=None, chunked_request_map={"0:10": object()}),
        save=save,
    )
    await node.execute(
        subject=cast(Any, SimpleNamespace(subject_unique_id="acme.example")),
        deferred_subject=cast(Any, deferred),
        scraped_text_file=cast(Any, SimpleNamespace(text="")),
        timestamp=T0,
        pipeline_context=PipelineContext(),
        eager=True,
    )


@pytest.mark.asyncio
async def test_the_answers_that_arrived_are_recorded_before_the_raise(monkeypatch):
    node = _Node()
    node.fail_ids = {"b"}
    recorded: list[list[str]] = []
    _install(monkeypatch, recorded)

    with pytest.raises(EagerDispatchFailure) as excinfo:
        await _run(node)

    assert recorded == [["a"]], "a was answered and must be written, not discarded"
    assert "b" in str(excinfo.value)
    assert "1 of 2" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, RuntimeError)


@pytest.mark.asyncio
async def test_nothing_is_raised_when_every_dispatch_succeeds(monkeypatch):
    node = _Node()
    recorded: list[list[str]] = []
    _install(monkeypatch, recorded)

    await _run(node)

    assert recorded == [["a", "b"]]
