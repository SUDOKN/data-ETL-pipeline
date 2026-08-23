"""The mention node's two embed passes (PIPELINE_V3_PLAN.md, user decisions
2026-08-22): pass 1 pools the chunk's forms and hands each sub-window the ones
that occur in it; pass 2, once the group answers are complete, assesses each
window and embeds ONE retry request set for the ids left undescribed."""

from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import LLM_Model
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    BatchedMentionCollectionNodeMetadata,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_mention_collection_node import (
    LLMPhraseMentionCollectionNode,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_mention_collection_node_service import (
    collect_sub_window,
    render_mention_location_context,
    window_text_of,
)

SEP = "#" * 50
SUBJECT = "acme.example"
T0 = datetime(2026, 8, 22, 12, 0, 0)
PAGE_A, PAGE_B = "https://acme.example/materials", "https://acme.example/about"
TEXT = (
    f"{SEP}\n{PAGE_A}\n\n"
    "We stock Aluminum and Brass.\n"  # window 0
    "aluminum alloys ship daily.\n"  # window 1 starts here
    f"{SEP}\n{PAGE_B}\n\n"
    "Lead-free solder only. Aluminum | Brass | Steel\n"
)
SPLIT = TEXT.index("aluminum alloys")
SUB0, SUB1 = f"0:{SPLIT}", f"{SPLIT}:{len(TEXT)}"
CHUNK = f"0:{len(TEXT)}"


@pytest.fixture(autouse=True, scope="module")
def offline_gpt_batch_request_settings():
    """Request creation constructs a ``GPTBatchRequest`` Document; validating one
    needs only its ``_document_settings`` (the synchronous half of
    ``init_beanie``) — the same offline pattern the service tests use."""
    from beanie.odm.settings.document import DocumentSettings
    from llm_providers.db_models.gpt_batch_request import GPTBatchRequest

    settings_class = getattr(GPTBatchRequest, "Settings")  # noqa: B009 — pyright-safe access
    settings_vars = {
        a: getattr(settings_class, a) for a in dir(settings_class) if not a.startswith("__")
    }
    GPTBatchRequest._document_settings = DocumentSettings(**settings_vars)


class _Field:
    name = "material_caps"


def _request(result_json: str, user_message: str) -> Any:
    return SimpleNamespace(
        response=SimpleNamespace(result=result_json),
        request=SimpleNamespace(body=SimpleNamespace(user_message=lambda: user_message)),
    )


def _search(phrases: list[str]) -> Any:
    import json

    return _request(json.dumps({"phrases": phrases}), "")


def _answer(by_id: dict[str, str]) -> str:
    import json

    return json.dumps(
        {"mentions": [{"mention_id": i, "location": loc} for i, loc in by_id.items()]}
    )


class _Node(LLMPhraseMentionCollectionNode):
    """The node with its two Mongo-backed checks and its upstream maps faked."""

    def __init__(self, search_map: dict[str, Any]):
        super().__init__(
            field_type=cast(Any, _Field()),
            next_node=cast(Any, None),
            phrase_mention_collection_prompt=Prompt(
                text="P", s3_version_id="v", name="p", num_tokens=1
            ),
        )
        self._search = search_map
        self.complete = False
        self.completed: dict[str, Any] = {}

    def get_upstream_phrase_search_map(self, pipeline_context: PipelineContext):
        return self._search

    def get_upstream_recursive_search_map(self, pipeline_context: PipelineContext):
        return {}

    async def are_all_requests_complete(self, subject_unique_id, chunked_request_map):  # type: ignore[override]
        return self.complete

    async def get_completed_request_map(
        self, subject_unique_id, chunked_request_map, all_requests_must_be_complete=True
    ):  # type: ignore[override]
        return self.completed


def _metadata(max_mentions: int) -> Any:
    return SimpleNamespace(
        llm_phrase_mention_collection=BatchedMentionCollectionNodeMetadata(
            llm_model=LLM_Model(name="gpt-4.1", max_context_tokens=128000),
            model_params=GPTModelParams.with_defaults(),
            prompt_name="material_cap_phrase_mention_collection",
            prompt_version_id="pv",
            created_at=T0,
            max_mentions_per_request=max_mentions,
        )
    )


@pytest.mark.asyncio
async def test_pass_one_pools_the_chunks_forms_and_filters_each_window_by_occurrence():
    node = _Node({"s0": _search(["Aluminum", "Brass"]), "s1": _search(["Lead"])})
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[SUB0, SUB1], llm_phrase_search_req_ids=["s0", "s1"]
    )
    ctx = PipelineContext(subject_text=TEXT)
    await node.embed_request_ids(SUBJECT, ctx, _metadata(2), {CHUNK: bundle}, T0)
    # 'Lead' was search's find in window 1 only; it does not occur in window 0, so
    # window 0 is not sent it; window 1 gets the pool ('Aluminum' occurs there as a casing)
    assert bundle.llm_phrase_mention_sent_forms == {
        SUB0: ["Aluminum", "Brass"],
        SUB1: ["Aluminum", "Brass", "Lead"],
    }
    assert [len(v) for v in bundle.llm_phrase_mention_req_ids.values()] == [
        1,
        2,
    ]  # 1 item; 3 items / 2 per request
    assert all(
        ">retry>" not in rid for rids in bundle.llm_phrase_mention_req_ids.values() for rid in rids
    )
    assert (
        bundle.llm_phrase_mention_retry_mention_ids == {}
        and bundle.llm_phrase_mention_retry_req_ids == {}
    )
    # a second entry before the answers exist changes nothing
    before = bundle.model_dump()
    await node.embed_request_ids(SUBJECT, ctx, _metadata(2), {CHUNK: bundle}, T0)
    assert bundle.model_dump() == before


@pytest.mark.asyncio
async def test_pass_two_assesses_every_window_once_and_retries_only_the_undescribed():
    node = _Node({"s0": _search(["Aluminum", "Brass"]), "s1": _search(["Lead"])})
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[SUB0, SUB1], llm_phrase_search_req_ids=["s0", "s1"]
    )
    ctx = PipelineContext(subject_text=TEXT)
    metadata = _metadata(2)
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    (g0,), (g1a, g1b) = (
        bundle.llm_phrase_mention_req_ids[SUB0],
        bundle.llm_phrase_mention_req_ids[SUB1],
    )
    c0 = collect_sub_window(TEXT, SUB0, bundle.llm_phrase_mention_sent_forms[SUB0])
    c1 = collect_sub_window(TEXT, SUB1, bundle.llm_phrase_mention_sent_forms[SUB1])
    (intro,), (alloys, solder, footer) = c0.items, c1.items
    w0, w1 = window_text_of(TEXT, SUB0), window_text_of(TEXT, SUB1)
    node.complete = True
    node.completed = {
        g0: _request(
            _answer({intro.mention_id: "intro"}), render_mention_location_context(w0, [intro])
        ),
        # group 1a answered one of its two items and echoed a never-sent id
        g1a: _request(
            _answer({alloys.mention_id: "line 2", "a02b6f52": "no such mention"}),
            render_mention_location_context(w1, [alloys, solder]),
        ),
        g1b: _request(
            _answer({footer.mention_id: "footer"}), render_mention_location_context(w1, [footer])
        ),
    }
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    assert bundle.llm_phrase_mention_retry_mention_ids == {SUB0: [], SUB1: [solder.mention_id]}
    (retry_id,) = bundle.llm_phrase_mention_retry_req_ids[SUB1]
    assert f">sub>{SUB1}>retry>1>group>0>" in retry_id and "|gs=2|ud=" in retry_id
    assert SUB0 not in bundle.llm_phrase_mention_retry_req_ids
    assert retry_id in node.get_embedded_request_ids(SUBJECT, {CHUNK: bundle})
    # a third entry, with the retry still unanswered or answered, adds nothing
    before = bundle.model_dump()
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    assert bundle.model_dump() == before
    # the retry requests are built for the stored ids (the retry id was the missing one)
    requests = await node.create_batch_requests(
        subject_unique_id=SUBJECT,
        scraped_text_file=cast(Any, SimpleNamespace(text=TEXT)),
        missing_request_ids={retry_id},
        metadata=metadata,
        chunked_request_map={CHUNK: bundle},
        pipeline_context=ctx,
        timestamp=T0,
        eager=True,
    )
    assert [r.request.custom_id for r in requests] == [retry_id]
