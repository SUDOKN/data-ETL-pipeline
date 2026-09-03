"""The synthesis node's two embed passes (PIPELINE_V3_PLAN.md Phase 3.2, user
decisions 2026-08-22): pass 1 folds each chunk from the mention stage's answers
and packs its records into group requests; pass 2, once those are complete,
assesses each chunk and embeds ONE retry request set for the records left
unsynthesized. Also: the contract/pure identity, the location arm and the
snippet radius reaching the ids, and the result/dump over the held answers."""

import json
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
from core.models.extraction_results.extraction_node_metadata import (
    AggregationFoldMetadata,
    BatchedMentionCollectionNodeMetadata,
    BatchedSynthesisNodeMetadata,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_mention_collection_node import (
    LLMPhraseMentionCollectionNode,
)
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_synthesis_node import (
    LLMPhraseSynthesisNode,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_mention_collection_node_service import (
    collect_sub_window,
    render_mention_location_context,
    window_text_of,
)
from core.utils.form_normalizer import NORMALIZER_VERSION
from core.utils.synthesis_dump_util import build_synthesis_dump

SEP = "#" * 50
SUBJECT = "acme.example"
T0 = datetime(2026, 8, 22, 12, 0, 0)
TEXT = (
    f"{SEP}\nhttps://acme.example/materials\n\n"
    "We stock Aluminum and Brass. Brass is polished here.\n"
    "aluminum alloys ship daily.\n"
    f"{SEP}\nhttps://acme.example/about\n\n"
    "Lead-free solder only. Aluminum | Brass | Steel\n"
)
CHUNK = f"0:{len(TEXT)}"
MODEL = LLM_Model(name="gpt-4.1", max_context_tokens=128000)


@pytest.fixture(autouse=True, scope="module")
def offline_gpt_batch_request_settings():
    from beanie.odm.settings.document import DocumentSettings
    from llm_providers.db_models.gpt_batch_request import GPTBatchRequest

    settings_class = getattr(GPTBatchRequest, "Settings")  # noqa: B009
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
    return _request(json.dumps({"phrases": phrases}), "")


def _locations(by_id: dict[str, str]) -> str:
    return json.dumps(
        {"mentions": [{"mention_id": i, "location": loc} for i, loc in by_id.items()]}
    )


def _syntheses(by_id: dict[str, str]) -> str:
    return json.dumps({"syntheses": [{"record_id": i, "synthesis": s} for i, s in by_id.items()]})


class _MentionNode(LLMPhraseMentionCollectionNode):
    def __init__(self, search_map):
        super().__init__(
            field_type=cast(Any, _Field()),
            next_node=cast(Any, None),
            phrase_mention_collection_prompt=Prompt(
                text="P", s3_version_id="v", name="p", num_tokens=1
            ),
        )
        self._search = search_map

    def get_upstream_phrase_search_map(self, pipeline_context):
        return self._search

    def get_upstream_recursive_search_map(self, pipeline_context):
        return {}


class _SynthesisNode(LLMPhraseSynthesisNode):
    """The node with its Mongo-backed checks and its upstream map faked."""

    def __init__(self, mention_map: dict[str, Any]):
        super().__init__(
            field_type=cast(Any, _Field()),
            next_node=cast(Any, None),
            phrase_synthesis_prompt=Prompt(text="S", s3_version_id="v", name="s", num_tokens=1),
        )
        self._mentions = mention_map
        self.complete = False
        self.completed: dict[str, Any] = {}

    def get_upstream_mention_collection_map(self, pipeline_context):
        return self._mentions

    async def are_all_requests_complete(self, subject_unique_id, chunked_request_map):  # type: ignore[override]
        return self.complete

    async def get_completed_request_map(
        self, subject_unique_id, chunked_request_map, all_requests_must_be_complete=True
    ):  # type: ignore[override]
        return self.completed


def _metadata(*, max_entries: int = 50, include_location: bool = True, radius: int = 0) -> Any:
    return SimpleNamespace(
        llm_phrase_mention_collection=BatchedMentionCollectionNodeMetadata(
            llm_model=MODEL,
            model_params=GPTModelParams.with_defaults(),
            prompt_name="material_cap_phrase_mention_collection",
            prompt_version_id="pv-m",
            created_at=T0,
            max_mentions_per_request=50,
            snippet_radius=radius,
        ),
        aggregation_fold=AggregationFoldMetadata(
            normalizer_version=NORMALIZER_VERSION, verb_fold=False
        ),
        llm_phrase_synthesis=BatchedSynthesisNodeMetadata(
            llm_model=MODEL,
            model_params=GPTModelParams.with_defaults(),
            prompt_name="material_cap_phrase_synthesis",
            prompt_version_id="pv-s",
            created_at=T0,
            max_entries_per_request=max_entries,
            include_location=include_location,
        ),
    )


async def _mention_state(
    metadata,
) -> tuple[LLMPhraseExtractionRequestBundle, dict[str, Any], PipelineContext]:
    """Run the mention node's pass 1 over one chunk / one window and answer
    every location request, so the synthesis node has a completed upstream."""
    node = _MentionNode({"s0": _search(["Aluminum", "Brass", "Lead"])})
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[CHUNK], llm_phrase_search_req_ids=["s0"]
    )
    ctx = PipelineContext(subject_name="Acme Example", subject_text=TEXT)
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    (group_id,) = bundle.llm_phrase_mention_req_ids[CHUNK]
    radius = metadata.llm_phrase_mention_collection.snippet_radius
    collection = collect_sub_window(
        TEXT, CHUNK, bundle.llm_phrase_mention_sent_forms[CHUNK], snippet_radius=radius
    )
    answered = {item.mention_id: f"loc of {item.mention[:12]}" for item in collection.items}
    mention_map = {
        group_id: _request(
            _locations(answered),
            render_mention_location_context(window_text_of(TEXT, CHUNK), collection.items),
        )
    }
    return bundle, mention_map, ctx


@pytest.mark.asyncio
async def test_pass_one_packs_the_chunks_records_in_bundle_order_and_is_idempotent():
    metadata = _metadata(max_entries=2)
    bundle, mention_map, ctx = await _mention_state(metadata)
    node = _SynthesisNode(mention_map)
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    ids = bundle.llm_phrase_synthesis_req_ids
    # three non-empty groups (aluminum: 3 entries, brass: 3 entries, lead: 1) under
    # a cap of 2 → every record alone → three requests, in bundle order
    assert len(ids) == 3
    assert all(f">material_caps>llm_phrase_synthesis>chunk>{CHUNK}>group>" in rid for rid in ids)
    assert all("|gs=2|loc=1|ud=" in rid for rid in ids)
    assert bundle.llm_phrase_synthesis_retry_record_ids is None
    assert bundle.llm_phrase_synthesis_retry_req_ids == []
    before = bundle.model_dump()
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    assert bundle.model_dump() == before
    # a larger cap packs them together; the arm and the radius change the ids
    wide = LLMPhraseExtractionRequestBundle(
        **{k: v for k, v in before.items() if k != "llm_phrase_synthesis_req_ids"}
    )
    wide.llm_phrase_synthesis_req_ids = []
    await _SynthesisNode(mention_map).embed_request_ids(
        SUBJECT, ctx, _metadata(max_entries=50), {CHUNK: wide}, T0
    )
    assert (
        len(wide.llm_phrase_synthesis_req_ids) == 1
        and "|gs=50|loc=1|ud=" in wide.llm_phrase_synthesis_req_ids[0]
    )
    no_loc = LLMPhraseExtractionRequestBundle(
        **{k: v for k, v in before.items() if k != "llm_phrase_synthesis_req_ids"}
    )
    no_loc.llm_phrase_synthesis_req_ids = []
    await _SynthesisNode(mention_map).embed_request_ids(
        SUBJECT, ctx, _metadata(max_entries=50, include_location=False), {CHUNK: no_loc}, T0
    )
    (no_loc_id,) = no_loc.llm_phrase_synthesis_req_ids
    assert "|gs=50|loc=0|ud=" in no_loc_id
    assert no_loc_id.split("|ud=")[1] != wide.llm_phrase_synthesis_req_ids[0].split("|ud=")[1]


@pytest.mark.asyncio
async def test_snippet_radius_reaches_the_mention_ids_and_the_synthesis_digest():
    tight = _metadata(radius=0)
    wide = _metadata(radius=1)
    b0, m0, ctx = await _mention_state(tight)
    b1, m1, _ = await _mention_state(wide)
    (id0,), (id1,) = b0.llm_phrase_mention_req_ids[CHUNK], b1.llm_phrase_mention_req_ids[CHUNK]
    assert "|rad=" not in id0 and "|gs=50|rad=1|ud=" in id1 and id0 != id1
    n0, n1 = _SynthesisNode(m0), _SynthesisNode(m1)
    await n0.embed_request_ids(SUBJECT, ctx, tight, {CHUNK: b0}, T0)
    await n1.embed_request_ids(SUBJECT, ctx, wide, {CHUNK: b1}, T0)
    (s0,), (s1,) = b0.llm_phrase_synthesis_req_ids, b1.llm_phrase_synthesis_req_ids
    assert s0.split("|ud=")[1] != s1.split("|ud=")[1]  # wider snippets → different records


@pytest.mark.asyncio
async def test_pass_two_assesses_once_retries_only_the_missing_and_the_result_reads_both():
    metadata = _metadata(max_entries=50)
    bundle, mention_map, ctx = await _mention_state(metadata)
    node = _SynthesisNode(mention_map)
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    (group_req_id,) = bundle.llm_phrase_synthesis_req_ids
    # what the request carried: the chunk's three records, in bundle order
    requests = await node.create_batch_requests(
        subject_unique_id=SUBJECT,
        scraped_text_file=cast(Any, SimpleNamespace(text=TEXT)),
        missing_request_ids={group_req_id},
        metadata=metadata,
        chunked_request_map={CHUNK: bundle},
        pipeline_context=ctx,
        timestamp=T0,
        eager=True,
    )
    (req,) = requests
    assert req.request.custom_id == group_req_id
    user_message = req.request.body.user_message()
    # (the request nonce header comes first; the context follows it)
    assert "\nthe name of the manufacturer in question: Acme Example\n" in user_message
    from core.services.phrase_blocks_contract import (
        sent_record_ids_from_user_message,
        sent_records_from_user_message,
    )

    sent = sent_record_ids_from_user_message(user_message) or []
    records = sent_records_from_user_message(user_message) or []
    assert len(sent) == 3 and [r["focal_form"] for r in records] == ["Aluminum", "Brass", "Lead"]
    assert all("location" in e for r in records for e in r["entries"])
    # the model answers two of three and echoes a never-sent id
    node.complete = True
    node.completed = {
        group_req_id: _request(
            _syntheses({sent[0]: "aluminum text", sent[2]: "lead text", "gnope": "x"}), user_message
        )
    }
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    assert bundle.llm_phrase_synthesis_retry_record_ids == [sent[1]]
    (retry_id,) = bundle.llm_phrase_synthesis_retry_req_ids
    assert f">chunk>{CHUNK}>retry>1>group>0>" in retry_id and "|gs=50|loc=1|ud=" in retry_id
    assert retry_id in node.get_embedded_request_ids(SUBJECT, {CHUNK: bundle})
    before = bundle.model_dump()
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    assert bundle.model_dump() == before
    # the retry request carries exactly the missing record
    (retry_req,) = await node.create_batch_requests(
        subject_unique_id=SUBJECT,
        scraped_text_file=cast(Any, SimpleNamespace(text=TEXT)),
        missing_request_ids={retry_id},
        metadata=metadata,
        chunked_request_map={CHUNK: bundle},
        pipeline_context=ctx,
        timestamp=T0,
        eager=True,
    )
    retry_message = retry_req.request.body.user_message()
    assert sent_record_ids_from_user_message(retry_message) == [sent[1]]
    # the result reads groups then the retry; the dump shows all of it
    node.completed[retry_id] = _request(_syntheses({sent[1]: "brass text (retry)"}), retry_message)
    result = await node.get_result(
        subject_unique_id=SUBJECT,
        field_type=cast(Any, _Field()),
        chunk_bounds=CHUNK,
        extraction_bundle=bundle,
        completed_request_map=node.completed,
        timestamp=T0,
        mention_completed_request_map=mention_map,
        subject_text=TEXT,
        verb_fold=False,
        snippet_radius=0,
        include_location=True,
    )
    assert result.syntheses == {
        sent[0]: "aluminum text",
        sent[1]: "brass text (retry)",
        sent[2]: "lead text",
    }
    assert result.not_synthesized == [] and result.answer.unknown_answer_ids == ["gnope"]
    dump = build_synthesis_dump(result, subject_name="Acme Example")
    assert dump["include_location"] is True
    assert dump["summary"]["records"] == 3 and dump["summary"]["synthesized"] == 3
    assert dump["summary"]["retried"] == [sent[1]] and dump["summary"]["unknown_answer_ids"] == [
        "gnope"
    ]
    assert dump["summary"]["group_requests"] == 1 and dump["summary"]["retry_requests"] == 1
    rows = {r["group_id"]: r for r in dump["records"]}
    assert rows[sent[1]]["retried"] is True and rows[sent[1]]["synthesis"] == "brass text (retry)"
    assert (
        rows[sent[0]]["forms"] == ["Aluminum", "aluminum"]
        and rows[sent[0]]["focal_form"] == "Aluminum"
    )
    assert rows[sent[0]]["entries"] == 3 and rows[sent[0]]["status"] == "synthesized"


DESIG_TEXT = (
    f"{SEP}\nhttps://acme.example/alloys\n\n"
    "Aluminum castings in 319 and A357 alloys ship daily.\n"
)
DESIG_CHUNK = f"0:{len(DESIG_TEXT)}"


async def _designation_mention_state(metadata):
    """One chunk over designation-bearing text, mentions answered — the
    under-enumeration flow's upstream."""
    node = _MentionNode({"s0": _search(["Aluminum"])})
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[DESIG_CHUNK], llm_phrase_search_req_ids=["s0"]
    )
    ctx = PipelineContext(subject_name="Acme Example", subject_text=DESIG_TEXT)
    await node.embed_request_ids(SUBJECT, ctx, metadata, {DESIG_CHUNK: bundle}, T0)
    (group_id,) = bundle.llm_phrase_mention_req_ids[DESIG_CHUNK]
    radius = metadata.llm_phrase_mention_collection.snippet_radius
    collection = collect_sub_window(
        DESIG_TEXT, DESIG_CHUNK, bundle.llm_phrase_mention_sent_forms[DESIG_CHUNK],
        snippet_radius=radius,
    )
    answered = {item.mention_id: f"loc of {item.mention[:12]}" for item in collection.items}
    mention_map = {
        group_id: _request(
            _locations(answered),
            render_mention_location_context(
                window_text_of(DESIG_TEXT, DESIG_CHUNK), collection.items
            ),
        )
    }
    return bundle, mention_map, ctx


@pytest.mark.asyncio
async def test_an_answered_record_that_drops_designations_is_retried_and_the_better_answer_wins():
    """2026-09-02 (Phase B): the under-enumeration conservation check. The
    first answer names the focal form but drops the designations its entries
    carry (319, A357); the assess pass re-asks the record even though it WAS
    answered, and the read path keeps whichever answer names more
    designations — here the retry's."""
    metadata = _metadata(max_entries=50)
    bundle, mention_map, ctx = await _designation_mention_state(metadata)
    node = _SynthesisNode(mention_map)
    await node.embed_request_ids(SUBJECT, ctx, metadata, {DESIG_CHUNK: bundle}, T0)
    (group_req_id,) = bundle.llm_phrase_synthesis_req_ids
    (req,) = await node.create_batch_requests(
        subject_unique_id=SUBJECT,
        scraped_text_file=cast(Any, SimpleNamespace(text=DESIG_TEXT)),
        missing_request_ids={group_req_id},
        metadata=metadata,
        chunked_request_map={DESIG_CHUNK: bundle},
        pipeline_context=ctx,
        timestamp=T0,
        eager=True,
    )
    user_message = req.request.body.user_message()
    from core.services.phrase_blocks_contract import sent_record_ids_from_user_message

    (record_id,) = sent_record_ids_from_user_message(user_message) or []
    # the first answer ELIDES: no 319, no A357
    node.complete = True
    node.completed = {
        group_req_id: _request(
            _syntheses({record_id: "The entries show aluminum castings, various alloys."}),
            user_message,
        )
    }
    await node.embed_request_ids(SUBJECT, ctx, metadata, {DESIG_CHUNK: bundle}, T0)
    assert bundle.llm_phrase_synthesis_retry_record_ids == [record_id]
    (retry_id,) = bundle.llm_phrase_synthesis_retry_req_ids
    (retry_req,) = await node.create_batch_requests(
        subject_unique_id=SUBJECT,
        scraped_text_file=cast(Any, SimpleNamespace(text=DESIG_TEXT)),
        missing_request_ids={retry_id},
        metadata=metadata,
        chunked_request_map={DESIG_CHUNK: bundle},
        pipeline_context=ctx,
        timestamp=T0,
        eager=True,
    )
    retry_message = retry_req.request.body.user_message()
    good = "The entries show Aluminum castings in 319 and A357 alloys."
    node.completed[retry_id] = _request(_syntheses({record_id: good}), retry_message)
    result = await node.get_result(
        subject_unique_id=SUBJECT,
        field_type=cast(Any, _Field()),
        chunk_bounds=DESIG_CHUNK,
        extraction_bundle=bundle,
        completed_request_map=node.completed,
        timestamp=T0,
        mention_completed_request_map=mention_map,
        subject_text=DESIG_TEXT,
        verb_fold=False,
        snippet_radius=0,
        include_location=True,
    )
    assert result.syntheses == {record_id: good}
    assert result.under_enumeration_resolved == {record_id: "retry"}
    assert result.not_synthesized == []


@pytest.mark.asyncio
async def test_a_worse_retry_never_regresses_the_first_answer():
    """The comparator half of the check: when the re-ask comes back WORSE
    (fewer designations named), the first answer is kept and the provenance
    says so."""
    from core.models.extraction_schemas.synthesis import SynthesisEntry, SynthesisRecordInput
    from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
        ChunkAnswer,
        resolve_under_enumeration,
    )

    record = SynthesisRecordInput(
        record_id="g1",
        focal_form="Aluminum",
        entries=[SynthesisEntry(snippet="Aluminum castings in 319 and A357 alloys.")],
    )
    answer = ChunkAnswer(
        sent_ids=["g1"],
        syntheses={"g1": "Aluminum in 319 and A357 alloys."},
        unknown_answer_ids=[],
        retried_record_ids=["g1"],
        retry_syntheses={"g1": "Aluminum in A357 alloy, among others."},
    )
    resolved, provenance = resolve_under_enumeration(answer, [record])
    assert resolved.syntheses["g1"] == "Aluminum in 319 and A357 alloys."
    assert provenance == {"g1": "first"}


@pytest.mark.asyncio
async def test_a_chunk_with_no_records_gets_one_dummy_that_is_never_dispatched():
    metadata = _metadata()
    node = _MentionNode({"s0": _search(["Unobtainium"])})
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[CHUNK], llm_phrase_search_req_ids=["s0"]
    )
    ctx = PipelineContext(subject_name="Acme Example", subject_text=TEXT)
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    (mention_req_id,) = bundle.llm_phrase_mention_req_ids[CHUNK]
    mention_map = {
        mention_req_id: _request(
            json.dumps({"mentions": []}),
            "No mention location needed - no mentions collected in this window.",
        )
    }
    synthesis = _SynthesisNode(mention_map)
    await synthesis.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    (req_id,) = bundle.llm_phrase_synthesis_req_ids
    (req,) = await synthesis.create_batch_requests(
        subject_unique_id=SUBJECT,
        scraped_text_file=cast(Any, SimpleNamespace(text=TEXT)),
        missing_request_ids={req_id},
        metadata=metadata,
        chunked_request_map={CHUNK: bundle},
        pipeline_context=ctx,
        timestamp=T0,
        eager=True,
    )
    assert req.response is not None  # pre-answered: the recursive base never dispatches it
    assert json.loads(req.response.result or "") == {"syntheses": []}
