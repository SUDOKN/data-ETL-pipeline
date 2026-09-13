"""The synthesis node's embed passes (PIPELINE_V3_PLAN.md Phase 3.2; the
location-stage merge, 2026-09-03): pass 0 stores each sub-window's sent forms
from the completed search maps (the retired mention node's job, absorbed);
pass 1 folds each chunk pure-code and packs its records into group requests;
pass 2, once those are complete, assesses each chunk and embeds ONE retry
request set for the records left unsynthesized or under-enumerated. Also: the
contract/pure identity, the snippet radius reaching the digests, the chunk
text in the request, and the result/dump over the held answers."""

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
    BatchedSynthesisNodeMetadata,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_synthesis_node import (
    LLMPhraseSynthesisNode,
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


# The four labels every wire record carries since 2026-09-13 (see synthesis.py).
_LABELS = {
    "doer": "the manufacturer",
    "doer_name": "",
    "capacity": "makes, performs, or provides it as its own",
    "dealing_words": "works with",
}

def _syntheses(by_id: dict[str, str]) -> str:
    """Answers as the wire writes them: {record_id: synthesis}."""
    return json.dumps(
        {"syntheses": [{"record_id": i, **_LABELS, "synthesis": s} for i, s in by_id.items()]}
    )


class _SynthesisNode(LLMPhraseSynthesisNode):
    """The node with its Mongo-backed checks and its upstream maps faked."""

    def __init__(self, search_map: dict[str, Any]):
        super().__init__(
            field_type=cast(Any, _Field()),
            next_node=cast(Any, None),
            phrase_synthesis_prompt=Prompt(text="S", s3_version_id="v", name="s", num_tokens=1),
        )
        self._search = search_map
        self.complete = False
        self.completed: dict[str, Any] = {}

    def get_upstream_phrase_search_map(self, pipeline_context):
        return self._search

    def get_upstream_recursive_search_map(self, pipeline_context):
        return {}

    async def are_all_requests_complete(self, subject_unique_id, chunked_request_map):  # type: ignore[override]
        return self.complete

    async def get_completed_request_map(
        self, subject_unique_id, chunked_request_map, all_requests_must_be_complete=True
    ):  # type: ignore[override]
        return self.completed


def _metadata(*, max_entries: int = 50, radius: int = 0) -> Any:
    return SimpleNamespace(
        aggregation_fold=AggregationFoldMetadata(
            normalizer_version=NORMALIZER_VERSION, verb_fold=False, snippet_radius=radius
        ),
        llm_phrase_synthesis=BatchedSynthesisNodeMetadata(
            llm_model=MODEL,
            model_params=GPTModelParams.with_defaults(),
            prompt_name="material_cap_phrase_synthesis",
            prompt_version_id="pv-s",
            created_at=T0,
            max_entries_per_request=max_entries,
        ),
    )


def _bundle() -> LLMPhraseExtractionRequestBundle:
    return LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[CHUNK], llm_phrase_search_req_ids=["s0"]
    )


def _ctx(text: str = TEXT) -> PipelineContext:
    return PipelineContext(subject_name="Acme Example", subject_text=text)


@pytest.mark.asyncio
async def test_pass_zero_stores_the_sent_forms_and_pass_one_packs_in_bundle_order():
    metadata = _metadata(max_entries=2)
    bundle, ctx = _bundle(), _ctx()
    node = _SynthesisNode({"s0": _search(["Aluminum", "Brass", "Lead"])})
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    # pass 0: the sent forms landed on the bundle (occurrence-filtered)
    assert bundle.llm_phrase_mention_sent_forms[CHUNK] == ["Aluminum", "Brass", "Lead"]
    ids = bundle.llm_phrase_synthesis_req_ids
    # three non-empty groups (aluminum: 3 snippets, brass: 3 snippets, lead: 1) under
    # a cap of 2 → every record alone → three requests, in bundle order
    assert len(ids) == 3
    assert all(f">material_caps>llm_phrase_synthesis>chunk>{CHUNK}>group>" in rid for rid in ids)
    assert all("|gs=2|ud=" in rid for rid in ids)
    assert all("|loc=" not in rid for rid in ids)
    assert bundle.llm_phrase_synthesis_retry_record_ids is None
    assert bundle.llm_phrase_synthesis_retry_req_ids == []
    before = bundle.model_dump()
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    assert bundle.model_dump() == before
    # a larger cap packs them together
    wide = _bundle()
    await _SynthesisNode({"s0": _search(["Aluminum", "Brass", "Lead"])}).embed_request_ids(
        SUBJECT, _ctx(), _metadata(max_entries=50), {CHUNK: wide}, T0
    )
    assert (
        len(wide.llm_phrase_synthesis_req_ids) == 1
        and "|gs=50|ud=" in wide.llm_phrase_synthesis_req_ids[0]
    )


@pytest.mark.asyncio
async def test_snippet_radius_reaches_the_synthesis_digest():
    tight_bundle, wide_bundle = _bundle(), _bundle()
    search = {"s0": _search(["Aluminum"])}
    await _SynthesisNode(search).embed_request_ids(
        SUBJECT, _ctx(), _metadata(radius=0), {CHUNK: tight_bundle}, T0
    )
    await _SynthesisNode(search).embed_request_ids(
        SUBJECT, _ctx(), _metadata(radius=1), {CHUNK: wide_bundle}, T0
    )
    (s0,), (s1,) = tight_bundle.llm_phrase_synthesis_req_ids, wide_bundle.llm_phrase_synthesis_req_ids
    assert s0.split("|ud=")[1] != s1.split("|ud=")[1]  # wider snippets → different records


@pytest.mark.asyncio
async def test_pass_two_assesses_once_retries_only_the_missing_and_the_result_reads_both():
    metadata = _metadata(max_entries=50)
    bundle, ctx = _bundle(), _ctx()
    node = _SynthesisNode({"s0": _search(["Aluminum", "Brass", "Lead"])})
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    (group_req_id,) = bundle.llm_phrase_synthesis_req_ids
    # what the request carried: the chunk's text, the name, the three records
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
    assert "text scraped from a manufacturer's website:\n" in user_message
    assert "We stock Aluminum and Brass." in user_message  # the chunk text rides the wire
    assert "\nthe name of the manufacturer in question: Acme Example\n" in user_message
    from core.services.phrase_blocks_contract import (
        sent_record_ids_from_user_message,
        sent_records_from_user_message,
    )

    sent = sent_record_ids_from_user_message(user_message) or []
    records = sent_records_from_user_message(user_message) or []
    assert len(sent) == 3 and [r["focal_form"] for r in records] == ["Aluminum", "Brass", "Lead"]
    # snippets are bare verbatim strings since the merge
    assert all(isinstance(s, str) for r in records for s in r["snippets"])
    # the model answers two of three and echoes a never-sent id
    node.complete = True
    node.completed = {
        group_req_id: _request(
            _syntheses({sent[0]: "aluminum text", sent[2]: "lead text", "gnope": "x"}),
            user_message,
        )
    }
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    assert bundle.llm_phrase_synthesis_retry_record_ids == [sent[1]]
    (retry_id,) = bundle.llm_phrase_synthesis_retry_req_ids
    assert f">chunk>{CHUNK}>retry>1>group>0>" in retry_id and "|gs=50|ud=" in retry_id
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
    node.completed[retry_id] = _request(
        _syntheses({sent[1]: "brass text (retry)"}), retry_message
    )
    result = await node.get_result(
        subject_unique_id=SUBJECT,
        field_type=cast(Any, _Field()),
        chunk_bounds=CHUNK,
        extraction_bundle=bundle,
        completed_request_map=node.completed,
        timestamp=T0,
        subject_text=TEXT,
        verb_fold=False,
        snippet_radius=0,
    )
    assert {gid: a.synthesis for gid, a in result.syntheses.items()} == {
        sent[0]: "aluminum text",
        sent[1]: "brass text (retry)",
        sent[2]: "lead text",
    }
    assert result.not_synthesized == [] and result.answer.unknown_answer_ids == ["gnope"]
    dump = build_synthesis_dump(result, subject_name="Acme Example")
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
    assert rows[sent[0]]["snippets"] == 3 and rows[sent[0]]["status"] == "synthesized"


@pytest.mark.asyncio
async def test_a_record_answered_twice_is_re_asked_together_with_the_sibling_it_overwrote():
    """2026-09-12: one production response answered a record id twice with two
    different paragraphs and left a sibling unanswered (the sibling's id was
    overwritten). The parser used to raise, the parse-error retry re-asked the
    whole request three times, the model repeated the defect, and the subject
    lost two fields. Now the repeated id is dropped with both its answers and
    the ordinary under-answer retry re-asks it together with the sibling —
    through the real assess path, not a stub."""
    metadata = _metadata(max_entries=50)
    bundle, ctx = _bundle(), _ctx()
    node = _SynthesisNode({"s0": _search(["Aluminum", "Brass", "Lead"])})
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    (group_req_id,) = bundle.llm_phrase_synthesis_req_ids
    (req,) = await node.create_batch_requests(
        subject_unique_id=SUBJECT,
        scraped_text_file=cast(Any, SimpleNamespace(text=TEXT)),
        missing_request_ids={group_req_id},
        metadata=metadata,
        chunked_request_map={CHUNK: bundle},
        pipeline_context=ctx,
        timestamp=T0,
        eager=True,
    )
    user_message = req.request.body.user_message()
    from core.services.phrase_blocks_contract import sent_record_ids_from_user_message

    sent = sent_record_ids_from_user_message(user_message) or []
    assert len(sent) == 3
    # aluminum answered twice (the second paragraph is really brass's), brass
    # never answered, lead answered once
    twice = json.dumps({"syntheses": [
        {"record_id": sent[0], **_LABELS, "synthesis": "aluminum text"},
        {"record_id": sent[0], **_LABELS, "synthesis": "brass text under aluminum's id"},
        {"record_id": sent[2], **_LABELS, "synthesis": "lead text"},
    ]})
    node.complete = True
    node.completed = {group_req_id: _request(twice, user_message)}
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    # no parse error was recorded (that path touches Mongo and would have
    # raised here); the retry names the repeated id AND the overwritten sibling
    assert bundle.llm_phrase_synthesis_retry_record_ids == [sent[0], sent[1]]
    (retry_id,) = bundle.llm_phrase_synthesis_retry_req_ids
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
    assert sent_record_ids_from_user_message(retry_message) == [sent[0], sent[1]]
    node.completed[retry_id] = _request(
        _syntheses({sent[0]: "aluminum text (retry)", sent[1]: "brass text (retry)"}),
        retry_message,
    )
    result = await node.get_result(
        subject_unique_id=SUBJECT,
        field_type=cast(Any, _Field()),
        chunk_bounds=CHUNK,
        extraction_bundle=bundle,
        completed_request_map=node.completed,
        timestamp=T0,
        subject_text=TEXT,
        verb_fold=False,
        snippet_radius=0,
    )
    assert {gid: a.synthesis for gid, a in result.syntheses.items()} == {
        sent[0]: "aluminum text (retry)",
        sent[1]: "brass text (retry)",
        sent[2]: "lead text",
    }
    assert result.not_synthesized == [] and result.answer.unknown_answer_ids == []
    dump = build_synthesis_dump(result, subject_name="Acme Example")
    assert dump["summary"]["synthesized"] == 3 and dump["summary"]["retried"] == [sent[0], sent[1]]


@pytest.mark.asyncio
async def test_an_answered_record_that_drops_designations_is_retried_and_the_better_answer_wins():
    """2026-09-02 (Phase B): the under-enumeration conservation check. The
    first answer names the focal form but drops the designations its snippets
    carry (319, A357); the assess pass re-asks the record even though it WAS
    answered, and the read path keeps whichever answer names more
    designations — here the retry's."""
    desig_text = (
        f"{SEP}\nhttps://acme.example/alloys\n\n"
        "Aluminum castings in 319 and A357 alloys ship daily.\n"
    )
    desig_chunk = f"0:{len(desig_text)}"
    metadata = _metadata(max_entries=50)
    bundle = LLMPhraseExtractionRequestBundle(
        search_sub_bounds=[desig_chunk], llm_phrase_search_req_ids=["s0"]
    )
    ctx = _ctx(desig_text)
    node = _SynthesisNode({"s0": _search(["Aluminum"])})
    await node.embed_request_ids(SUBJECT, ctx, metadata, {desig_chunk: bundle}, T0)
    (group_req_id,) = bundle.llm_phrase_synthesis_req_ids
    (req,) = await node.create_batch_requests(
        subject_unique_id=SUBJECT,
        scraped_text_file=cast(Any, SimpleNamespace(text=desig_text)),
        missing_request_ids={group_req_id},
        metadata=metadata,
        chunked_request_map={desig_chunk: bundle},
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
            _syntheses({record_id: "The snippets show aluminum castings, various alloys."}),
            user_message,
        )
    }
    await node.embed_request_ids(SUBJECT, ctx, metadata, {desig_chunk: bundle}, T0)
    assert bundle.llm_phrase_synthesis_retry_record_ids == [record_id]
    (retry_id,) = bundle.llm_phrase_synthesis_retry_req_ids
    (retry_req,) = await node.create_batch_requests(
        subject_unique_id=SUBJECT,
        scraped_text_file=cast(Any, SimpleNamespace(text=desig_text)),
        missing_request_ids={retry_id},
        metadata=metadata,
        chunked_request_map={desig_chunk: bundle},
        pipeline_context=ctx,
        timestamp=T0,
        eager=True,
    )
    retry_message = retry_req.request.body.user_message()
    good = "The snippets show Aluminum castings in 319 and A357 alloys."
    node.completed[retry_id] = _request(
        _syntheses({record_id: good}), retry_message
    )
    result = await node.get_result(
        subject_unique_id=SUBJECT,
        field_type=cast(Any, _Field()),
        chunk_bounds=desig_chunk,
        extraction_bundle=bundle,
        completed_request_map=node.completed,
        timestamp=T0,
        subject_text=desig_text,
        verb_fold=False,
        snippet_radius=0,
    )
    assert result.synthesis_of(record_id) == good
    assert result.under_enumeration_resolved == {record_id: "retry"}
    assert result.not_synthesized == []


@pytest.mark.asyncio
async def test_a_worse_retry_never_regresses_the_first_answer():
    """The comparator half of the check: when the re-ask comes back WORSE
    (fewer designations named), the first answer is kept and the provenance
    says so."""
    from core.models.extraction_schemas.synthesis import (
        SynthesisAnswer,
        SynthesisRecordInput,
    )
    from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
        ChunkAnswer,
        resolve_under_enumeration,
    )

    record = SynthesisRecordInput(
        record_id="g1",
        focal_form="Aluminum",
        snippets=["Aluminum castings in 319 and A357 alloys."],
    )
    answer = ChunkAnswer(
        sent_ids=["g1"],
        syntheses={
            "g1": SynthesisAnswer(synthesis="Aluminum in 319 and A357 alloys.")
        },
        unknown_answer_ids=[],
        retried_record_ids=["g1"],
        retry_syntheses={
            "g1": SynthesisAnswer(synthesis="Aluminum in A357 alloy, among others.")
        },
    )
    resolved, provenance = resolve_under_enumeration(answer, [record], sibling_forms={})
    assert resolved.syntheses["g1"].synthesis == "Aluminum in 319 and A357 alloys."
    assert provenance == {"g1": "first"}


def test_a_tie_keeps_the_first_answer_and_sibling_forms_scope_the_demand():
    """2026-09-10 (D2/D16): the comparator counts only the record's OWN
    designations, cut at the chunk's sibling forms, and a retry that names
    exactly as many as the first answer does not replace it — the retried
    population failed at 19.8% against 5.4%, so a tie is not a recovery."""
    from core.models.extraction_schemas.synthesis import (
        SynthesisAnswer,
        SynthesisRecordInput,
    )
    from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
        ChunkAnswer,
        resolve_under_enumeration,
        under_enumerated_record_ids,
    )

    record = SynthesisRecordInput(
        record_id="g1",
        focal_form="Inconel",
        snippets=["We machine Inconel 625 and Inconel 718 daily."],
    )
    first = SynthesisAnswer(synthesis="The site says it machines Inconel daily.")
    # With Inconel 625 / 718 as their own records, the bare 'Inconel' owns
    # nothing but its name: the first answer is not under-enumerated ...
    siblings = {"g1": ["Inconel 625", "Inconel 718"]}
    assert under_enumerated_record_ids([record], {"g1": first}, sibling_forms=siblings) == []
    # ... and would be, were those grades not sibling records.
    assert under_enumerated_record_ids([record], {"g1": first}, sibling_forms={}) == ["g1"]

    answer = ChunkAnswer(
        sent_ids=["g1"],
        syntheses={"g1": first},
        unknown_answer_ids=[],
        retried_record_ids=["g1"],
        retry_syntheses={"g1": SynthesisAnswer(synthesis="Inconel is machined here, the site says.")},
    )
    resolved, provenance = resolve_under_enumeration(answer, [record], sibling_forms=siblings)
    assert resolved.syntheses["g1"] is first
    assert provenance == {"g1": "first"}


@pytest.mark.asyncio
async def test_a_chunk_with_no_records_gets_one_dummy_that_is_never_dispatched():
    metadata = _metadata()
    bundle, ctx = _bundle(), _ctx()
    node = _SynthesisNode({"s0": _search(["Unobtainium"])})
    await node.embed_request_ids(SUBJECT, ctx, metadata, {CHUNK: bundle}, T0)
    assert bundle.llm_phrase_mention_sent_forms[CHUNK] == []  # occurs nowhere
    (req_id,) = bundle.llm_phrase_synthesis_req_ids
    (req,) = await node.create_batch_requests(
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
