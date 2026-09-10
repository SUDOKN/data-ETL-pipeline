"""Phase 3.2 of pipeline v3 (PIPELINE_V3_PLAN.md D15 as amended 2026-08-22,
D16; the location-stage merge, 2026-09-03): the synthesis stage's service —
soft-cap packing that never splits a record, the request context with the
chunk's text, the exact hold (unknown ids dropped and reported, missing ids
left for the retry), the merged chunk answer with per-entry contexts, the
dummy."""

import json
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import LLM_Model, NO_MODEL
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
)
from core.models.extraction_schemas.synthesis import SynthesisRecordInput
from core.services.phrase_blocks_contract import (
    sent_record_ids_from_user_message,
    sent_records_from_user_message,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    ChunkAnswer,
    create_dummy_completed_synthesis_request,
    create_synthesis_gpt_request,
    get_chunk_syntheses,
    group_digest_payload,
    pack_records,
    parse_synthesis_group_result,
    render_synthesis_context,
    retry_records_of_chunk,
)

T0 = datetime(2026, 8, 22, 12, 0, 0)
SUBJECT = "acme.example"


class _Field:
    name = "material_caps"


@pytest.fixture(autouse=True, scope="module")
def offline_gpt_batch_request_settings():
    from beanie.odm.settings.document import DocumentSettings
    from llm_providers.db_models.gpt_batch_request import GPTBatchRequest

    settings_class = getattr(GPTBatchRequest, "Settings")  # noqa: B009
    settings_vars = {
        a: getattr(settings_class, a) for a in dir(settings_class) if not a.startswith("__")
    }
    GPTBatchRequest._document_settings = DocumentSettings(**settings_vars)


def _record(rid: str, n_snippets: int, *, focal: str = "Aluminum"):
    return SynthesisRecordInput(
        record_id=rid,
        focal_form=focal,
        snippets=[f"{focal} snippet {i}" for i in range(n_snippets)],
    )


def _request(result_json: str, user_message: str) -> Any:
    return SimpleNamespace(
        response=SimpleNamespace(result=result_json),
        request=SimpleNamespace(body=SimpleNamespace(user_message=lambda: user_message)),
    )


def _answer(by_id: dict[str, str]) -> str:
    return json.dumps(
        {"syntheses": [{"record_id": i, "synthesis": s} for i, s in by_id.items()]}
    )


# --- packing ---------------------------------------------------------------------------------


def test_pack_is_a_soft_cap_that_never_splits_a_record_and_isolates_a_monster():
    records = [
        _record("g1", 3),
        _record("g2", 3),
        _record("g3", 10),
        _record("g4", 1),
        _record("g5", 4),
    ]
    groups = pack_records(records, 5)
    assert [[r.record_id for r in g] for g in groups] == [["g1"], ["g2"], ["g3"], ["g4", "g5"]]
    # order kept, every record exactly once, each group's total under the cap unless alone
    assert [r.record_id for g in groups for r in g] == [r.record_id for r in records]
    for g in groups:
        total = sum(len(r.snippets) for r in g)
        assert total <= 5 or len(g) == 1
    assert pack_records(records, 100) == [records]
    assert pack_records([], 5) == [[]]
    with pytest.raises(ValueError, match="max_entries_per_request"):
        pack_records(records, 0)


def test_retry_records_keep_bundle_order_and_refuse_unknown_ids():
    records = [_record("g1", 1), _record("g2", 1), _record("g3", 1)]
    assert [
        r.record_id
        for r in retry_records_of_chunk(SUBJECT, _Field(), "0:10", records, ["g3", "g1"])
    ] == [
        "g1",
        "g3",
    ]
    with pytest.raises(ValueError, match="not among the chunk's fold records"):
        retry_records_of_chunk(SUBJECT, _Field(), "0:10", records, ["g9"])


# --- requests --------------------------------------------------------------------------------


def test_context_carries_the_text_the_manufacturer_and_both_blocks():
    records = [_record("g1", 2), _record("g2", 1, focal="Brass")]
    message = render_synthesis_context("the chunk's page text", "Acme Inc", records)
    assert message.startswith(
        "text scraped from a manufacturer's website:\nthe chunk's page text\n\n"
    )
    assert "\nthe name of the manufacturer in question: Acme Inc\n" in message
    assert sent_record_ids_from_user_message(message) == ["g1", "g2"]
    assert sent_records_from_user_message(message) == group_digest_payload(records)
    assert (sent_records_from_user_message(message) or [])[0]["focal_form"] == "Aluminum"
    # snippets are bare strings since the merge — no location, no nulls
    assert '"location"' not in message and "null" not in message


def test_request_carries_the_strict_schema_and_the_dummy_is_pre_answered():
    req = create_synthesis_gpt_request(
        deferred_at=T0,
        subject_unique_id=SUBJECT,
        request_id="r1",
        wire_text="the chunk's page text",
        subject_name="Acme",
        records=[_record("g1", 1)],
        phrase_synthesis_prompt=Prompt(text="P", s3_version_id="v", name="p", num_tokens=1),
        gpt_model=LLM_Model(name="gpt-4.1", max_context_tokens=128000),
        model_params=GPTModelParams.with_defaults(),
        eager=True,
    )
    assert req.request.custom_id == "r1" and req.response is None
    assert "phrase_synthesis" in json.dumps(req.request.body.model_dump(mode="json"))
    dummy = create_dummy_completed_synthesis_request(
        deferred_at=T0,
        subject_unique_id=SUBJECT,
        request_id="r0",
        model_params=GPTModelParams.with_defaults(),
        eager=True,
    )
    assert dummy.response is not None
    assert dummy.request.body.model == NO_MODEL.name or dummy.request.body.model == NO_MODEL
    assert json.loads(dummy.response.result or "") == {"syntheses": []}


# --- parse + hold ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parse_holds_exactly_drops_unknown_ids_and_leaves_missing_ones():
    records = [_record("g1", 1), _record("g2", 1)]
    message = render_synthesis_context("text", "Acme", records)
    completed = {
        "r1": _request(_answer({"g2": "two", "gzzzzzz": "never sent"}), message),
    }
    sent, held, unknown = await parse_synthesis_group_result(SUBJECT, _Field(), "r1", completed, T0)
    assert sent == ["g1", "g2"]
    assert {rid: a.synthesis for rid, a in held.items()} == {"g2": "two"}
    assert unknown == ["gzzzzzz"]


@pytest.mark.asyncio
async def test_chunk_answer_merges_groups_then_retry_and_reports_what_is_still_missing():
    g1 = [_record("g1", 1), _record("g2", 1)]
    g2 = [_record("g3", 1)]
    bundle = LLMPhraseExtractionRequestBundle(
        llm_phrase_synthesis_req_ids=["r1", "r2"],
        llm_phrase_synthesis_retry_record_ids=["g2", "g3"],
        llm_phrase_synthesis_retry_req_ids=["r1-retry"],
    )
    completed = {
        "r1": _request(_answer({"g1": "one"}), render_synthesis_context("t", "Acme", g1)),
        "r2": _request(_answer({}), render_synthesis_context("t", "Acme", g2)),
        "r1-retry": _request(
            _answer({"g2": "two (retry)", "g1": "duplicate, ignored"}),
            render_synthesis_context("t", "Acme", [g1[1], g2[0]]),
        ),
    }
    assessment = await get_chunk_syntheses(
        SUBJECT, _Field(), "0:10", bundle, completed, T0, include_retry=False
    )
    assert assessment.sent_ids == ["g1", "g2", "g3"]
    assert assessment.missing_ids == ["g2", "g3"] and assessment.retried_record_ids == []
    full = await get_chunk_syntheses(SUBJECT, _Field(), "0:10", bundle, completed, T0)
    assert {rid: a.synthesis for rid, a in full.syntheses.items()} == {
        "g1": "one",
        "g2": "two (retry)",
    }
    assert full.missing_ids == ["g3"]  # asked twice, never answered: reported, not retried again
    assert full.retried_record_ids == ["g2", "g3"]
    assert isinstance(full, ChunkAnswer)


@pytest.mark.asyncio
async def test_chunk_answer_refuses_a_chunk_with_no_embedded_requests():
    with pytest.raises(ValueError, match="no synthesis requests embedded"):
        await get_chunk_syntheses(
            SUBJECT, _Field(), "0:10", LLMPhraseExtractionRequestBundle(), {}, T0
        )
