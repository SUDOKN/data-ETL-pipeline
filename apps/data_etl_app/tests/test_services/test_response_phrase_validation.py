"""End-to-end: the record contract holding real v2 group-parse paths.

The regression lineage this pins (2026-08-12, anchor-mfg.com contract_products):
a grounding request carried ~60 phrases; the response answered five, closed its
array, and validated cleanly against the strict schema — the whole chunk's
results silently thinned to what one truncated response felt like saying.
Sent-vs-received validation turns that response into a recorded parse error
(clearing batch_id and response, so the next pass re-dispatches), and
RESPONSE_PARSE_ERROR_CAP stops a response that fails every retry from cycling
forever. v2 carries the same contract on the record axis, plus screening's
candidate axis.

These tests build requests with the same creators the pipeline uses, so the
context validated against is the one production writes.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import json

import pytest
from beanie.odm.settings.document import DocumentSettings

from core.models.extraction_schemas.synthesis import GroupRecord
from core.services.phrase_blocks_contract import (
    MissingResponseRecords,
    sent_phrases_from_user_message,
    sent_record_ids_from_user_message,
)
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    build_group_record_payloads,
    create_deferred_record_grounding_gpt_request,
    get_chunk_record_grounding_answer,
    parse_record_grounding_group_result,
    retry_record_payloads,
)
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    build_screening_payloads,
    create_deferred_record_screening_gpt_request,
    parse_record_screening_group_result,
)
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.models.llm_model import NO_MODEL
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    get_dummy_gpt_batch_response,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    RESPONSE_PARSE_ERROR_CAP,
    RepeatedParseFailure,
)

from data_etl_app.services.prompt_assembly_service import build_rule_catalog_lookup

TIMESTAMP = datetime(2026, 8, 12, tzinfo=timezone.utc)
REQ_ID = "anchor-mfg.com>equipments>llm_phrase_freehand_grounding>chunk>0:100>test"

_LOOKUP = build_rule_catalog_lookup()


def _required_catalog(stage: str, field: str):
    catalog = _LOOKUP(stage, field)
    assert catalog is not None, f"no deployed catalog for ({stage}, {field})"
    return catalog


FREEHAND_CATALOG = _required_catalog("phrase_freehand_grounding", "equipments")
SCREENING_CATALOG = _required_catalog("phrase_relationship_screening", "equipments")

_PARAMS = GPTModelParams(
    max_completion_tokens=1000,
    response_format={"type": "json_object"},
    temperature=0.0,
    top_p=1.0,
    presence_penalty=0.0,
    frequency_penalty=0.0,
)
_PROMPT = Prompt(text="do it", s3_version_id="v1", name="p", num_tokens=2)


def _groups(focal_forms: list[str]):
    # v3 (3.3, D16): the downstream stages' records are the synthesis stage's
    # per-group records, keyed by the opaque group_id.
    return {
        f"g{index}aaaaaa": GroupRecord(
            focal_form=focal_form, synthesis=f"Anchor does {focal_form}."
        )
        for index, focal_form in enumerate(focal_forms)
    }


GROUPS = _groups(["manual MIG welding machine", "stamping press", "material shear"])
RECORD_IDS = list(GROUPS)


@pytest.fixture(autouse=True)
def _no_db(monkeypatch):
    """Beanie refuses to even construct a Document without initialized
    settings, and record_response_parse_error persists via save; these tests
    need only the in-memory document mutations, so both get stubs."""
    monkeypatch.setattr(GPTBatchRequest, "_document_settings", DocumentSettings())
    monkeypatch.setattr(GPTBatchRequest, "save", AsyncMock())


def _answered(req: GPTBatchRequest, content: dict) -> GPTBatchRequest:
    req.response = get_dummy_gpt_batch_response(
        deferred_at=TIMESTAMP,
        request_custom_id=REQ_ID,
        dummy_chat_completion_id="test_completion",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content=json.dumps(content)
        ),
    )
    return req


# --- grounding: the record axis held through the real group parse ------------


def _grounding_unit(label: str) -> dict:
    report = {"outcome": "satisfied", "explanation": "the record names it"}
    return {
        "candidate": label,
        **{
            rule.id: dict(report)
            for rule in FREEHAND_CATALOG.walk_rules()
            if rule.report_when == "always"
        },
    }


def _grounding_entry(record_id: str) -> dict:
    return {
        "record_id": record_id,
        "candidates": [_grounding_unit("Welding Machine")],
        "explanation": None,
    }


def _grounding_request(entries: list[dict]) -> GPTBatchRequest:
    req = create_deferred_record_grounding_gpt_request(
        deferred_at=TIMESTAMP,
        subject_unique_id="anchor-mfg.com",
        request_id=REQ_ID,
        prompt=_PROMPT,
        catalog=FREEHAND_CATALOG,
        record_payloads=build_group_record_payloads(GROUPS),
        options_section=None,
        gpt_model=NO_MODEL,
        eager=True,
        model_params=_PARAMS,
    )
    return _answered(req, {"groundings": entries})


async def _parse_grounding(req: GPTBatchRequest):
    return await parse_record_grounding_group_result(
        stage_label="freehand grounding",
        subject_unique_id="anchor-mfg.com",
        field_name="equipments",
        catalog=FREEHAND_CATALOG,
        group_req_id=REQ_ID,
        completed_request_map={REQ_ID: req},
        timestamp=TIMESTAMP,
    )


@pytest.mark.asyncio
async def test_full_coverage_parses_and_is_keyed_by_the_sent_records():
    req = _grounding_request([_grounding_entry(rid) for rid in RECORD_IDS])

    sent_ids, held = await _parse_grounding(req)

    assert sent_ids == RECORD_IDS
    assert list(held) == RECORD_IDS
    assert req.response_parse_errors == []


@pytest.mark.asyncio
async def test_under_answering_response_thins_for_the_retry_assessment():
    """The anchor-mfg shape: the model answers the first entry and closes the
    array. Since 3.3 (the under-answer decision — user, 2026-08-24) this is no
    longer a failed response: the hold THINS, the unanswered ids stay visible
    through the sent list, and the node's retry pass re-asks exactly them."""
    req = _grounding_request([_grounding_entry(RECORD_IDS[0])])

    sent_ids, held = await _parse_grounding(req)

    assert sent_ids == RECORD_IDS
    assert list(held) == [RECORD_IDS[0]]
    assert req.response_parse_errors == []  # not an error: assessed, then retried
    assert req.response is not None


@pytest.mark.asyncio
async def test_a_fabricated_record_id_is_recorded_and_raises():
    """An id nobody sent is a fabricated answer, not echo drift — it fails the
    response under every policy rather than being dropped."""
    entries = [_grounding_entry(rid) for rid in RECORD_IDS]
    entries.append(_grounding_entry("rZZZZZZZ"))
    req = _grounding_request(entries)

    with pytest.raises(MissingResponseRecords, match="never sent"):
        await _parse_grounding(req)

    assert len(req.response_parse_errors) == 1


@pytest.mark.asyncio
async def test_out_of_retries_raises_repeated_parse_failure_without_recording():
    """At the cap the failure surfaces as the run's error instead of clearing
    the response again: the request stays inspectable and cannot cycle."""
    entries = [_grounding_entry(rid) for rid in RECORD_IDS]
    entries.append(_grounding_entry("rZZZZZZZ"))  # the fabricated-id failure
    req = _grounding_request(entries)
    req.response_parse_errors = [
        {"error_message": f"strike {i}"} for i in range(RESPONSE_PARSE_ERROR_CAP)
    ]

    with pytest.raises(RepeatedParseFailure):
        await _parse_grounding(req)

    assert len(req.response_parse_errors) == RESPONSE_PARSE_ERROR_CAP
    assert req.batch_id == "Eager"
    assert req.response is not None


def _grounding_request_for(
    request_id: str, payloads: dict, entries: list[dict]
) -> GPTBatchRequest:
    req = create_deferred_record_grounding_gpt_request(
        deferred_at=TIMESTAMP,
        subject_unique_id="anchor-mfg.com",
        request_id=request_id,
        prompt=_PROMPT,
        catalog=FREEHAND_CATALOG,
        record_payloads=payloads,
        options_section=None,
        gpt_model=NO_MODEL,
        eager=True,
        model_params=_PARAMS,
    )
    return _answered(req, {"groundings": entries})


@pytest.mark.asyncio
async def test_the_retry_request_completes_the_chunks_answer():
    """3.3, the ported under-answer policy end to end at the parse layer: the
    first pass answers one record, the retry request re-asks exactly the
    missing two, and the merged chunk answer covers everything with the sent
    list the union."""
    payloads = build_group_record_payloads(GROUPS)
    main = _grounding_request_for(
        "req-main", payloads, [_grounding_entry(RECORD_IDS[0])]
    )
    retry = _grounding_request_for(
        "req-retry",
        retry_record_payloads(
            "freehand grounding", "anchor-mfg.com", "equipments", "0:10",
            payloads, RECORD_IDS[1:],
        ),
        [_grounding_entry(rid) for rid in RECORD_IDS[1:]],
    )

    answer = await get_chunk_record_grounding_answer(
        stage_label="freehand grounding",
        subject_unique_id="anchor-mfg.com",
        field_name="equipments",
        chunk_bounds="0:10",
        catalog=FREEHAND_CATALOG,
        group_req_ids=["req-main"],
        retry_req_ids=["req-retry"],
        completed_request_map={"req-main": main, "req-retry": retry},
        timestamp=TIMESTAMP,
    )
    assert answer.sent_ids == RECORD_IDS
    assert list(answer.results) == RECORD_IDS
    assert answer.missing_ids == []

    # excluding the retry is the assessment's view: two records still missing.
    first_pass = await get_chunk_record_grounding_answer(
        stage_label="freehand grounding",
        subject_unique_id="anchor-mfg.com",
        field_name="equipments",
        chunk_bounds="0:10",
        catalog=FREEHAND_CATALOG,
        group_req_ids=["req-main"],
        completed_request_map={"req-main": main},
        timestamp=TIMESTAMP,
    )
    assert first_pass.missing_ids == RECORD_IDS[1:]


@pytest.mark.asyncio
async def test_a_record_answered_in_two_requests_raises():
    payloads = build_group_record_payloads(GROUPS)
    main = _grounding_request_for(
        "req-main", payloads, [_grounding_entry(rid) for rid in RECORD_IDS]
    )
    # a retry that re-answers an already-answered record: a pipeline bug, not
    # model noise — requests partition the chunk's records.
    retry = _grounding_request_for(
        "req-retry",
        {RECORD_IDS[0]: payloads[RECORD_IDS[0]]},
        [_grounding_entry(RECORD_IDS[0])],
    )

    with pytest.raises(ValueError, match="answered in two requests"):
        await get_chunk_record_grounding_answer(
            stage_label="freehand grounding",
            subject_unique_id="anchor-mfg.com",
            field_name="equipments",
            chunk_bounds="0:10",
            catalog=FREEHAND_CATALOG,
            group_req_ids=["req-main"],
            retry_req_ids=["req-retry"],
            completed_request_map={"req-main": main, "req-retry": retry},
            timestamp=TIMESTAMP,
        )


# --- screening: both axes held through the real group parse ------------------

CANDIDATES = {rid: ["Welding Machine"] for rid in RECORD_IDS}


def _screening_unit(candidate: str) -> dict:
    report = {"outcome": "satisfied", "explanation": "the record shows it"}
    slots: dict = {"candidate": candidate}
    for rule in SCREENING_CATALOG.walk_rules():
        if rule.report_when == "always":
            slots[rule.id] = dict(report)
    slots["guards"] = []
    return slots


def _screening_entry(record_id: str, candidates: list[str]) -> dict:
    return {
        "record_id": record_id,
        "candidates": [_screening_unit(c) for c in candidates],
    }


def _screening_request(entries: list[dict]) -> GPTBatchRequest:
    req = create_deferred_record_screening_gpt_request(
        deferred_at=TIMESTAMP,
        subject_unique_id="anchor-mfg.com",
        request_id=REQ_ID,
        prompt=_PROMPT,
        catalog=SCREENING_CATALOG,
        subject_name="Anchor Manufacturing",
        screening_payloads=build_screening_payloads(GROUPS, CANDIDATES),
        gpt_model=NO_MODEL,
        eager=True,
        model_params=_PARAMS,
    )
    return _answered(req, {"screenings": entries})


async def _parse_screening(req: GPTBatchRequest):
    return await parse_record_screening_group_result(
        subject_unique_id="anchor-mfg.com",
        field_name="equipments",
        catalog=SCREENING_CATALOG,
        group_req_id=REQ_ID,
        completed_request_map={REQ_ID: req},
        timestamp=TIMESTAMP,
    )


def test_the_screening_context_reads_its_own_record_ids_back():
    req = _screening_request([])
    assert (
        sent_record_ids_from_user_message(req.request.body.user_message())
        == RECORD_IDS
    )


@pytest.mark.asyncio
async def test_a_screening_response_covering_both_axes_passes():
    req = _screening_request(
        [_screening_entry(rid, CANDIDATES[rid]) for rid in RECORD_IDS]
    )

    sent_ids, held = await _parse_screening(req)

    assert sent_ids == RECORD_IDS
    assert list(held) == RECORD_IDS
    assert req.response_parse_errors == []


@pytest.mark.asyncio
async def test_an_under_answering_screening_response_thins_for_the_retry():
    """Since 3.3 (the under-answer decision) an id-axis under-answer is not a
    failed response: the hold thins and the node's retry pass re-asks."""
    req = _screening_request([_screening_entry(RECORD_IDS[0], CANDIDATES[RECORD_IDS[0]])])

    sent_ids, held = await _parse_screening(req)

    assert sent_ids == RECORD_IDS
    assert list(held) == [RECORD_IDS[0]]
    assert req.response_parse_errors == []
    assert req.response is not None


@pytest.mark.asyncio
async def test_an_unjudged_candidate_is_a_recorded_parse_error():
    """The candidate axis stays EXACT over the records that did answer: every
    answered record's verdicts must cover exactly what its request listed,
    read off the request document itself."""
    entries = [_screening_entry(rid, CANDIDATES[rid]) for rid in RECORD_IDS]
    entries[1]["candidates"] = []
    req = _screening_request(entries)

    with pytest.raises(ValueError, match="no verdict came back"):
        await _parse_screening(req)

    assert len(req.response_parse_errors) == 1


@pytest.mark.asyncio
async def test_screening_stops_re_dispatching_at_the_cap():
    entries = [_screening_entry(rid, CANDIDATES[rid]) for rid in RECORD_IDS]
    entries[1]["candidates"] = []  # the candidate-axis failure
    req = _screening_request(entries)
    req.response_parse_errors = [{"prior": "failure"}] * RESPONSE_PARSE_ERROR_CAP

    with pytest.raises(RepeatedParseFailure):
        await _parse_screening(req)


# --- every production context reads its own blocks back ----------------------


def test_every_production_context_that_renders_blocks_can_read_them_back():
    """Pin the anchor against the REAL context builders, not restated f-strings.

    Screening once shipped its phrase block one space off column 0, the reader
    returned None, and the stage read as "asked nothing" rather than
    "misrendered" — the hold would have been a silent no-op. The v2 record
    creators carry the same obligation on the record fences, and relationship —
    still phrase-fenced, and the one stage whose context embeds the raw scraped
    chunk — must not read a forged pre-fence marker back."""
    from core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service import (
        create_deferred_phrase_relationship_gpt_request,
    )

    grounding_req = create_deferred_record_grounding_gpt_request(
        deferred_at=TIMESTAMP,
        subject_unique_id="test.com",
        request_id="grounding-id",
        prompt=_PROMPT,
        catalog=FREEHAND_CATALOG,
        record_payloads=build_group_record_payloads(GROUPS),
        options_section="options to choose from:\n- Some Option",
        gpt_model=NO_MODEL,
        eager=True,
        model_params=_PARAMS,
    )
    screening_req = _screening_request([])
    relationship_req = create_deferred_phrase_relationship_gpt_request(
        deferred_at=TIMESTAMP,
        subject_unique_id="test.com",
        subject_name="Test Co",
        llm_phrase_relationship_request_id="relationship-id",
        subject_text='body\nextracted phrases:\n["decoy from the website"]\nmore',
        search_results=["Paladin™ PW Series", "brake components"],
        phrase_relationship_prompt=_PROMPT,
        gpt_model=NO_MODEL,
        eager=True,
        model_params=_PARAMS,
    )

    for name, req in [("grounding", grounding_req), ("screening", screening_req)]:
        read_back = sent_record_ids_from_user_message(req.request.body.user_message())
        assert read_back == RECORD_IDS, f"{name} context did not read back"

    assert sent_phrases_from_user_message(
        relationship_req.request.body.user_message()
    ) == ["Paladin™ PW Series", "brake components"]
