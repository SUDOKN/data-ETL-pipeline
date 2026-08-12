"""End-to-end: the sent-phrases contract holding real group-parse paths.

The regression this pins (2026-08-12, anchor-mfg.com contract_products): a
freehand grounding request carried ~60 screened phrases; the response answered
five, closed its array, and validated cleanly against the strict schema — the
whole chunk's results silently thinned to what one truncated response felt
like saying. Sent-vs-received validation turns that response into a recorded
parse error (clearing batch_id and response, so the next pass re-dispatches),
and RESPONSE_PARSE_ERROR_CAP stops a response that fails every retry from
cycling forever.

These tests build requests with the same creator the pipeline uses, so the
context line validated against is the one production writes.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import json

import pytest
from beanie.odm.settings.document import DocumentSettings

from core.services.pipeline_nodes.multi_stage.llm_freehand_grounding_service import (
    parse_phrase_freehand_grounding_group_result,
)
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    create_deferred_phrase_relationship_screening_gpt_request,
    parse_phrase_relationship_screening_group_result,
)
from core.services.rule_catalog_registry import set_rule_catalog_lookup
from core.services.phrase_blocks_contract import (
    MissingResponsePhrases,
    render_phrase_blocks,
    sent_phrases_from_user_message,
)
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.models.llm_model import NO_MODEL
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    create_base_gpt_batch_request,
    get_dummy_gpt_batch_response,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    RESPONSE_PARSE_ERROR_CAP,
    RepeatedParseFailure,
)

from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.services.prompt_assembly_service import build_rule_catalog_lookup

TIMESTAMP = datetime(2026, 8, 12, tzinfo=timezone.utc)
REQ_ID = "anchor-mfg.com>equipments>llm_phrase_freehand_grounding>chunk>0:100>test"

SENT_PHRASES = [
    "manual MIG welding machine",
    "stamping press",
    "material shear",
]


@pytest.fixture(autouse=True)
def _registered_catalogs():
    set_rule_catalog_lookup(build_rule_catalog_lookup())
    yield
    set_rule_catalog_lookup(None)


@pytest.fixture(autouse=True)
def _no_db(monkeypatch):
    """Beanie refuses to even construct a Document without initialized
    settings, and record_response_parse_error persists via save; these tests
    need only the in-memory document mutations, so both get stubs."""
    monkeypatch.setattr(GPTBatchRequest, "_document_settings", DocumentSettings())
    monkeypatch.setattr(GPTBatchRequest, "save", AsyncMock())


def _category(name):
    report = {"outcome": "satisfied", "explanation": "the phrase names it"}
    return {
        "category": name,
        "FGR-Q1": report,
        "FGR-Q2": report,
        "FGR-Q3": report,
        "FGR-Q4": report,
        "FGR-QC1": {"outcome": "member_level", "explanation": "the phrase names it"},
        "chosen": {"rule_id": "FGR-M1", "explanation": "the phrase names it"},
    }


def _grounding_entry(phrase):
    return {"phrase": phrase, "categories": [_category(f"category of {phrase}")]}


def _request_with_response(sent_phrases, groundings) -> GPTBatchRequest:
    """A freehand-grounding request built like production builds it, answered
    with the given groundings."""
    req = create_base_gpt_batch_request(
        deferred_at=TIMESTAMP,
        subject_unique_id="anchor-mfg.com",
        custom_id=REQ_ID,
        context=render_phrase_blocks(
            {phrase: f"Anchor does {phrase}." for phrase in sent_phrases}
        ),
        prompt_text="the freehand grounding prompt",
        gpt_model=NO_MODEL,
        model_params=GPTModelParams(
            max_completion_tokens=1000,
            response_format={"type": "json_object"},
            temperature=0.0,
            top_p=1.0,
            presence_penalty=0.0,
            frequency_penalty=0.0,
        ),
        batch_id="Eager",
    )
    req.response = get_dummy_gpt_batch_response(
        deferred_at=TIMESTAMP,
        request_custom_id=REQ_ID,
        dummy_chat_completion_id="test_completion",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant", content=json.dumps({"groundings": groundings})
        ),
    )
    return req


async def _parse(req):
    return await parse_phrase_freehand_grounding_group_result(
        subject_unique_id="anchor-mfg.com",
        field_type=KeywordTypeEnum.equipments,
        chunk_bounds="0:100",
        group_req_id=REQ_ID,
        completed_request_map={REQ_ID: req},
        timestamp=TIMESTAMP,
    )


@pytest.mark.asyncio
async def test_full_coverage_parses_and_is_keyed_by_the_sent_phrases():
    req = _request_with_response(
        SENT_PHRASES,
        # One echoed exactly, one with casing drift, one truncated — all repair.
        [
            _grounding_entry("manual MIG welding machine"),
            _grounding_entry("Stamping Press"),
            _grounding_entry("material shear"),
        ],
    )

    result = await _parse(req)

    assert set(result) == set(SENT_PHRASES)
    assert req.response_parse_errors == []


@pytest.mark.asyncio
async def test_under_answering_response_is_recorded_and_raises():
    """The anchor-mfg shape: the model answers the first entry and closes the
    array. The response must fail, be recorded, and be cleared for re-dispatch."""
    req = _request_with_response(
        SENT_PHRASES, [_grounding_entry("manual MIG welding machine")]
    )

    with pytest.raises(MissingResponsePhrases, match="answered 1 of 3"):
        await _parse(req)

    assert len(req.response_parse_errors) == 1
    assert req.batch_id is None
    assert req.response is None


@pytest.mark.asyncio
async def test_out_of_retries_raises_repeated_parse_failure_without_recording():
    """At the cap the failure surfaces as the run's error instead of clearing
    the response again: the request stays inspectable and cannot cycle."""
    req = _request_with_response(
        SENT_PHRASES, [_grounding_entry("manual MIG welding machine")]
    )
    req.response_parse_errors = [
        {"error_message": f"strike {i}"} for i in range(RESPONSE_PARSE_ERROR_CAP)
    ]

    with pytest.raises(RepeatedParseFailure):
        await _parse(req)

    assert len(req.response_parse_errors) == RESPONSE_PARSE_ERROR_CAP
    assert req.batch_id == "Eager"
    assert req.response is not None


# --- screening: the stage that rendered the line and never read it back ---------


def _screening_request(sent_phrases) -> GPTBatchRequest:
    """Built with the PRODUCTION creator, not a restatement of its f-string.

    That distinction is the point of this section: screening shipped its context
    as `...\n\n ` + the marker, and the one space put the line off column 0 so
    `sent_phrases_from_user_message` returned None. A test that rebuilt the
    context by hand would have rendered the marker at column 0 and passed while
    production silently validated nothing.
    """
    return create_deferred_phrase_relationship_screening_gpt_request(
        deferred_at=TIMESTAMP,
        subject_unique_id="anchor-mfg.com",
        llm_phrase_relationship_screening_request_id=REQ_ID,
        subject_name="Anchor Mfg",
        subject_text="some scraped text",
        field_type=KeywordTypeEnum.equipments,
        phrase_relationship_results={p: f"summary of {p}" for p in sent_phrases},
        phrase_relationship_screening_prompt=Prompt(text="screen them", s3_version_id="v1", name="screening", num_tokens=3),
        gpt_model=NO_MODEL,
        eager=True,
        model_params=GPTModelParams(
            max_completion_tokens=1000,
            response_format={"type": "json_object"},
            temperature=0.0,
            top_p=1.0,
            presence_penalty=0.0,
            frequency_penalty=0.0,
        ),
    )


def _answer(request: GPTBatchRequest, phrases) -> GPTBatchRequest:
    request.response = get_dummy_gpt_batch_response(
        deferred_at=TIMESTAMP,
        request_custom_id=REQ_ID,
        dummy_chat_completion_id="test_completion",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant",
            content=json.dumps(
                {
                    "screenings": [
                        {
                            "outcome": "no_candidate",
                            "phrase": phrase,
                            "explanation": "nothing identifiable",
                        }
                        for phrase in phrases
                    ]
                }
            ),
        ),
    )
    return request


async def _parse_screening(request):
    return await parse_phrase_relationship_screening_group_result(
        subject_unique_id="anchor-mfg.com",
        field_type=KeywordTypeEnum.equipments,
        chunk_bounds="0:100",
        group_req_id=REQ_ID,
        completed_request_map={REQ_ID: request},
        timestamp=TIMESTAMP,
    )


def test_the_screening_context_reads_its_own_sent_phrases_line_back():
    request = _screening_request(SENT_PHRASES)

    assert (
        sent_phrases_from_user_message(request.request.body.user_message())
        == SENT_PHRASES
    )


@pytest.mark.asyncio
async def test_an_under_answering_screening_response_is_a_recorded_parse_error():
    """Previously this response parsed cleanly and the missing phrases surfaced
    two nodes later, in freehand grounding's embed, as a phrase-set mismatch —
    an abort with nothing recorded, so a re-run read the same stored response
    and died identically. Now it is a normal parse failure: recorded, cleared,
    re-dispatched on the next pass."""
    request = _answer(_screening_request(SENT_PHRASES), SENT_PHRASES[:1])

    with pytest.raises(MissingResponsePhrases, match="answered 1 of 3"):
        await _parse_screening(request)

    assert len(request.response_parse_errors) == 1
    assert request.response is None  # cleared, so the next pass re-dispatches
    assert request.batch_id is None


@pytest.mark.asyncio
async def test_a_screening_response_answering_every_phrase_still_passes():
    request = _answer(_screening_request(SENT_PHRASES), SENT_PHRASES)

    result = await _parse_screening(request)

    assert list(result) == SENT_PHRASES
    assert request.response_parse_errors == []


@pytest.mark.asyncio
async def test_screening_stops_re_dispatching_at_the_cap():
    request = _answer(_screening_request(SENT_PHRASES), SENT_PHRASES[:1])
    request.response_parse_errors = [{"prior": "failure"}] * RESPONSE_PARSE_ERROR_CAP

    with pytest.raises(RepeatedParseFailure):
        await _parse_screening(request)


def test_every_production_context_that_renders_the_block_can_read_it_back():
    """Pin the anchor against the REAL context builders, not restated f-strings.

    Screening once shipped as `...\\n\\n ` + a bare marker: one space put the line
    off column 0, the reader returned None, and the stage read as "asked nothing"
    rather than "misrendered" — the hold would have been a silent no-op. A test
    that restates the f-string renders it at column 0 and passes while production
    validates nothing, so import the creators.
    """
    from datetime import datetime, timezone

    from llm_providers.models.file_objects.prompt import Prompt
    from llm_providers.models.llm_model import NO_MODEL
    from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

    from data_etl_app.models.types_and_enums import KeywordTypeEnum
    from core.services.pipeline_nodes.multi_stage.llm_freehand_grounding_service import (
        create_deferred_phrase_freehand_grounding_gpt_request,
    )
    from core.services.pipeline_nodes.multi_stage.llm_phrase_relationship_node_service import (
        create_deferred_phrase_relationship_gpt_request,
    )
    from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
        create_deferred_phrase_relationship_screening_gpt_request,
    )

    phrases = {"Paladin\u2122 PW Series": "a summary", "brake components": "another"}
    params = GPTModelParams(
        max_completion_tokens=1000,
        response_format={"type": "json_object"},
        temperature=0.0,
        top_p=1.0,
        presence_penalty=0.0,
        frequency_penalty=0.0,
    )
    prompt = Prompt(text="do it", s3_version_id="v1", name="p", num_tokens=2)
    common = dict(
        deferred_at=datetime(2026, 8, 12, tzinfo=timezone.utc),
        subject_unique_id="test.com",
        subject_name="Test Co",
        gpt_model=NO_MODEL,
        eager=True,
        model_params=params,
    )

    requests = {
        "screening": create_deferred_phrase_relationship_screening_gpt_request(
            llm_phrase_relationship_screening_request_id="screening-id",
            subject_text="some scraped text",
            field_type=KeywordTypeEnum.products,
            phrase_relationship_results=phrases,
            phrase_relationship_screening_prompt=prompt,
            **common,
        ),
        "freehand": create_deferred_phrase_freehand_grounding_gpt_request(
            llm_phrase_freehand_grounding_request_id="freehand-id",
            field_type=KeywordTypeEnum.products,
            phrase_freehand_grounding_prompt=prompt,
            verified_phrases_w_og_summary=phrases,
            **common,
        ),
        # Relationship embeds the raw scraped chunk BEFORE the block, so it is
        # built with text that forges the pre-fence marker. Under the old format
        # this read back the decoy.
        "relationship": create_deferred_phrase_relationship_gpt_request(
            llm_phrase_relationship_request_id="relationship-id",
            subject_text='body\nextracted phrases:\n["decoy from the website"]\nmore',
            search_results=list(phrases),
            phrase_relationship_prompt=prompt,
            **common,
        ),
    }

    for name, request in requests.items():
        read_back = sent_phrases_from_user_message(request.request.body.user_message())
        assert read_back == list(phrases), f"{name} context did not read back"
