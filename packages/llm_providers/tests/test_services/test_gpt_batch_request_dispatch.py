"""What the eager dispatch actually puts on the wire.

The bug this guards against, from 2026-08-18: ``dispatch_gpt_batch_request``
took ``model_params`` as an argument while the node had written its own strict
``response_format`` onto ``request.body`` via ``with_response_format``. Every
caller passed ``metadata.<stage>.model_params`` instead, whose response_format
was the pipeline-wide ``{"type": "json_object"}``, so the catalog-generated
schema was stored in Mongo and never sent. Nothing failed at dispatch — the loss
surfaced a stage later, as a freehand-grounding response that answered a
`condition` rule in a `quality` rule's vocabulary and failed parse, aborting the
whole subject after the tokens had been spent.

The batch path sends ``request.body`` verbatim, so an eager dispatch that sends
anything else is also an eager/batch divergence.
"""

from datetime import UTC, datetime

import pytest

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.models.llm_model import GPT_4_1, GPT_4o_mini
from llm_providers.models.open_ai.gpt_batch_request_blob import GPTBatchRequestBlob
from llm_providers.models.open_ai.gpt_request_body import GPTRequestBody
from llm_providers.services.gpt_batch_request import gpt_batch_request_service
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)

STRICT_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "equipment_phrase_freehand_grounding_result",
        "strict": True,
        "schema": {"type": "object", "additionalProperties": False},
    },
}


def _batch_request(
    *, model: str = GPT_4_1.name, response_format: dict = STRICT_SCHEMA
) -> GPTBatchRequest:
    """``model_construct`` rather than the constructor: Beanie 2.0's
    ``Document.__init__`` reaches for the collection, so a plain
    ``GPTBatchRequest(...)`` needs a live database."""
    now = datetime.now(UTC)
    return GPTBatchRequest.model_construct(
        created_at=now,
        updated_at=now,
        subject_unique_id="anchor-mfg.com",
        batch_id="Eager",
        num_batches_paired_with=0,
        response=None,
        response_parse_errors=[],
        request=GPTBatchRequestBlob(
            custom_id="anchor-mfg.com>equipments>llm_phrase_freehand_grounding>group>0",
            body=GPTRequestBody(
                model=model,
                messages=[
                    {"role": "system", "content": "ground these"},
                    {"role": "user", "content": "phrases: ..."},
                ],
                max_completion_tokens=20_000,
                temperature=0.0,
                top_p=1.0,
                presence_penalty=0.0,
                frequency_penalty=0.0,
                seed=12345,
                response_format=response_format,
            ),
            input_tokens=11,
        ),
    )


@pytest.fixture
def sent(monkeypatch):
    """Captures the kwargs dispatch hands the OpenAI call, without making one."""
    captured: dict = {}

    async def _fake_fetch(**kwargs):
        captured.update(kwargs)
        return "response-blob-sentinel"

    monkeypatch.setattr(
        gpt_batch_request_service, "fetch_gpt_batch_response", _fake_fetch
    )
    return captured


@pytest.mark.asyncio
async def test_dispatch_sends_the_bodys_strict_schema(sent):
    """The node-owned schema on the body reaches the call."""
    await dispatch_gpt_batch_request(
        gpt_batch_request=_batch_request(), gpt_model=GPT_4_1
    )
    assert sent["model_params"].response_format == STRICT_SCHEMA


@pytest.mark.asyncio
async def test_dispatch_sends_the_bodys_sampling_params(sent):
    """Everything else on the body travels with it, not just the schema."""
    await dispatch_gpt_batch_request(
        gpt_batch_request=_batch_request(), gpt_model=GPT_4_1
    )
    params = sent["model_params"]
    assert (params.temperature, params.seed, params.max_completion_tokens) == (
        0.0,
        12345,
        20_000,
    )


@pytest.mark.asyncio
async def test_dispatch_sends_the_bodys_messages(sent):
    """Prompt and context come off the same body as the params."""
    await dispatch_gpt_batch_request(
        gpt_batch_request=_batch_request(), gpt_model=GPT_4_1
    )
    assert sent["prompt"] == "ground these"
    assert sent["context"] == "phrases: ..."


@pytest.mark.asyncio
async def test_dispatch_rejects_a_model_the_body_was_not_built_for(sent):
    """The body is what gets sent, so a caller cannot redirect it to another
    model — its token budget was checked against the one it names."""
    with pytest.raises(ValueError, match="but dispatch was handed"):
        await dispatch_gpt_batch_request(
            gpt_batch_request=_batch_request(), gpt_model=GPT_4o_mini
        )
    assert not sent
