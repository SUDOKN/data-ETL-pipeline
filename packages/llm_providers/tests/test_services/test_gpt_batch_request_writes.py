"""What the upsert actually writes.

The bug this guards against: ``source_texts`` was set on the in-memory
``GPTBatchRequest`` but absent from the upsert's hand-written field list, so it
never reached Mongo. Nothing failed at write time — the loss only surfaced a
stage later, as a ``None`` summary that made every evidence quote citing it
invalid. That field is gone now (the summaries travel in the request context
instead), but the shape of the mistake is still available to any field added to
the model later.
"""

from datetime import UTC, datetime

from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.models.open_ai.gpt_batch_request_blob import GPTBatchRequestBlob
from llm_providers.models.open_ai.gpt_request_body import GPTRequestBody
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    build_request_body_update_document,
)

# Beanie exposes the Mongo _id under both of these; neither is ours to write.
_BEANIE_INTERNAL = {"id", "revision_id"}


def _batch_request() -> GPTBatchRequest:
    """``model_construct`` rather than the constructor: Beanie 2.0's
    ``Document.__init__`` reaches for the collection, so a plain
    ``GPTBatchRequest(...)`` needs a live database. Nothing here is testing
    validation, so skipping it costs nothing."""
    now = datetime.now(UTC)
    return GPTBatchRequest.model_construct(
        created_at=now,
        updated_at=now,
        subject_unique_id="anchor-mfg.com",
        batch_id="Eager",
        num_batches_paired_with=0,
        request=GPTBatchRequestBlob(
            custom_id="anchor-mfg.com>products>llm_phrase_relationship_screening>group>0",
            body=GPTRequestBody(
                model="gpt-4.1",
                messages=[
                    {"role": "system", "content": "screen these"},
                    {"role": "user", "content": "extracted phrases: ..."},
                ],
                max_completion_tokens=20_000,
                temperature=0.0,
                top_p=1.0,
                presence_penalty=0.0,
                frequency_penalty=0.0,
                response_format={"type": "json_object"},
            ),
            input_tokens=11,
        ),
    )


def _written_fields(update_document: dict) -> set[str]:
    """Top-level model field behind every key the update touches, so that a
    dotted path like ``request.body`` counts as coverage of ``request``."""
    return {
        key.split(".", 1)[0] for section in update_document.values() for key in section
    }


def test_the_upsert_writes_every_persisted_field():
    """A field on the model that this update never mentions is a field that
    silently does not survive the round trip."""
    update_document = build_request_body_update_document(_batch_request())

    persisted = set(GPTBatchRequest.model_fields) - _BEANIE_INTERNAL
    assert _written_fields(update_document) == persisted


def test_the_request_body_is_refreshed_rather_than_write_once():
    """It is derived from current pipeline state, so re-creating a request has to
    be able to correct it on a document that already exists."""
    update_document = build_request_body_update_document(_batch_request())

    assert "request.body" in update_document["$set"]
    assert "request.body" not in update_document["$setOnInsert"]
