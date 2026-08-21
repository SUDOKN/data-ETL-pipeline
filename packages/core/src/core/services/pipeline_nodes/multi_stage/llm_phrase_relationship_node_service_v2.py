"""Pipeline v2's relationship stage: records in, masked records out.

Sibling of the v1 service rather than an edit of it — the v1 parse path keeps
serving the v1 chain until the phase-2.9 flip re-points the node and deletes
it. Signatures mirror the v1 functions they replace so the flip is a drop-in.

The phrase axis is UNCHANGED here: relationship requests still carry the
``<<<PHRASES`` fence and the response is held to it, because this is the stage
where phrase identity is set. Masking happens after the hold — the parse
result is re-keyed by the content-derived record_id, and every stage
downstream of this one speaks record ids only.
"""

from __future__ import annotations

import logging
import traceback
from datetime import datetime
from typing import Optional

from pydantic import ValidationError

from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
)
from core.models.extraction_schemas.relationship import (
    LLMPhraseRelationshipRecords,
    MaskedLLMPhraseRelationshipResults,
    PhraseMention,
    PhraseRecordsResponse,
    PhraseRelationshipRecord,
)
from core.models.extraction_schemas.response_format_util import (
    build_gpt_response_format,
)
from core.models.field_types import ExtractionFieldType
from core.services.phrase_blocks_contract import hold_response_to_sent_phrases
from core.utils.record_id_util import mask_relationship_records
from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.field_types import BatchRequestIDType
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    record_response_parse_error_capped,
)

logger = logging.getLogger(__name__)


LLM_PHRASE_RELATIONSHIP_RESPONSE_SCHEMA_V2 = build_gpt_response_format(
    PhraseRecordsResponse, name="phrase_relationship_records"
)

# What a dummy (no phrases found) relationship request answers with.
DUMMY_RECORDS_RESPONSE_CONTENT = '{"records": []}'


def parse_llm_phrase_relationship_records(
    gpt_response: Optional[str],
) -> LLMPhraseRelationshipRecords:
    """The wire's record entries as the stored phrase → record map."""
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_llm_phrase_relationship_records: Empty or invalid response from GPT"
        )

    try:
        parsed = PhraseRecordsResponse.model_validate_json(gpt_response)
    except ValidationError as e:
        raise ValueError(
            f"parse_llm_phrase_relationship_records: Invalid response from GPT:{gpt_response}"
        ) from e

    records: LLMPhraseRelationshipRecords = {}
    for entry in parsed.records:
        if entry.phrase in records:
            raise ValueError(
                f"parse_llm_phrase_relationship_records: Duplicate phrase "
                f"{entry.phrase!r} in records response"
            )
        records[entry.phrase] = PhraseRelationshipRecord(
            mentions=[
                PhraseMention(
                    form=mention.form, page=mention.page, account=mention.account
                )
                for mention in entry.mentions
            ],
            synthesis=entry.synthesis,
        )
    return records


async def parse_phrase_relationship_records_group_result(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    group_req_id: BatchRequestIDType,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    repairs: Optional[dict[str, str]] = None,
) -> LLMPhraseRelationshipRecords:
    """Parse the records returned by a single relationship group request."""
    req_obj = completed_request_map.get(group_req_id)
    if not req_obj:
        raise ValueError(
            f"phrase_relationship_node_v2: Missing GPTBatchRequest for "
            f"phrase_relationship request ID {group_req_id} in "
            f"{subject_unique_id}:{field_type.name}"
        )
    elif not req_obj.response:
        raise ValueError(
            f"phrase_relationship_node_v2: GPTBatchRequest for phrase_relationship "
            f"request ID {group_req_id} has no response_blob in "
            f"{subject_unique_id}:{field_type.name}"
        )

    try:
        records = parse_llm_phrase_relationship_records(req_obj.response.result)
        # Same hold, same reasons as v1: this is the stage where phrase identity
        # is set, relationship is a total function of its candidate list, and a
        # drifted key would silently rename the phrase everywhere downstream.
        return hold_response_to_sent_phrases(
            user_message=req_obj.request.body.user_message(),
            response_by_phrase=records,
            where=f"{subject_unique_id}:{field_type.name} relationship {group_req_id}",
            on_missing="raise",
            repairs=repairs,
        )
    except Exception as e:
        await record_response_parse_error_capped(
            gpt_batch_request=req_obj,
            error_message=str(e),
            timestamp=timestamp,
            traceback_str=traceback.format_exc(),
        )
        logger.error(
            f"phrase_relationship_node_v2: Error parsing phrase_relationship "
            f"records for subject {subject_unique_id} from GPT response: {e}"
        )
        raise


async def get_masked_phrase_relationship_result(
    subject_unique_id: str,
    field_type: ExtractionFieldType,
    chunk_bounds: str,
    extraction_bundle: LLMPhraseExtractionRequestBundle,
    completed_request_map: dict[BatchRequestIDType, GPTBatchRequest],
    timestamp: datetime,
    repairs: Optional[dict[str, str]] = None,
) -> MaskedLLMPhraseRelationshipResults:
    """The chunk's relationship result, merged across groups and MASKED.

    This is the shape every downstream stage consumes: record_id keys, the
    phrase riding inside each entry. Masking is derived purely from the phrase
    (see ``record_id_util``), so re-deriving this map at every downstream
    parse/create site always reproduces identical ids.
    """
    group_req_ids = extraction_bundle.llm_phrase_relationship_req_ids
    if not group_req_ids:
        raise ValueError(
            f"phrase_relationship_node_v2: phrase_relationship_req_ids is empty "
            f"for chunk bounds {chunk_bounds} in {subject_unique_id}:{field_type.name}"
        )

    merged: LLMPhraseRelationshipRecords = {}
    for group_req_id in group_req_ids:
        merged.update(
            await parse_phrase_relationship_records_group_result(
                subject_unique_id=subject_unique_id,
                field_type=field_type,
                chunk_bounds=chunk_bounds,
                group_req_id=group_req_id,
                completed_request_map=completed_request_map,
                timestamp=timestamp,
                repairs=repairs,
            )
        )
    return mask_relationship_records(merged)


def records_with_mentions(
    masked: MaskedLLMPhraseRelationshipResults,
) -> MaskedLLMPhraseRelationshipResults:
    """The records that carry evidence — the honest not-found branch removed.

    A record with no mentions has nothing to ground and nothing to judge, so it
    skips grounding AND screening (fork F9). It stays in the stored relationship
    stats regardless; this filter shapes downstream INPUT, never the record.
    """
    return {
        record_id: entry
        for record_id, entry in masked.items()
        if entry.record.mentions
    }
