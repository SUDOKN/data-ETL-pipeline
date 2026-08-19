from typing import Optional

from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    bulk_delete_gpt_batch_requests_by_subject_id_and_field,
)
from core.models.field_types import ExtractionFieldType
from core.models.pipeline_nodes.base.pipeline_stage import (
    PipelineStage,
    request_id_tokens_from,
)


async def bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field(
    mfg_etld1: str,
    field_type: Optional[ExtractionFieldType] = None,
    stage: Optional[PipelineStage] = None,
    and_downstream: bool = False,
) -> int:
    """
    Bulk delete GPT batch requests for a manufacturer, narrowed to a field
    and/or a pipeline stage.

    The four scopes, by what is passed:

    - neither: every stored request for the manufacturer.
    - field only: every stage of that field.
    - stage only: that stage of every field.
    - both: that one stage of that one field.

    ``and_downstream`` widens whichever stage scope was asked for to that stage
    plus every later one, which is how a field is rewound to the state it was in
    just before *stage* ran: the surviving requests are exactly its upstream.
    The stages re-create their requests on the next run, because a node asks the
    DB which of its embedded request ids are missing.

    Only the GPTBatchRequest documents are deleted. The deferred subject keeps
    its embedded request ids, and — for concept fields — the recursive-grounding
    tag tree it grew from the responses now being deleted, so a re-run walks the
    previously discovered tree rather than re-deriving it. Delete the deferred
    subject when that matters.

    Args:
        mfg_etld1: Manufacturer mfg_etld1 for which to delete batch requests
        field_type: Field type to narrow deletion scope, or None for every field
        stage: Pipeline stage to narrow deletion scope, or None for every stage
        and_downstream: Widen *stage* to itself plus every later stage

    Returns:
        Number of deleted documents
    """
    if stage is None and and_downstream:
        raise ValueError(
            "and_downstream needs a stage to be downstream of; pass one, or drop "
            "the flag to delete every stage."
        )

    return await bulk_delete_gpt_batch_requests_by_subject_id_and_field(
        subject_unique_id=mfg_etld1,
        field_name=field_type.name if field_type else None,
        stage_request_id_tokens=(
            request_id_tokens_from(stage, and_downstream=and_downstream)
            if stage
            else None
        ),
    )
