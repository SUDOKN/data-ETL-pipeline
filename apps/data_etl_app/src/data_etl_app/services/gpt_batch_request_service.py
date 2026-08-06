from typing import Optional

from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    bulk_delete_gpt_batch_requests_by_subject_id_and_field,
)
from core.models.types_and_enums import LLMExtractedFieldTypeEnum


async def bulk_delete_gpt_batch_requests_by_mfg_etld1_and_field(
    mfg_etld1: str,
    field_type: Optional[LLMExtractedFieldTypeEnum],
) -> int:
    """
    Bulk delete GPT batch requests associated with a manufacturer mfg_etld1 and field type.

    Args:
        mfg_etld1: Manufacturer mfg_etld1 for which to delete batch requests
        field_type: Field type to narrow deletion scope

    Returns:
        Number of deleted documents
    """
    return await bulk_delete_gpt_batch_requests_by_subject_id_and_field(
        subject_unique_id=mfg_etld1,
        field_name=field_type.name if field_type else None,
    )
