import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer
from core.services.manufacturer_service import update_manufacturer

from core.models.binary_classification_result import (
    BinaryClassificationResult,
    BinaryClassificationStats,
    ChunkBinaryClassificationResult,
)
from core.models.deferred_binary_classification import (
    DeferredBinaryClassification,
)
from data_etl_app.models.types_and_enums import BinaryClassificationTypeEnum
from data_etl_app.services.llm_powered.classification.binary_classifier_service import (
    parse_chunk_binary_classification_result_from_gpt_response,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from data_etl_app.models.pipeline_nodes.reconcile.reconcile_node import ReconcileNode
from core.services.gpt_batch_request_service import (
    find_gpt_batch_request_by_custom_id,
    bulk_delete_gpt_batch_requests_by_custom_ids,
)

logger = logging.getLogger(__name__)


class BinaryReconcileNode(ReconcileNode):

    def __init__(self, binary_field_type: BinaryClassificationTypeEnum) -> None:
        super().__init__(field_type=binary_field_type)

    async def reconcile(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        timestamp: datetime,
        force: bool = False,
    ) -> None:
        if mfg.addresses and not force:
            return

        deferred_binary_classification: Optional[DeferredBinaryClassification] = (
            getattr(deferred_mfg, self.field_type.name)
        )
        if not deferred_binary_classification:
            raise ValueError(
                f"reconcile was called for addresses but no deferred address extraction exists."
            )

        final_chunk_gpt_request_id = (
            deferred_binary_classification.chunk_request_id_map.get(
                deferred_binary_classification.final_chunk_key
            )
        )
        if not final_chunk_gpt_request_id:
            raise ValueError(
                f"Deferred binary classification for addresses is missing final chunk GPT request ID."
            )

        gpt_request = await find_gpt_batch_request_by_custom_id(
            final_chunk_gpt_request_id
        )
        if not gpt_request or not gpt_request.response_blob:
            raise ValueError(
                f"Could not find completed GPTBatchRequest for address extraction with custom_id={final_chunk_gpt_request_id}."
            )

        final_chunk_result: ChunkBinaryClassificationResult = (
            parse_chunk_binary_classification_result_from_gpt_response(
                gpt_response=gpt_request.response_blob.result
            )
        )

        binary_classification_result = BinaryClassificationResult(
            evaluated_at=timestamp,
            answer=final_chunk_result.answer,
            confidence=final_chunk_result.confidence,
            reason=final_chunk_result.reason,
            stats=BinaryClassificationStats(
                prompt_version_id=deferred_binary_classification.prompt_version_id,
                final_chunk_key=deferred_binary_classification.final_chunk_key,
                chunk_result_map={
                    deferred_binary_classification.final_chunk_key: final_chunk_result
                },
            ),
        )

        setattr(mfg, self.field_type.name, binary_classification_result)

        await update_manufacturer(updated_at=timestamp, manufacturer=mfg)

        await bulk_delete_gpt_batch_requests_by_custom_ids(
            gpt_batch_request_custom_ids=[final_chunk_gpt_request_id],
            mfg_etld1=mfg.etld1,
        )

        # call super reconcile to clear deferred field
        await super().reconcile(
            mfg=mfg,
            deferred_mfg=deferred_mfg,
            timestamp=timestamp,
            force=force,
        )
