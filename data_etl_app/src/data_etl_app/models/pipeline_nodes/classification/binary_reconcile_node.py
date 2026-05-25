import logging
from datetime import datetime
from typing import Optional

from core.models.db.manufacturer import Manufacturer
from core.models.binary_classification_result import (
    BinaryClassificationResult,
    BinaryClassificationStats,
    BinaryClassificationStatsMap,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.deferred_single_stage_extraction_requests import (
    DeferredSingleStageExtractionRequests,
)
from data_etl_app.models.pipeline_nodes.base_node import (
    PipelineContext,
)
from data_etl_app.models.types_and_enums import (
    BinaryClassificationTypeEnum,
)
from data_etl_app.models.pipeline_nodes.reconcile_node import ReconcileNode
from data_etl_app.models.pipeline_nodes.classification.binary_classification_node import (
    BinaryClassificationNode,
)
from open_ai_key_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.manufacturer_service import update_manufacturer

logger = logging.getLogger(__name__)


class BinaryReconcileNode(ReconcileNode[BinaryClassificationTypeEnum]):

    def __init__(self, binary_field_type: BinaryClassificationTypeEnum) -> None:
        super().__init__(field_type=binary_field_type)

    async def execute(
        self,
        mfg: Manufacturer,
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        eager: bool,
    ) -> None:

        extraction_requests: Optional[DeferredSingleStageExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"reconcile/execute was called for {self.field_type.name} but no deferred extraction requests exist."
            )

        completed_classification_requests = pipeline_context[BinaryClassificationNode]
        first_chunk_bounds, first_req_bundle = list(
            extraction_requests.request_map.items()
        )[0]
        result = await BinaryClassificationNode.parse_batch_request_result(
            mfg_etld1=deferred_mfg.etld1,
            field_type=self.field_type,
            chunk_bounds=first_chunk_bounds,
            extraction_bundle=first_req_bundle,
            completed_request_map=completed_classification_requests,
            deferred_at=timestamp,
        )
        chunk_stats: BinaryClassificationStatsMap = {
            first_chunk_bounds: BinaryClassificationStats(result=result)
        }

        classification_result = BinaryClassificationResult(
            metadata=extraction_requests.metadata,
            result=result,
            chunk_stats=chunk_stats,
        )

        setattr(mfg, self.field_type.name, classification_result)
        await update_manufacturer(updated_at=timestamp, manufacturer=mfg)

        # call super wipe_down to clear deferred field and completed GPT requests from pipeline context
        await super().wipe_down(
            deferred_mfg=deferred_mfg,
            associated_batch_request_custom_ids=list(
                completed_classification_requests.keys()
            ),
        )
