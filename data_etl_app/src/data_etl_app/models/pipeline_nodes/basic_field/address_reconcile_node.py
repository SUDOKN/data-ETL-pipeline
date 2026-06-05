import logging
from datetime import datetime
from typing import Optional

from core.models.address_extraction_result import (
    Address,
    AddressExtractionStats,
    AddressExtractionStatsMap,
    AddressExtractionResult,
)
from core.models.db.manufacturer import Manufacturer
from core.models.deferred_single_stage_extraction_requests import (
    DeferredSingleStageExtractionRequests,
)
from core.models.db.deferred_manufacturer import DeferredManufacturer
from data_etl_app.models.types_and_enums import BasicFieldTypeEnum
from data_etl_app.models.pipeline_nodes.llm_extraction_node import PipelineContext
from data_etl_app.models.pipeline_nodes.basic_field.address_extraction_node import (
    AddressExtractionNode,
)
from data_etl_app.models.pipeline_nodes.reconcile_node import ReconcileNode
from litellm_proxy_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.manufacturer_service import update_manufacturer

logger = logging.getLogger(__name__)


class AddressReconcileNode(ReconcileNode[BasicFieldTypeEnum.addresses]):
    def __init__(self) -> None:
        super().__init__(field_type=BasicFieldTypeEnum.addresses)

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
                f"reconcile was called for {self.field_type.name} but no deferred extraction requests exist."
            )

        completed_extraction_requests = pipeline_context[AddressExtractionNode]
        all_addresses: list[Address] = []
        chunk_stats: AddressExtractionStatsMap = {}
        for (
            chunk_bounds,
            bundle,
        ) in extraction_requests.request_map.items():
            address_extraction_results = (
                await AddressExtractionNode.parse_batch_request_result(
                    mfg_etld1=deferred_mfg.etld1,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    extraction_bundle=bundle,
                    completed_request_map=completed_extraction_requests,
                    deferred_at=timestamp,
                )
            )
            chunk_stats[chunk_bounds] = AddressExtractionStats(
                result=address_extraction_results
            )
            all_addresses.extend(address_extraction_results)

        final_extraction_result = AddressExtractionResult(
            metadata=extraction_requests.metadata,
            result=all_addresses,
            chunk_stats=chunk_stats,
        )

        setattr(mfg, self.field_type.name, final_extraction_result)
        await update_manufacturer(updated_at=timestamp, manufacturer=mfg)

        # call super wipe_down to clear deferred field and completed GPT requests from pipeline context
        await super().wipe_down(
            deferred_mfg=deferred_mfg,
            associated_batch_request_custom_ids=list(
                completed_extraction_requests.keys()
            ),
        )
