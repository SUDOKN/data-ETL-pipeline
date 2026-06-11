from __future__ import annotations
import logging
from datetime import datetime
import traceback
from typing import Optional, Union
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
    ConceptExtractionRequestBundle,
)
from core.models.deferred_keyword_extraction import (
    DeferredKeywordExtractionRequests,
    KeywordExtractionRequestBundle,
)
from core.models.field_types import LLMEvidenceResults
from core.models.prompt import Prompt
from data_etl_app.models.types_and_enums import LLMExtractedFieldTypeEnum
from data_etl_app.models.pipeline_nodes.base_node import (
    LLMExtractedFieldTypeVar,
    PipelineContext,
)
from data_etl_app.models.pipeline_nodes.reconcile_node import ReconcileNode
from data_etl_app.models.pipeline_nodes.search_node import SearchNode
from data_etl_app.models.pipeline_nodes.llm_extraction_node import (
    LLMExtractionNode,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from litellm_proxy_app.models.llm_model import LLM_Model
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from scraper_app.models.scraped_text_file import ScrapedTextFile

from data_etl_app.services.extraction.deferred_llm_evidence_node_service import (
    create_missing_evidence_requests,
    parse_llm_evidence_result,
)
from core.services.gpt_batch_request_writes import record_response_parse_error

logger = logging.getLogger(__name__)

# Both the keyword pipeline (search -> evidence -> reconcile) and the concept pipeline
# (search -> evidence -> mapping -> reconcile) share the evidence phase. Both deferred
# request types carry an ``llm_evidence_request_id`` on each chunk bundle, so the evidence
# node logic can be implemented once and shared via thin subclasses.
DeferredEvidenceExtractionRequests = Union[
    DeferredConceptExtractionRequests, DeferredKeywordExtractionRequests
]
EvidenceExtractionRequestBundle = Union[
    ConceptExtractionRequestBundle, KeywordExtractionRequestBundle
]


class EvidenceNode(LLMExtractionNode[LLMExtractedFieldTypeVar, LLMEvidenceResults]):
    """Phase 2: LLM finds evidence in the text.

    Shared base for ``ConceptEvidenceNode`` and ``KeywordEvidenceNode``, mirroring the way
    ``SearchNode`` is shared by ``ConceptSearchNode`` and ``KeywordSearchNode``. Subclasses
    only narrow constructor types and declare which upstream ``SearchNode`` subclass produced
    the search results they consume from the pipeline context.
    """

    # Subclasses set this to the concrete upstream SearchNode subclass so the evidence phase
    # can read the completed search requests from the pipeline context, which is keyed by the
    # node class that produced them.
    upstream_search_node_cls: type[SearchNode]

    def __init__(
        self,
        field_type: LLMExtractedFieldTypeVar,
        evidence_prompt: Prompt,
        next_node: LLMExtractionNode | ReconcileNode,
    ):
        super().__init__(field_type=field_type, next_node=next_node)
        self.evidence_prompt = evidence_prompt

    def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        extraction_requests: DeferredEvidenceExtractionRequests,
    ) -> set[GPTBatchRequestCustomID]:
        llm_evidence_req_ids: set[GPTBatchRequestCustomID] = set()
        for (
            chunk_bounds,
            extraction_bundle,
        ) in extraction_requests.request_map.items():
            if not extraction_bundle.llm_evidence_request_id:
                raise ValueError(
                    f"get_embedded_request_ids was called for {mfg_etld1}:{self.field_type.name} but llm_evidence_request_id is None for chunk bounds {chunk_bounds}."
                )
            llm_evidence_req_ids.add(extraction_bundle.llm_evidence_request_id)
        return llm_evidence_req_ids

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
    ) -> GPTBatchRequestCustomID:
        return f"{mfg_etld1}>{field_type.name}>llm_evidence>chunk>{chunk_bounds}>{model_params.to_custom_id_segment(llm_model.model_name)}"

    @staticmethod
    async def parse_batch_request_result(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        extraction_bundle: EvidenceExtractionRequestBundle,
        completed_request_map: dict[GPTBatchRequestCustomID, GPTBatchRequest],
        deferred_at: datetime,
    ) -> LLMEvidenceResults:
        llm_evidence_request_id = extraction_bundle.llm_evidence_request_id
        if not llm_evidence_request_id:
            raise ValueError(
                f"evidence_node.parse_batch_request_result: llm_evidence_request_id is None for chunk bounds {chunk_bounds} in {mfg_etld1}:{field_type.name}"
            )

        llm_evidence_req = completed_request_map.get(llm_evidence_request_id)
        if not llm_evidence_req:
            raise ValueError(
                f"evidence_node.parse_batch_request_result: Missing GPTBatchRequest for evidence request ID {llm_evidence_request_id} in {mfg_etld1}:{field_type.name}"
            )
        elif not llm_evidence_req.response:
            raise ValueError(
                f"evidence_node.parse_batch_request_result: GPTBatchRequest for evidence request ID {llm_evidence_request_id} has no response_blob in {mfg_etld1}:{field_type.name}"
            )

        try:
            llm_evidence_results = parse_llm_evidence_result(
                llm_evidence_req.response.result
            )
            return llm_evidence_results
        except Exception as e:
            await record_response_parse_error(
                gpt_batch_request=llm_evidence_req,
                error_message=str(e),
                timestamp=deferred_at,
                traceback_str=traceback.format_exc(),
            )
            logger.error(
                f"evidence_node.parse_batch_request_result: Error parsing evidence results for manufacturer {mfg_etld1} from GPT response: {e}"
            )
            raise

    async def create_batch_requests(
        self,
        missing_request_ids: set[GPTBatchRequestCustomID],
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        eager: bool,
    ) -> list[GPTBatchRequest]:
        """Create batch requests for the evidence phase."""
        mfg_name = pipeline_context.mfg_name
        if not mfg_name:
            raise ValueError(
                f"evidence_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but pipeline_context.mfg_name is not set. Ensure business_desc is extracted before evidence."
            )

        extraction_requests: Optional[DeferredEvidenceExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"evidence_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but no deferred extraction exists."
            )

        # create_missing_evidence_requests only creates batch requests fresh or only missing ones,
        # for e.g., new mfg or some batch requests failed earlier and were deleted to allow re-processing
        batch_requests = await create_missing_evidence_requests(
            deferred_at=timestamp,
            mfg_etld1=deferred_mfg.etld1,
            mfg_name=mfg_name,
            field_type=self.field_type,
            missing_evidence_req_ids=missing_request_ids,
            extraction_requests=extraction_requests,
            mfg_text=scraped_text_file.text,
            evidence_prompt=self.evidence_prompt,
            upstream_completed_batch_req_map=pipeline_context[
                self.upstream_search_node_cls
            ],
            llm_model=llm_model,
            model_params=model_params,
            eager=eager,
        )

        return batch_requests
