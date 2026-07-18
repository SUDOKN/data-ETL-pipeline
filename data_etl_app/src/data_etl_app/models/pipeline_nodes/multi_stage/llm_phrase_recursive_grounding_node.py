from __future__ import annotations
import logging
from datetime import datetime

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.batch_request_objects.gpt_batch_response_blob import GPTBatchResponse
from core.models.field_types import PhraseToTagAndReasonMap
from core.models.file_objects.prompt import Prompt
from core.models.llm_model import LLM_Model
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestMap,
    ConceptExtractionMetadata,
    RecursiveTaggingRequest,
    TaggedResult,
    TaggedConceptResult,
)
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.types_and_enums import (
    ConceptTypeEnum,
    LLMExtractedFieldTypeEnum,
)
from data_etl_app.models.pipeline_nodes.base.base_node import (
    PipelineContext,
)
from data_etl_app.models.pipeline_nodes.base.base_reconcile_node import ReconcileNode
from data_etl_app.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from data_etl_app.models.pipeline_nodes.base.base_llm_recursive_extraction_node import (
    BaseLLMRecursiveExtractionNode,
)
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from open_ai_key_app.models.gpt_model_params import GPTModelParams
from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from data_etl_app.services.extraction.deferred_llm_initial_grounding_service import (
    flip_tag_to_phrase_reason_map,
    parse_initial_grounding_batch_request_result,
    get_descend_worthy_directly_tagged_concept_results,
)
from data_etl_app.services.extraction.deferred_llm_recursive_grounding_service import (
    create_missing_phrase_recursive_grounding_requests,
    parse_recursive_grounding_batch_request_result,
)


from data_etl_app.utils.rdf_to_graph_util import (
    get_match_label_to_concept_map,
)

logger = logging.getLogger(__name__)


class LLMPhraseRecursiveGroundingNode(
    BaseLLMRecursiveExtractionNode[ConceptTypeEnum, list[TaggedResult]]
):

    def __init__(
        self,
        field_type: ConceptTypeEnum,
        next_node: BaseLLMExtractionNode | ReconcileNode,
        phrase_recursive_grounding_prompt: Prompt,
        known_concepts: set[Concept],
    ):
        super().__init__(
            field_type=field_type,
            next_node=next_node,
        )
        self.phrase_recursive_grounding_prompt = phrase_recursive_grounding_prompt
        self.known_concepts = known_concepts
        self.match_label_to_concept_map = get_match_label_to_concept_map(known_concepts)

    def get_upstream_initial_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        """Return the completed initial-grounding request map from pipeline context."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_initial_grounding_map"
        )

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[GPTBatchRequestCustomID, GPTBatchRequest]:
        """Return the completed phrase-relationship request map from pipeline context."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_phrase_relationship_map"
        )

    def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        chunked_request_map: ConceptExtractionRequestMap,
    ) -> set[GPTBatchRequestCustomID]:
        all_chunks_recursive_grounding_req_ids: set[GPTBatchRequestCustomID] = set()
        for chunk_bounds, bundle in chunked_request_map.items():
            if not bundle.llm_phrase_recursive_tagging_reqs:
                raise ValueError(
                    f"Cannot get embedded req ids for {mfg_etld1}>{chunk_bounds} as bundle.llm_phrase_recursive_grounding_root_req_nodes is empty or None."
                )
            for _level, rtrs in bundle.llm_phrase_recursive_tagging_reqs.items():
                all_chunks_recursive_grounding_req_ids.update(
                    [
                        rtr.descend_req_id
                        for rtr in rtrs
                        if rtr.name in self.match_label_to_concept_map
                    ]
                )
        return all_chunks_recursive_grounding_req_ids

    async def embed_request_ids(  # prefill folded into this function
        self,
        mfg_etld1: str,
        pipeline_context: PipelineContext,
        metadata: ConceptExtractionMetadata,
        chunked_request_map: ConceptExtractionRequestMap,
        timestamp: datetime,
    ):
        if not chunked_request_map:
            raise ValueError(
                f"Cannot embed req ids for llm recursive grounding node, "
                f"as chunked_request_map found empty for mfg:{mfg_etld1}, field:{self.field_type.name}."
            )

        completed_initial_grounding_req_map = self.get_upstream_initial_grounding_map(
            pipeline_context
        )
        if not completed_initial_grounding_req_map:
            raise ValueError(
                f"Cannot embed req ids for llm recursive grounding node, "
                f"as recursive grounding req map is empty for mfg:{mfg_etld1}, field:{self.field_type.name}."
            )

        completed_recursive_grounding_req_map = await self.get_completed_request_map(
            mfg_etld1=mfg_etld1,
            chunked_request_map=chunked_request_map,
            all_requests_must_be_complete=True,
        )

        max_concept_level = max(c.level for c in self.known_concepts)

        for (
            chunk_bounds,
            extraction_request_bundle,
        ) in chunked_request_map.items():
            directly_tagged_descend_worthy_tcs = get_descend_worthy_directly_tagged_concept_results(  # tagged concept result will always be in vocab
                initially_tagged_trs=(
                    await parse_initial_grounding_batch_request_result(
                        mfg_etld1=mfg_etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=extraction_request_bundle,
                        completed_request_map=completed_initial_grounding_req_map,
                        deferred_at=timestamp,
                    )
                ),
                level=None,
                match_label_to_concept_map=self.match_label_to_concept_map,
            )

            if not extraction_request_bundle.llm_phrase_recursive_tagging_reqs:
                extraction_request_bundle.llm_phrase_recursive_tagging_reqs = {}
            else:
                if not await self.are_all_requests_complete(  # ensures last level requests are complete before proceeding
                    mfg_etld1=mfg_etld1, chunked_request_map=chunked_request_map
                ):
                    logger.info(
                        f"Must wait for existing requests to complete, before embedding new req ids."
                    )
                    return

            # fetch last level and parse results
            last_level_executed = max(
                extraction_request_bundle.llm_phrase_recursive_tagging_reqs.keys(),
                default=0,
            )
            last_level_descend_worthy_tagging_reqs = (
                (
                    req
                    for req in extraction_request_bundle.llm_phrase_recursive_tagging_reqs[
                        last_level_executed
                    ]
                    if req.name
                    in self.match_label_to_concept_map  # otherwise not descend worthy
                )
                if last_level_executed != 0
                else []
            )

            next_level_tagging_reqs: set[RecursiveTaggingRequest] = set()

            for parent_req in last_level_descend_worthy_tagging_reqs:
                parent_concept = self.match_label_to_concept_map.get(parent_req.name)
                if not parent_concept:
                    if last_level_executed != 1:
                        raise ValueError(
                            f"First level is allowed to only have in-vocab concepts, {parent_req.name} not recognized."
                        )
                    # else this req was never descended in the first place
                    continue

                tagged_children_trs = await parse_recursive_grounding_batch_request_result(  # throws error right here if unexpected level encountered
                    mfg_etld1=mfg_etld1,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    descend_req_id=parent_req.descend_req_id,
                    completed_request_map=completed_recursive_grounding_req_map,
                    deferred_at=timestamp,
                )
                for child_tr in tagged_children_trs:
                    child_concept = self.match_label_to_concept_map.get(child_tr.tag)
                    if child_concept:
                        child_tr.tag = child_concept.name
                        if child_concept.name not in parent_concept.children:
                            # this helps flag mischiveous in-vocab but not descend worthy in the next round
                            child_tr.tag += "-FALSE_CHILD"

                    rtr = RecursiveTaggingRequest(
                        descend_req_id=self.get_request_custom_id(
                            mfg_etld1=mfg_etld1,
                            field_type=self.field_type,
                            chunk_bounds=chunk_bounds,
                            level=(
                                parent_concept.level + 1  # equal to child_concept.level
                            ),
                            tag=child_tr.tag,
                            llm_model=metadata.llm_phrase_recursive_grounding.llm_model,
                            model_params=metadata.llm_phrase_recursive_grounding.model_params,
                        ),
                    )
                    next_level_tagging_reqs.add(rtr)
                    logger.info(
                        f"Embedded recursive_tagging_req for {rtr.name}-l[{rtr.level}] with descend_req_id:{rtr.descend_req_id}"
                    )

            next_level = last_level_executed + 1
            if (
                next_level_tagging_reqs
            ):  # it is possible that none of these are descend worthy
                extraction_request_bundle.llm_phrase_recursive_tagging_reqs[
                    next_level
                ] = next_level_tagging_reqs

            # embed directly tagged concepts from next_level or anything deeper, to keep recursion going
            # until the last possible level
            next_level_tagging_reqs = set()  # reset
            while next_level <= max_concept_level:
                dtcs_at_next_deeper_level = [
                    dtc
                    for dtc in directly_tagged_descend_worthy_tcs
                    if dtc.concept.level == next_level
                ]
                for dtc in dtcs_at_next_deeper_level:
                    rtr = RecursiveTaggingRequest(
                        descend_req_id=self.get_request_custom_id(
                            mfg_etld1=mfg_etld1,
                            field_type=self.field_type,
                            chunk_bounds=chunk_bounds,
                            level=dtc.concept.level,
                            tag=dtc.concept.name,
                            llm_model=metadata.llm_phrase_recursive_grounding.llm_model,
                            model_params=metadata.llm_phrase_recursive_grounding.model_params,
                        ),
                    )
                    next_level_tagging_reqs.add(
                        rtr
                    )  # has no effect if recursion already added this rtr
                    logger.info(
                        f"Embedded initially tagged rtr {rtr.name}-l[{rtr.level}] with descend_req_id:{rtr.descend_req_id}"
                    )

                did_any_descend_worthy_reqs_get_embedded = any(
                    next_level_tagging_req
                    for next_level_tagging_req in next_level_tagging_reqs
                    if next_level_tagging_req.name in self.match_label_to_concept_map
                )
                if did_any_descend_worthy_reqs_get_embedded:
                    break

                next_level = last_level_executed + 1

            extraction_request_bundle.llm_phrase_recursive_tagging_reqs[
                next_level
            ].update(
                next_level_tagging_reqs
            )  # .update because there is a chance the rtr from dtcs were found at the same level as next recursion level

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        level: int,
        tag: str,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
    ) -> GPTBatchRequestCustomID:
        return (
            f"{mfg_etld1}>{field_type.name}>llm_phrase_recursive_grounding>chunk>"
            f"{chunk_bounds}>l[{level}]>{tag}>{model_params.to_custom_id_segment(llm_model.name)}"
        )

    async def create_batch_requests(
        self,
        mfg_etld1: str,
        scraped_text_file: ScrapedTextFile,
        missing_request_ids: set[GPTBatchRequestCustomID],
        metadata: ConceptExtractionMetadata,
        chunked_request_map: ConceptExtractionRequestMap,
        pipeline_context: PipelineContext,
        timestamp: datetime,
        eager: bool,
    ) -> list[GPTBatchRequest]:

        mfg_name = pipeline_context.mfg_name
        if not mfg_name:
            raise ValueError(
                f"phrase_relationship_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but pipeline_context.mfg_name is not set. Ensure business_desc is extracted before phrase_relationship."
            )

        completed_initial_grounding_req_map = self.get_upstream_initial_grounding_map(
            pipeline_context
        )
        completed_recursive_grounding_req_map = await self.get_completed_request_map(
            mfg_etld1=mfg_etld1,
            chunked_request_map=chunked_request_map,
            all_requests_must_be_complete=False,
        )

        # create_missing_phrase_relationship_requests only creates batch requests fresh or only missing ones,
        # for e.g., new mfg or some batch requests failed earlier and were deleted to allow re-processing
        batch_requests = await create_missing_phrase_recursive_grounding_requests(
            # used for logging and debugging
            mfg_etld1=mfg_etld1,
            mfg_name=mfg_name,
            field_type=self.field_type,
            # context
            chunked_request_map=chunked_request_map,
            missing_phrase_recursive_grounding_req_ids=missing_request_ids,
            phrase_recursive_grounding_prompt=self.phrase_recursive_grounding_prompt,
            llm_phrase_relationship_gpt_request_map=self.get_upstream_phrase_relationship_map(
                pipeline_context
            ),
            completed_initial_grounding_req_map=completed_initial_grounding_req_map,
            completed_recursive_grounding_req_map=completed_recursive_grounding_req_map,
            match_label_to_concept_map=self.match_label_to_concept_map,
            # req metadata
            timestamp=timestamp,
            llm_model=metadata.llm_phrase_recursive_grounding.llm_model,
            model_params=metadata.llm_phrase_recursive_grounding.model_params,
            eager=eager,
        )

        return batch_requests

    async def dispatch_batch_request(
        self,
        gpt_batch_request: GPTBatchRequest,
        metadata: ConceptExtractionMetadata,
    ) -> GPTBatchResponse:
        return await dispatch_gpt_batch_request(
            gpt_batch_request=gpt_batch_request,
            gpt_model=metadata.llm_phrase_recursive_grounding.llm_model,
            model_params=metadata.llm_phrase_recursive_grounding.model_params,
        )
