from __future__ import annotations
import logging
from datetime import datetime
from requests.structures import CaseInsensitiveDict

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.batch_request_objects.gpt_batch_response_blob import GPTBatchResponse
from core.models.field_types import IterativeGroundingResult
from core.models.file_objects.prompt import Prompt
from core.models.llm_model import LLM_Model
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
    ConceptExtractionRequestMap,
    ConceptExtractionMetadata,
    IterativeTaggingRequest,
)
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.types_and_enums import (
    ConceptTypeEnum,
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
from core.services.gpt_batch_request_writes import (
    bulk_delete_gpt_batch_requests_by_custom_ids,
)
from core.services.gpt_batch_request_queries import (
    find_completed_gpt_batch_requests_by_custom_ids,
)
from data_etl_app.services.extraction.deferred_llm_initial_grounding_service import (
    get_descend_worthy_tcs_from_tagged_results,
    get_tagged_results_from_initial_grounding,
)
from data_etl_app.services.extraction.deferred_llm_recursive_grounding_service import (
    create_missing_phrase_recursive_grounding_requests,
    parse_recursive_grounding_batch_request_result,
    get_all_recursive_grounding_results,
)


from data_etl_app.utils.rdf_to_graph_util import (
    get_match_label_to_concept_map,
)

logger = logging.getLogger(__name__)


class LLMPhraseIterativeGroundingNode(
    BaseLLMRecursiveExtractionNode[ConceptTypeEnum, IterativeGroundingResult]
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

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: ConceptTypeEnum,
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

    def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        chunked_request_map: ConceptExtractionRequestMap,
    ) -> set[GPTBatchRequestCustomID]:
        all_chunks_recursive_grounding_req_ids: set[GPTBatchRequestCustomID] = set()
        for chunk_bounds, bundle in chunked_request_map.items():
            if bundle.llm_phrase_recursive_tagging_reqs is None:
                raise ValueError(
                    f"Cannot get embedded req ids for {mfg_etld1}>{chunk_bounds} as bundle.llm_phrase_recursive_tagging_reqs is None."
                )
            for _level, itrs in bundle.llm_phrase_recursive_tagging_reqs.items():
                logger.info(
                    f"Found {len(itrs)} embedded recursive grounding reqs for {mfg_etld1}:{self.field_type.name} in chunk {chunk_bounds} at level {_level}: {[itr.name for itr in itrs]}"
                )
                all_chunks_recursive_grounding_req_ids.update(
                    [
                        itr.descend_req_id
                        for itr in itrs
                        if itr.name
                        in self.match_label_to_concept_map  # otherwise they were part of a parent's results, but never descended further or will not be descended further
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

        # BASIC INITIALIZATION
        completed_initial_grounding_req_map = self.get_upstream_initial_grounding_map(
            pipeline_context
        )
        if not completed_initial_grounding_req_map:
            raise ValueError(
                f"Cannot embed req ids for llm recursive grounding node, "
                f"as recursive grounding req map is empty for mfg:{mfg_etld1}, field:{self.field_type.name}."
            )

        max_concept_level = max(c.level for c in self.known_concepts)
        for (
            chunk_bounds,
            bundle,
        ) in chunked_request_map.items():
            # without this code get_completed_request_map calls get_embedded_request_ids which
            # will throw error if llm_phrase_recursive_tagging_reqs is None which it will be initially
            if bundle.llm_phrase_recursive_tagging_reqs is None:
                bundle.llm_phrase_recursive_tagging_reqs = {
                    # lvl: set() for lvl in range(1, max_concept_level + 1)
                }

        completed_recursive_grounding_req_map = await self.get_completed_request_map(
            mfg_etld1=mfg_etld1,
            chunked_request_map=chunked_request_map,
            all_requests_must_be_complete=False,
        )
        logger.info(
            f"Completed request map fetched:{len(completed_recursive_grounding_req_map.keys())}, proceeding to embed new req ids."
        )

        if not await self.are_all_requests_complete(  # ensures last level requests are complete before proceeding
            mfg_etld1=mfg_etld1,
            chunked_request_map=chunked_request_map,
        ):
            logger.info(
                f"Must wait for existing requests to complete, before embedding new req ids."
            )
            incomplete_req_ids: list[GPTBatchRequestCustomID] = []
            for (
                chunk_bounds,
                bundle,
            ) in chunked_request_map.items():
                assert bundle.llm_phrase_recursive_tagging_reqs
                for _lvl, itrs in bundle.llm_phrase_recursive_tagging_reqs.items():
                    for itr in itrs:
                        if (
                            itr.descend_req_id
                            not in completed_recursive_grounding_req_map
                        ):
                            incomplete_req_ids.append(itr.descend_req_id)

            logger.info(f"Deleting batch reqs for:{incomplete_req_ids}")
            await bulk_delete_gpt_batch_requests_by_custom_ids(
                gpt_batch_request_custom_ids=incomplete_req_ids, mfg_etld1=mfg_etld1
            )

            return  # let missing req ids logic execute in the caller function

        # at this point, no level is incomplete or all levels are empty
        # and completed_recursive_grounding_req_map contains all reqs or is empty if levels are too
        for (
            chunk_bounds,
            bundle,
        ) in chunked_request_map.items():
            directly_tagged_descend_worthy_tcs = get_descend_worthy_tcs_from_tagged_results(  # tagged concept result will always be in vocab
                initially_tagged_trs=(
                    await get_tagged_results_from_initial_grounding(
                        mfg_etld1=mfg_etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=bundle,
                        completed_request_map=completed_initial_grounding_req_map,
                        timestamp=timestamp,
                    )
                ),
                match_label_to_concept_map=self.match_label_to_concept_map,
            )

            logger.info(
                f"Found directly_tagged_descend_worthy_tcs for {mfg_etld1}:{self.field_type.name} in chunk {chunk_bounds}: {[f"l[{dtc.concept.level}]:{dtc.concept.name}" for dtc in directly_tagged_descend_worthy_tcs]}"
            )

            # now we only want to use and iterate directly_tagged_descend_worthy_tcs once, so better to iterate each level to check
            # if all reqs
            # curr_level = 1
            new_itrs: set[IterativeTaggingRequest] = set()
            dtcs_at_next_level = [
                dtc
                for dtc in directly_tagged_descend_worthy_tcs
                if dtc.concept.level == 1
            ]
            for dtc in dtcs_at_next_level:
                itr = IterativeTaggingRequest(
                    parent_descend_req_id=None,
                    descend_req_id=self.get_request_custom_id(
                        mfg_etld1=mfg_etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        level=1,
                        tag=dtc.concept.name,
                        llm_model=metadata.llm_phrase_recursive_grounding.llm_model,
                        model_params=metadata.llm_phrase_recursive_grounding.model_params,
                    ),
                )
                new_itrs.add(itr)  # has no effect if recursion already added this itr

            assert bundle.llm_phrase_recursive_tagging_reqs is not None
            bundle.llm_phrase_recursive_tagging_reqs.setdefault(1, set()).update(
                new_itrs
            )  # this will throw error if [curr_level] is not initialized preemptively

            next_level = 2
            while next_level <= max_concept_level + 1:
                # now check if any of those were not only descend worthy but also missing in the updated completed_recursive_grounding_req_map
                descend_worthy_new_itr_ids = [
                    next_level_itr.descend_req_id
                    for next_level_itr in new_itrs
                    if next_level_itr.name
                    in self.match_label_to_concept_map  # we don't care about others
                ]
                # we update the completed req map and break out of the loop if any next_level_itr is missing
                logger.info(f"Updating completed req map just in case.")
                completed_recursive_grounding_req_map.update(
                    await find_completed_gpt_batch_requests_by_custom_ids(
                        mfg_etld1,
                        descend_worthy_new_itr_ids,
                    )
                )
                if any(
                    descend_worthy_new_itr_id
                    for descend_worthy_new_itr_id in descend_worthy_new_itr_ids
                    if descend_worthy_new_itr_id
                    not in completed_recursive_grounding_req_map
                ):
                    # we have just updated a level with new descend worthy reqs
                    break

                # else explore
                new_itrs = set()
                # find next level children from curr level parents
                for parent_itr in bundle.llm_phrase_recursive_tagging_reqs[
                    next_level - 1
                ]:
                    logger.info(
                        f"Parsing results for parent_itr: l{parent_itr.level}>{parent_itr.name} in chunk {chunk_bounds} for {mfg_etld1}:{self.field_type.name}"
                    )
                    parent_concept = self.match_label_to_concept_map.get(
                        parent_itr.name
                    )
                    if not parent_concept:
                        if next_level == 1:
                            raise ValueError(
                                f"First level is allowed to only have in-vocab concepts, {parent_itr.name} not recognized."
                            )
                        # else this req was never descended in the first place
                        logger.info(f"Skipping oov parent_itr:{parent_itr.name}")
                        continue
                    elif parent_concept.level != next_level - 1:
                        raise ValueError(
                            f"Parent concept {parent_concept.name} level {parent_concept.level} does not match last_level_embedded {next_level}."
                        )
                    elif (
                        # it is in-vocab parent req
                        parent_itr.descend_req_id
                        not in completed_recursive_grounding_req_map
                    ):
                        raise ValueError(
                            f"parent_itr:{parent_itr.name} is missing in completed_recursive_grounding_req_map."
                        )

                    parent_itr_results = (
                        await parse_recursive_grounding_batch_request_result(
                            mfg_etld1=mfg_etld1,
                            field_type=self.field_type,
                            chunk_bounds=chunk_bounds,
                            descend_req_id=parent_itr.descend_req_id,
                            completed_request_map=completed_recursive_grounding_req_map,
                            deferred_at=timestamp,
                        )
                    )
                    for child_tr in parent_itr_results:
                        child_concept = self.match_label_to_concept_map.get(
                            child_tr.group_id
                        )
                        if child_concept:
                            child_tr.group_id = child_concept.name
                            if child_concept.name not in parent_concept.children:
                                # this helps flag mischiveous in-vocab but not descend worthy in the next round
                                # ex: it could be a same level cousin but different parent, or be a different level entirely
                                logger.warning(
                                    f"Child concept {child_concept.name} is not a child of parent concept {parent_concept.name}. Marking as FALSE_CHILD."
                                )
                                child_tr.group_id += "-FALSE_CHILD"
                            else:
                                logger.info(
                                    f"Found a descend worthy child:{child_tr.group_id}."
                                )
                        itr = IterativeTaggingRequest(
                            parent_descend_req_id=parent_itr.descend_req_id,
                            descend_req_id=self.get_request_custom_id(
                                mfg_etld1=mfg_etld1,
                                field_type=self.field_type,
                                chunk_bounds=chunk_bounds,
                                level=(
                                    next_level
                                    # will automatically be
                                    # = parent_concept.level + 1 OR
                                    # = child_concept.level; when child_concept available
                                ),
                                tag=child_tr.group_id,
                                llm_model=metadata.llm_phrase_recursive_grounding.llm_model,
                                model_params=metadata.llm_phrase_recursive_grounding.model_params,
                            ),
                        )
                        new_itrs.add(itr)
                        logger.info(
                            f"Adding recursive_tagging_req for {itr.name}-l[{itr.level}] with descend_req_id:{itr.descend_req_id} for embedding downstream."
                        )

                # find next level dtcs waiting for descent
                dtcs_at_next_level = [
                    dtc
                    for dtc in directly_tagged_descend_worthy_tcs
                    if dtc.concept.level == next_level
                ]
                for dtc in dtcs_at_next_level:
                    itr = IterativeTaggingRequest(
                        parent_descend_req_id=None,
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
                    new_itrs.add(
                        itr
                    )  # has no effect if recursion already added this itr

                bundle.llm_phrase_recursive_tagging_reqs.setdefault(
                    next_level, set()
                ).update(new_itrs)

                next_level += 1

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
            all_requests_must_be_complete=False,  # because we are creating missing requests, some may be incomplete
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

    @staticmethod
    async def get_result(
        mfg_etld1: str,
        field_type: ConceptTypeEnum,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        completed_initial_grounding_req_map: dict[
            GPTBatchRequestCustomID, GPTBatchRequest
        ],
        completed_recursive_grounding_req_map: dict[
            GPTBatchRequestCustomID, GPTBatchRequest
        ],
        match_label_to_concept_map: CaseInsensitiveDict[Concept],
        timestamp: datetime,  # for recording errors
    ) -> IterativeGroundingResult:
        return await get_all_recursive_grounding_results(
            mfg_etld1=mfg_etld1,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_initial_grounding_req_map=completed_initial_grounding_req_map,
            completed_recursive_grounding_req_map=completed_recursive_grounding_req_map,
            match_label_to_concept_map=match_label_to_concept_map,
            timestamp=timestamp,
        )

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
