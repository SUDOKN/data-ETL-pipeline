from __future__ import annotations
from datetime import datetime
import copy
import logging
from typing import TYPE_CHECKING, Optional

from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.batch_request_objects.gpt_batch_response_blob import GPTBatchResponse
from core.models.field_types import LLMGroundingResults, RecursivelyTaggedConceptNode
from core.models.file_objects.prompt import Prompt
from core.models.llm_model import LLM_Model
from core.models.deferred_extraction.deferred_concept_extraction import (
    DeferredConceptExtractionRequests,
    ConceptExtractionRequestMap,
    ConceptExtractionMetadata,
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

if TYPE_CHECKING:
    from scraper_app.models.scraped_text_file import ScrapedTextFile

from core.services.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from data_etl_app.services.extraction.deferred_llm_initial_grounding_service import (
    parse_batch_request_result as parse_initial_grounding_batch_req_result,
)
from data_etl_app.services.extraction.deferred_llm_recursive_grounding_service import (
    parse_batch_request_result,
    get_tagged_concepts_from_grounding_results,
    get_flattened_embedded_tagged_concepts,
    create_missing_phrase_recursive_grounding_requests,
)

from data_etl_app.utils.rdf_to_graph_util import get_match_label_to_concept_map

logger = logging.getLogger(__name__)


class LLMPhraseRecursiveGroundingNode(
    BaseLLMRecursiveExtractionNode[ConceptTypeEnum, LLMGroundingResults]
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

    async def embed_request_ids(  # prefill folded into this function
        self,
        mfg_etld1: str,
        pipeline_context: PipelineContext,
        metadata: ConceptExtractionMetadata,
        request_map: ConceptExtractionRequestMap,
        timestamp: datetime,
    ):
        """
        LOGIC:

        Iterate over each chunk in request_map:
            1. check if llm_phrase_recursive_grounding_requests is empty
                1.1 Yes? implies first run after recursive grounding,
                use recursive grounding to find only the shallowest level concepts where phrase/s are tagged
                and only save the concepts at the same shallowest level as RecursiveGroundingRequestNode
                1.2 No? Validate metadata, then Skip

            2. check if existing requests are complete
                2.1 No? do nothing and return, no need to add more ids yet
                2.2 Yes? Target next level,
                    fetch previous level results from recursive grounding
                    fetch phrases from recursive grounding
                    fine the closest next level where previous results and recursive grounding phrase/s if any collide
                    create a request custom id for each unique parent material
        """

        if not request_map:
            raise ValueError(
                f"Cannot embed req ids for llm recursive grounding node, "
                f"as request_map found empty for mfg:{mfg_etld1}, field:{self.field_type.name}."
            )

        completed_initial_grounding_req_map = self.get_upstream_initial_grounding_map(
            pipeline_context
        )
        if not completed_initial_grounding_req_map:
            raise ValueError(
                f"Cannot embed req ids for llm recursive grounding node, "
                f"as recursive grounding req map is empty for mfg:{mfg_etld1}, field:{self.field_type.name}."
            )

        for (
            chunk_bounds,
            extraction_request_bundle,
        ) in request_map.items():
            """
            1. check if llm_phrase_recursive_grounding_requests is empty
            """

            """
            1.2 No? skip
            """
            if (
                extraction_request_bundle.llm_phrase_recursive_grounding_root_req_nodes
                is not None
            ):
                continue

            """
            1.1 Yes? implies first run after recursive grounding,
            use recursive grounding to find only the shallowest level where phrase/s are tagged
            """
            chunk_initial_grounding_results = (
                await (
                    parse_initial_grounding_batch_req_result(
                        mfg_etld1=mfg_etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=extraction_request_bundle,
                        completed_request_map=completed_initial_grounding_req_map,
                        deferred_at=timestamp,
                    )
                )
            )
            # find the closest level to surface where at least one phrase was tagged to a concept at this level
            initially_tagged_in_vocab_concepts: list[RecursivelyTaggedConceptNode] = (
                list(
                    (
                        get_tagged_concepts_from_grounding_results(
                            mfg_etld1=mfg_etld1,
                            field_type=self.field_type,
                            chunk_bounds=chunk_bounds,
                            initial=True,
                            grounding_results=chunk_initial_grounding_results,
                            match_label_to_concept_map=self.match_label_to_concept_map,
                            llm_model=metadata.llm_phrase_recursive_grounding.llm_model,
                            model_params=metadata.llm_phrase_recursive_grounding.model_params,
                            level=None,
                        )
                    ).values()
                )
            )

            if initially_tagged_in_vocab_concepts:
                shallowest_level = min(
                    initially_tagged_in_vocab_concepts,
                    key=lambda tagged_concept: tagged_concept.level,
                ).level

                shallowest_level_tagged_concepts = [
                    tagged_concept
                    for tagged_concept in initially_tagged_in_vocab_concepts
                    if tagged_concept.level == shallowest_level
                ]

                # now only save the concepts at the same common shallowest level
                extraction_request_bundle.llm_phrase_recursive_grounding_root_req_nodes = (
                    shallowest_level_tagged_concepts
                )
            else:
                logger.info(
                    f"Bummer, no phrase was tagged to any known {self.field_type.name} for {mfg_etld1}:{chunk_bounds}."
                )
                extraction_request_bundle.llm_phrase_recursive_grounding_root_req_nodes = (
                    []
                )

        """
            2. check if existing requests are complete
        """
        if not await self.are_all_requests_complete(  # automatically ensures deepest level requests are also complete
            mfg_etld1=mfg_etld1, request_map=request_map
        ):
            """
            2.1 No? do nothing and return, no need to add more ids yet
            """
            logger.info(
                f"Must wait for existing requests to complete, before embedding new req ids."
            )
            return

        """
        2.2 Yes? Target next level,
        """
        completed_recursive_grounding_req_map = await self.get_completed_request_map(
            mfg_etld1=mfg_etld1,
            request_map=request_map,
            all_requests_must_be_complete=True,
        )

        for (
            chunk_bounds,
            extraction_request_bundle,
        ) in request_map.items():
            embedded_tagged_in_vocab_concepts = get_flattened_embedded_tagged_concepts(
                extraction_bundle=extraction_request_bundle,
                in_vocab=True,
                match_label_to_concept_map=self.match_label_to_concept_map,
            )
            if not embedded_tagged_in_vocab_concepts:
                continue  # to next chunk

            # next, find latest level whose requests were completed
            # parse result and create children tagged_concept by coining new req ids
            # and add to parent
            deepest_level = max(
                embedded_tagged_in_vocab_concepts,
                key=lambda tagged_concept: tagged_concept.level,
            ).level

            # fetch tagged concepts from initial grounding results as well
            # but at this depth + 1, which waiting for descendants that arrived from above, if any
            chunk_initial_grounding_results = (
                await (
                    parse_initial_grounding_batch_req_result(
                        mfg_etld1=mfg_etld1,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        extraction_bundle=extraction_request_bundle,
                        completed_request_map=completed_initial_grounding_req_map,
                        deferred_at=timestamp,
                    )
                )
            )
            initially_tagged_in_vocab_concepts_one_below_deepest_level: dict[
                str, RecursivelyTaggedConceptNode
            ] = get_tagged_concepts_from_grounding_results(
                mfg_etld1=mfg_etld1,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                initial=True,
                grounding_results=chunk_initial_grounding_results,
                match_label_to_concept_map=self.match_label_to_concept_map,
                llm_model=metadata.llm_phrase_recursive_grounding.llm_model,
                model_params=metadata.llm_phrase_recursive_grounding.model_params,
                level=deepest_level + 1,
            )
            all_tagged_concepts_one_below_deepest_level: dict[  # this may start containing out-of-vocab tagged concepts from descent result downstream
                str, RecursivelyTaggedConceptNode
            ] = copy.deepcopy(
                initially_tagged_in_vocab_concepts_one_below_deepest_level
            )
            # now this already contains tagged concepts from initial grounding

            # next expand the tree by parsing results of descend reqs of the deepest level that just completed
            embedded_tagged_parent_concepts = [
                tagged_concept
                for tagged_concept in embedded_tagged_in_vocab_concepts
                if tagged_concept.level == deepest_level
            ]  # these are the parent tagged concepts of latest completed descend requests

            # iterate over each descend req and find their results which should already be present
            for tagged_parent_concept in embedded_tagged_parent_concepts:
                # results will contain the children concepts of the tagged_concepts
                descent_result = await parse_batch_request_result(
                    mfg_etld1=mfg_etld1,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    descend_req_id=tagged_parent_concept.descend_req_id,
                    completed_request_map=completed_recursive_grounding_req_map,
                    deferred_at=timestamp,
                )
                new_tagged_child_concepts = get_tagged_concepts_from_grounding_results(
                    mfg_etld1=mfg_etld1,
                    field_type=self.field_type,
                    chunk_bounds=chunk_bounds,
                    initial=False,
                    grounding_results=descent_result,
                    match_label_to_concept_map=self.match_label_to_concept_map,
                    llm_model=metadata.llm_phrase_recursive_grounding.llm_model,
                    model_params=metadata.llm_phrase_recursive_grounding.model_params,
                    level=deepest_level + 1,
                )  # may contain out-of-vocab concepts as well
                for (
                    child_concept_label,
                    tagged_child_concept,  # may be an out-of-vocab concept
                ) in new_tagged_child_concepts.items():
                    existing_tagged_concept_at_child_level = (
                        all_tagged_concepts_one_below_deepest_level.get(
                            child_concept_label
                        )
                    )
                    if existing_tagged_concept_at_child_level:
                        # there was a initially tagged phrase waiting for an ancestor to arrive
                        existing_tagged_concept_at_child_level.iteratively_tagged_phrase_reason_map = (
                            tagged_child_concept.iteratively_tagged_phrase_reason_map
                        )
                    else:
                        # no phrase was initially tagged to this level
                        all_tagged_concepts_one_below_deepest_level[
                            child_concept_label
                        ] = tagged_child_concept

                    tagged_parent_concept.children.append(tagged_child_concept)
                    # all tagged children now represent a new level to be descended
                    # are_all_requests_complete will return False now because more
                    # req ids are now embedded but not created as batch request doc
                    # but only for children that are in-vocab, as get_embedded_request_ids
                    # ignores out-of-vocab tagged concepts, can't descend from them

    def get_embedded_request_ids(
        self,
        mfg_etld1: str,
        request_map: ConceptExtractionRequestMap,
    ) -> set[GPTBatchRequestCustomID]:
        all_chunks_recursive_grounding_req_ids: set[GPTBatchRequestCustomID] = set()
        for chunk_bounds, extraction_bundle in request_map.items():
            if extraction_bundle.llm_phrase_recursive_grounding_root_req_nodes is None:
                raise ValueError(
                    f"get_embedded_request_ids was called for {mfg_etld1}:{self.field_type.name} but llm_phrase_recursive_grounding_request_ids is None for chunk bounds {chunk_bounds}."
                )

            embedded_grounding_req_ids = [
                tagged_concept.descend_req_id
                for tagged_concept in get_flattened_embedded_tagged_concepts(
                    extraction_bundle=extraction_bundle,
                    in_vocab=True,
                    match_label_to_concept_map=self.match_label_to_concept_map,
                )
                if tagged_concept.name in self.match_label_to_concept_map
                # Ignore out-of-vocab or "None of the above"
            ]
            all_chunks_recursive_grounding_req_ids.update(embedded_grounding_req_ids)

        return all_chunks_recursive_grounding_req_ids

    @staticmethod
    def get_request_custom_id(
        mfg_etld1: str,
        field_type: LLMExtractedFieldTypeEnum,
        chunk_bounds: str,
        parent_level: int,
        parent_tagged_concept_name: str,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
    ) -> GPTBatchRequestCustomID:
        return (
            f"{mfg_etld1}>{field_type.name}>llm_phrase_recursive_grounding>chunk>"
            f"{chunk_bounds}>l[{parent_level}]>{parent_tagged_concept_name}>{model_params.to_custom_id_segment(llm_model.name)}"
        )

    async def create_batch_requests(
        self,
        missing_request_ids: set[GPTBatchRequestCustomID],
        deferred_mfg: DeferredManufacturer,
        scraped_text_file: ScrapedTextFile,
        timestamp: datetime,
        pipeline_context: PipelineContext,
        eager: bool,
    ) -> list[GPTBatchRequest]:

        mfg_name = pipeline_context.mfg_name
        if not mfg_name:
            raise ValueError(
                f"phrase_relationship_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but pipeline_context.mfg_name is not set. Ensure business_desc is extracted before phrase_relationship."
            )

        extraction_requests: Optional[DeferredConceptExtractionRequests] = getattr(
            deferred_mfg, self.field_type.name
        )
        if not extraction_requests:
            raise ValueError(
                f"phrase_relationship_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but no deferred extraction exists."
            )
        metadata = extraction_requests.metadata

        completed_recursive_grounding_req_map = await self.get_completed_request_map(
            mfg_etld1=deferred_mfg.etld1,
            request_map=extraction_requests.chunked_request_map,
            all_requests_must_be_complete=False,
        )

        # create_missing_phrase_relationship_requests only creates batch requests fresh or only missing ones,
        # for e.g., new mfg or some batch requests failed earlier and were deleted to allow re-processing
        batch_requests = await create_missing_phrase_recursive_grounding_requests(
            # used for logging and debugging
            mfg_etld1=deferred_mfg.etld1,
            mfg_name=mfg_name,
            field_type=self.field_type,
            # context
            chunked_request_map=extraction_requests.chunked_request_map,
            missing_phrase_recursive_grounding_req_ids=missing_request_ids,
            phrase_recursive_grounding_prompt=self.phrase_recursive_grounding_prompt,
            completed_recursive_grounding_req_map=completed_recursive_grounding_req_map,
            llm_phrase_relationship_gpt_request_map=self.get_upstream_phrase_relationship_map(
                pipeline_context
            ),
            match_label_to_concept_map=self.match_label_to_concept_map,
            # req metadata
            deferred_at=timestamp,
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
