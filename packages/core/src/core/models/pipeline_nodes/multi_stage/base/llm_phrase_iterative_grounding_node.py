from __future__ import annotations
import logging
from datetime import datetime
from typing import Any, Optional

from requests.structures import CaseInsensitiveDict

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    GPTBatchResponse,
)
from core.models.extraction_schemas.grounding import (
    StopReason,
)
from core.models.extraction_schemas.iterative_tagging import (
    IterativeGroundingResult,
)
from core.models.extraction_schemas.synthesis import GroupRecords
from llm_providers.models.file_objects.prompt import Prompt
from core.models.extraction_results.extraction_node_metadata import (
    ExtractionNodeMetadata,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    ConceptExtractionMetadata,
)
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
    ConceptExtractionRequestMap,
    IterativeTaggingRequest,
    TaggingResult,
)
from core.models.skos_concept import Concept
from core.models.field_types import (
    ConceptFieldType,
)
from core.models.pipeline_nodes.base.base_node import (
    PipelineContext,
)
from core.models.pipeline_nodes.base.base_reconcile_node import (
    ReconcileNode,
)
from core.models.pipeline_nodes.base.base_llm_extraction_node import (
    BaseLLMExtractionNode,
)
from core.models.pipeline_nodes.base.base_llm_recursive_extraction_node import (
    BaseLLMRecursiveExtractionNode,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_synthesis_node_service import (
    get_chunk_group_records,
)
from llm_providers.field_types import BatchRequestIDType
from scraper.models.s3.scraped_text_file import ScrapedTextFile

from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    dispatch_gpt_batch_request,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_writes import (
    bulk_delete_gpt_batch_requests_by_custom_ids,
)
from llm_providers.services.gpt_batch_request.gpt_batch_request_queries import (
    find_completed_gpt_batch_requests_by_custom_ids,
)
from core.services.pipeline_nodes.multi_stage.llm_recursive_grounding_service import (
    create_missing_phrase_recursive_grounding_requests,
    descent_evidence_record_ids,
    descent_record_payloads,
    get_descend_worthy_tcs_from_tagged_results,
    get_descent_seed_tagging_results,
    get_itp_from_itr,
    get_itr_descendable_concept,
    parse_recursive_grounding_batch_request_result,
    get_all_recursive_grounding_results,
)


from core.utils.rdf_to_graph_util import (
    get_match_label_to_concept_map,
)
from core.utils.request_custom_id_util import upstream_digest_segment
from typing import ClassVar
from core.models.pipeline_nodes.base.pipeline_stage import (
    STAGE_REQUEST_ID_TOKEN,
    PipelineStage,
)

logger = logging.getLogger(__name__)


def _merge_itrs_into_level(
    level_set: set[IterativeTaggingRequest],
    candidates: list[IterativeTaggingRequest],
    match_label_to_concept_map: CaseInsensitiveDict[Concept],
) -> list[IterativeTaggingRequest]:
    """Add candidate nodes to a level's set, deduping ordinary nodes by concept.

    Node identity is (parent, name), so the set alone would keep a direct-tagged
    concept node (parent None) next to the same concept named by its parent's
    response — two descents of one concept, where downstream asserts one per
    level. Ordinary in-vocab nodes therefore merge by concept name, first-in
    wins: an earlier pass's node may already carry a dispatched request, so the
    existing node must survive. Stopped (false-child) and out-of-vocab nodes
    keep per-parent identity — each parent's verdict is its own record.

    Returns the candidates actually added, so the walk waits only on requests
    that can still be created.
    """
    claimed_concept_names = {
        concept.name
        for itr in level_set
        if itr.stop_reason is None
        and (concept := match_label_to_concept_map.get(itr.name)) is not None
    }
    added: list[IterativeTaggingRequest] = []
    for itr in candidates:
        if itr in level_set:
            # The same (parent, name) node from an earlier pass; its request,
            # if any, is already accounted for.
            continue
        if itr.stop_reason is None and (
            concept := match_label_to_concept_map.get(itr.name)
        ):
            if concept.name in claimed_concept_names:
                logger.info(
                    f"Skipping duplicate node for concept {concept.name!r} "
                    f"(parent {itr.parent_name!r}); the concept already has a node at this level."
                )
                continue
            claimed_concept_names.add(concept.name)
        level_set.add(itr)
        added.append(itr)
    return added


class LLMPhraseIterativeGroundingNode(
    BaseLLMRecursiveExtractionNode[ConceptFieldType, IterativeGroundingResult]
):

    stage: ClassVar[PipelineStage] = PipelineStage.iterative_grounding

    def __init__(
        self,
        field_type: ConceptFieldType,
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

    def get_upstream_in_vocab_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """Return the completed in-vocab grounding request map from pipeline context."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_in_vocab_grounding_map"
        )

    def get_upstream_screening_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """Return the completed screening request map from pipeline context."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_screening_map"
        )


    def get_upstream_synthesis_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        """The completed synthesis request map — the per-group records the
        descent requests carry are derived from its held answers."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_upstream_synthesis_map"
        )

    @staticmethod
    def _subject_text_of(
        pipeline_context: PipelineContext, subject_unique_id: str, field: str
    ) -> str:
        subject_text = pipeline_context.subject_text
        if subject_text is None:
            raise ValueError(
                f"Cannot embed req ids for recursive grounding: "
                f"PipelineContext.subject_text is None for subject:{subject_unique_id}, "
                f"field:{field}. The orchestrator must set it (the node recomputes the "
                f"synthesis stage's fold from the text when it derives its records)."
            )
        return subject_text

    @staticmethod
    def get_request_custom_id(
        subject_unique_id: str,
        field_type: ConceptFieldType,
        chunk_bounds: str,
        level: int,
        tag: str,
        parent_tag: Optional[str],
        node_metadata: ExtractionNodeMetadata,
        group_payload: dict[str, dict[str, Any]],
    ) -> BatchRequestIDType:
        # p[...] carries the parent because node identity is (parent, name): two
        # same-named nodes under different parents must not share an ID. ROOT
        # marks a direct-tagged node. Placed after the tag so anything reading
        # the tag positionally (right after l[N]>) keeps working. `|ud=` (fork
        # F12): the records this node would descend over are part of request
        # identity, so a re-run whose upstream changed re-asks the descent.
        return (
            f"{subject_unique_id}>{field_type.name}"
            f">{STAGE_REQUEST_ID_TOKEN[PipelineStage.iterative_grounding]}>chunk>"
            f"{chunk_bounds}>l[{level}]>{tag}>p[{parent_tag or 'ROOT'}]>"
            f"{node_metadata.to_custom_id_segment()}"
            f"{upstream_digest_segment(group_payload)}"
        )

    async def _mint_descend_req_id(
        self,
        subject_unique_id: str,
        chunk_bounds: str,
        bundle: ConceptExtractionRequestBundle,
        itr: IterativeTaggingRequest,
        seed_trs: list[TaggingResult],
        group_records: GroupRecords,
        completed_recursive_grounding_req_map: dict[
            BatchRequestIDType, GPTBatchRequest
        ],
        metadata: ConceptExtractionMetadata,
        timestamp: datetime,
    ) -> BatchRequestIDType:
        """The node's request id, digest included — derived from the same
        evidence walk request creation uses, so the id embedded and the payload
        sent can never disagree."""
        itp = await get_itp_from_itr(
            subject_unique_id=subject_unique_id,
            field_type=self.field_type,
            chunk_bounds=chunk_bounds,
            bundle=bundle,
            it_req=itr,
            seed_trs=seed_trs,
            completed_recursive_grounding_req_map=completed_recursive_grounding_req_map,
            match_label_to_concept_map=self.match_label_to_concept_map,
            timestamp=timestamp,
        )
        payload = descent_record_payloads(
            descent_evidence_record_ids(itp), group_records
        )
        return self.get_request_custom_id(
            subject_unique_id=subject_unique_id,
            field_type=self.field_type,
            chunk_bounds=chunk_bounds,
            level=itr.level,
            tag=itr.name,
            parent_tag=itr.parent_name,
            node_metadata=metadata.llm_phrase_recursive_grounding,
            group_payload=payload,
        )

    def get_embedded_request_ids(
        self,
        subject_unique_id: str,
        chunked_request_map: ConceptExtractionRequestMap,
    ) -> set[BatchRequestIDType]:
        all_chunks_recursive_grounding_req_ids: set[BatchRequestIDType] = set()
        for chunk_bounds, bundle in chunked_request_map.items():
            if bundle.llm_phrase_recursive_tagging_reqs is None:
                raise ValueError(
                    f"Cannot get embedded req ids for {subject_unique_id}>{chunk_bounds} as bundle.llm_phrase_recursive_tagging_reqs is None."
                )
            for _level, itrs in bundle.llm_phrase_recursive_tagging_reqs.items():
                logger.info(
                    f"Found {len(itrs)} embedded recursive grounding reqs for {subject_unique_id}:{self.field_type.name} in chunk {chunk_bounds} at level {_level}: {[itr.name for itr in itrs]}"
                )
                all_chunks_recursive_grounding_req_ids.update(
                    [
                        itr.descend_req_id
                        for itr in itrs
                        # Only descendable nodes (in-vocab, with children, not
                        # stopped) ever get a request created; declaring any
                        # other ID here would make it expected-but-never-created
                        # and stall the completeness checks forever.
                        # Non-descendable nodes (out-of-vocab proposals,
                        # false-child, leaf) still live in the tree for the
                        # phrase trail — they just carry no request.
                        if get_itr_descendable_concept(
                            itr, self.match_label_to_concept_map
                        )
                        is not None
                    ]
                )
        return all_chunks_recursive_grounding_req_ids

    async def embed_request_ids(  # prefill folded into this function
        self,
        subject_unique_id: str,
        pipeline_context: PipelineContext,
        metadata: ConceptExtractionMetadata,
        chunked_request_map: ConceptExtractionRequestMap,
        timestamp: datetime,
    ):
        if not chunked_request_map:
            raise ValueError(
                f"Cannot embed req ids for llm recursive grounding node, "
                f"as chunked_request_map found empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )

        # BASIC INITIALIZATION
        completed_in_vocab_grounding_req_map = self.get_upstream_in_vocab_grounding_map(
            pipeline_context
        )
        if not completed_in_vocab_grounding_req_map:
            raise ValueError(
                f"Cannot embed req ids for llm recursive grounding node, "
                f"as the in-vocab grounding req map is empty for subject:{subject_unique_id}, field:{self.field_type.name}."
            )
        completed_screening_req_map = self.get_upstream_screening_map(pipeline_context)
        synthesis_map = self.get_upstream_synthesis_map(pipeline_context)
        subject_text = self._subject_text_of(
            pipeline_context, subject_unique_id, self.field_type.name
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
            subject_unique_id=subject_unique_id,
            chunked_request_map=chunked_request_map,
            all_requests_must_be_complete=False,
        )
        logger.info(
            f"Completed request map fetched:{len(completed_recursive_grounding_req_map.keys())}, proceeding to embed new req ids."
        )

        if not await self.are_all_requests_complete(  # ensures last level requests are complete before proceeding
            subject_unique_id=subject_unique_id,
            chunked_request_map=chunked_request_map,
        ):
            logger.info(
                f"Must wait for existing requests to complete, before embedding new req ids."
            )
            incomplete_req_ids: list[BatchRequestIDType] = []
            for (
                chunk_bounds,
                bundle,
            ) in chunked_request_map.items():
                assert bundle.llm_phrase_recursive_tagging_reqs
                for _lvl, itrs in bundle.llm_phrase_recursive_tagging_reqs.items():
                    for itr in itrs:
                        # Non-descendable nodes never had a request; collecting
                        # them here would issue deletes for docs that don't
                        # exist and mislabel them incomplete in the logs.
                        if (
                            get_itr_descendable_concept(
                                itr, self.match_label_to_concept_map
                            )
                            is not None
                            and itr.descend_req_id
                            not in completed_recursive_grounding_req_map
                        ):
                            incomplete_req_ids.append(itr.descend_req_id)

            logger.info(f"Deleting batch reqs for:{incomplete_req_ids}")
            await bulk_delete_gpt_batch_requests_by_custom_ids(
                gpt_batch_request_custom_ids=incomplete_req_ids,
                subject_unique_id=subject_unique_id,
            )

            return  # let missing req ids logic execute in the caller function

        # at this point, no level is incomplete or all levels are empty
        # and completed_recursive_grounding_req_map contains all reqs or is empty if levels are too
        for (
            chunk_bounds,
            bundle,
        ) in chunked_request_map.items():
            seed_trs = await get_descent_seed_tagging_results(
                subject_unique_id=subject_unique_id,
                field_type=self.field_type,
                chunk_bounds=chunk_bounds,
                extraction_bundle=bundle,
                completed_in_vocab_grounding_req_map=completed_in_vocab_grounding_req_map,
                completed_screening_req_map=completed_screening_req_map,
                match_label_to_concept_map=self.match_label_to_concept_map,
                timestamp=timestamp,
            )
            group_records = await get_chunk_group_records(
                subject_unique_id,
                self.field_type,
                chunk_bounds,
                bundle,
                timestamp,
                synthesis_completed_request_map=synthesis_map,
                subject_text=subject_text,
                metadata=metadata,
            )
            directly_tagged_descend_worthy_tcs = get_descend_worthy_tcs_from_tagged_results(
                seed_trs=seed_trs,
                match_label_to_concept_map=self.match_label_to_concept_map,
            )

            logger.info(
                f"Found directly_tagged_descend_worthy_tcs for {subject_unique_id}:{self.field_type.name} in chunk {chunk_bounds}: {[f"l[{dtc.concept.level}]:{dtc.concept.name}" for dtc in directly_tagged_descend_worthy_tcs]}"
            )

            candidate_itrs: list[IterativeTaggingRequest] = []
            dtcs_at_next_level = [
                dtc
                for dtc in directly_tagged_descend_worthy_tcs
                if dtc.concept.level == 1
            ]
            for dtc in dtcs_at_next_level:
                itr = IterativeTaggingRequest(
                    parent_descend_req_id=None,
                    descend_req_id="",  # minted below, once the evidence is known
                    name=dtc.concept.name,
                    level=1,
                    parent_name=None,
                )
                itr.descend_req_id = await self._mint_descend_req_id(
                    subject_unique_id=subject_unique_id,
                    chunk_bounds=chunk_bounds,
                    bundle=bundle,
                    itr=itr,
                    seed_trs=seed_trs,
                    group_records=group_records,
                    completed_recursive_grounding_req_map=completed_recursive_grounding_req_map,
                    metadata=metadata,
                    timestamp=timestamp,
                )
                candidate_itrs.append(itr)

            assert bundle.llm_phrase_recursive_tagging_reqs is not None
            # new_itrs carries only the nodes actually added this pass — the
            # walk below waits solely on requests that can still be created.
            new_itrs = _merge_itrs_into_level(
                bundle.llm_phrase_recursive_tagging_reqs.setdefault(1, set()),
                candidate_itrs,
                self.match_label_to_concept_map,
            )

            next_level = 2
            while next_level <= max_concept_level + 1:
                # now check if any of those were not only descend worthy but also missing in the updated completed_recursive_grounding_req_map
                descend_worthy_new_itr_ids = [
                    next_level_itr.descend_req_id
                    for next_level_itr in new_itrs
                    # Same gate as get_embedded_request_ids: waiting on a
                    # non-descendable node (out-of-vocab proposal, false-child,
                    # leaf) would block the walk on a request that will never be
                    # created.
                    if get_itr_descendable_concept(
                        next_level_itr, self.match_label_to_concept_map
                    )
                    is not None
                ]
                # we update the completed req map and break out of the loop if any next_level_itr is missing
                logger.info(f"Updating completed req map just in case.")
                completed_recursive_grounding_req_map.update(
                    await find_completed_gpt_batch_requests_by_custom_ids(
                        subject_unique_id,
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
                candidate_itrs = []
                # find next level children from curr level parents
                for parent_itr in bundle.llm_phrase_recursive_tagging_reqs[
                    next_level - 1
                ]:
                    logger.info(
                        f"Parsing results for parent_itr: l{parent_itr.level}>{parent_itr.name} in chunk {chunk_bounds} for {subject_unique_id}:{self.field_type.name}"
                    )
                    parent_concept = get_itr_descendable_concept(
                        parent_itr, self.match_label_to_concept_map
                    )
                    if not parent_concept:
                        if next_level == 1:
                            raise ValueError(
                                f"First level is allowed to only have in-vocab concepts, {parent_itr.name} not recognized."
                            )
                        # else this req was never descended in the first place:
                        # out-of-vocab proposals have no concept, a false child
                        # is stopped, and a leaf concept gets no descent request
                        # — either way there is no result to parse for children.
                        logger.info(
                            f"Skipping non-descendable parent_itr:{parent_itr.name}"
                        )
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

                    parent_answer = await parse_recursive_grounding_batch_request_result(
                        subject_unique_id=subject_unique_id,
                        field_type=self.field_type,
                        chunk_bounds=chunk_bounds,
                        descend_req_id=parent_itr.descend_req_id,
                        completed_request_map=completed_recursive_grounding_req_map,
                        deferred_at=timestamp,
                    )
                    # The declined half is not consulted here: this walk creates
                    # the CHILD nodes a response named, and a declination names
                    # none. It is placed on the parent node itself, by
                    # `get_itp_from_itr` when the results are read back.
                    for child_tr in parent_answer.tagging_results:
                        child_concept = self.match_label_to_concept_map.get(
                            child_tr.group_id
                        )
                        stop_reason: Optional[StopReason] = None
                        if child_concept:
                            child_tr.group_id = child_concept.name
                            if child_concept.name not in parent_concept.children:
                                # A real concept, but not a child of the parent
                                # it was asked under (a same-level cousin, or a
                                # different level entirely). Recorded as data —
                                # the old name suffix ("-FALSE_CHILD") made the
                                # node unmatchable and silently erased the
                                # event from trail and results.
                                logger.warning(
                                    f"Child concept {child_concept.name} is not a child of parent concept {parent_concept.name}. Marking as false child."
                                )
                                stop_reason = "false_child"
                            else:
                                logger.info(
                                    f"Found a descend worthy child:{child_tr.group_id}."
                                )
                        # No sentinel arm any more: a record from which nothing
                        # more specific qualifies answers an empty options
                        # array, so it produces no child at all — the
                        # empty-options stop.
                        itr = IterativeTaggingRequest(
                            parent_descend_req_id=parent_itr.descend_req_id,
                            descend_req_id="",  # minted below
                            name=child_tr.group_id,
                            level=next_level,
                            parent_name=parent_itr.name,
                            stop_reason=stop_reason,
                        )
                        itr.descend_req_id = await self._mint_descend_req_id(
                            subject_unique_id=subject_unique_id,
                            chunk_bounds=chunk_bounds,
                            bundle=bundle,
                            itr=itr,
                            seed_trs=seed_trs,
                            group_records=group_records,
                            completed_recursive_grounding_req_map=completed_recursive_grounding_req_map,
                            metadata=metadata,
                            timestamp=timestamp,
                        )
                        candidate_itrs.append(itr)
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
                        descend_req_id="",  # minted below
                        name=dtc.concept.name,
                        level=dtc.concept.level,
                        parent_name=None,
                    )
                    itr.descend_req_id = await self._mint_descend_req_id(
                        subject_unique_id=subject_unique_id,
                        chunk_bounds=chunk_bounds,
                        bundle=bundle,
                        itr=itr,
                        seed_trs=seed_trs,
                        group_records=group_records,
                        completed_recursive_grounding_req_map=completed_recursive_grounding_req_map,
                        metadata=metadata,
                        timestamp=timestamp,
                    )
                    # Appended after the response-derived children so that when
                    # both name one concept, the parented node claims it and the
                    # merge drops this one.
                    candidate_itrs.append(itr)

                new_itrs = _merge_itrs_into_level(
                    bundle.llm_phrase_recursive_tagging_reqs.setdefault(
                        next_level, set()
                    ),
                    candidate_itrs,
                    self.match_label_to_concept_map,
                )

                next_level += 1

    async def create_batch_requests(
        self,
        subject_unique_id: str,
        scraped_text_file: ScrapedTextFile,
        missing_request_ids: set[BatchRequestIDType],
        metadata: ConceptExtractionMetadata,
        chunked_request_map: ConceptExtractionRequestMap,
        pipeline_context: PipelineContext,
        timestamp: datetime,
        eager: bool,
    ) -> list[GPTBatchRequest]:

        subject_name = pipeline_context.subject_name
        if not subject_name:
            raise ValueError(
                f"phrase_relationship_node.create_batch_requests was called for {self.field_type.name} in {self.__class__.__name__} but pipeline_context.subject_name is not set. Ensure business_desc is extracted before phrase_relationship."
            )

        completed_recursive_grounding_req_map = await self.get_completed_request_map(
            subject_unique_id=subject_unique_id,
            chunked_request_map=chunked_request_map,
            all_requests_must_be_complete=False,  # because we are creating missing requests, some may be incomplete
        )

        # create_missing_phrase_recursive_grounding_requests only creates batch requests fresh or only missing ones,
        # for e.g., new subject or some batch requests failed earlier and were deleted to allow re-processing
        batch_requests = await create_missing_phrase_recursive_grounding_requests(
            # used for logging and debugging
            subject_unique_id=subject_unique_id,
            subject_name=subject_name,
            field_type=self.field_type,
            # context
            chunked_request_map=chunked_request_map,
            missing_phrase_recursive_grounding_req_ids=missing_request_ids,
            phrase_recursive_grounding_prompt=self.phrase_recursive_grounding_prompt,
            synthesis_completed_request_map=self.get_upstream_synthesis_map(
                pipeline_context
            ),
            subject_text=scraped_text_file.text,
            metadata=metadata,
            completed_in_vocab_grounding_req_map=self.get_upstream_in_vocab_grounding_map(
                pipeline_context
            ),
            completed_screening_req_map=self.get_upstream_screening_map(
                pipeline_context
            ),
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
        subject_unique_id: str,
        field_type: ConceptFieldType,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        completed_in_vocab_grounding_req_map: dict[BatchRequestIDType, GPTBatchRequest],
        completed_screening_req_map: dict[BatchRequestIDType, GPTBatchRequest],
        completed_recursive_grounding_req_map: dict[
            BatchRequestIDType, GPTBatchRequest
        ],
        match_label_to_concept_map: CaseInsensitiveDict[Concept],
        timestamp: datetime,  # for recording errors
    ) -> IterativeGroundingResult:
        return await get_all_recursive_grounding_results(
            subject_unique_id=subject_unique_id,
            field_type=field_type,
            chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle,
            completed_in_vocab_grounding_req_map=completed_in_vocab_grounding_req_map,
            completed_screening_req_map=completed_screening_req_map,
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
        )
