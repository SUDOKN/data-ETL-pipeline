from __future__ import annotations
import logging
from datetime import datetime
from typing import TYPE_CHECKING

from llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from llm_providers.models.file_objects.prompt import Prompt
from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.field_types import ConceptFieldType
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_relationship_screening_node import (
    LLMPhraseRelationshipScreeningNode,
)
from core.models.skos_concept import Concept
from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (
    get_record_grounding_result,
)
from core.services.pipeline_nodes.multi_stage.pipeline_v2_derivations import (
    candidates_for_screening,
)
from core.services.rule_catalog_registry import get_rule_catalog
from core.models.rule_catalog import (
    STAGE_INITIAL_GROUNDING,
    STAGE_OOV_GROUNDING,
)
from core.utils.rdf_to_graph_util import get_match_label_to_concept_map
from llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from core.models.pipeline_nodes.multi_stage.concept.concept_iterative_grounding_node import (
        ConceptIterativeGroundingNode,
    )

logger = logging.getLogger(__name__)


class ConceptRelationshipScreeningNode(
    LLMPhraseRelationshipScreeningNode[ConceptFieldType]
):
    """Concept screening: candidates are the union of the in-vocab pass and the
    OOV discovery pass, casefold-deduped preferring the in-vocab spelling."""

    def __init__(
        self,
        concept_type: ConceptFieldType,
        next_node: ConceptIterativeGroundingNode,
        phrase_relationship_screening_prompt: Prompt,
        known_concepts: set[Concept],
    ):
        super().__init__(
            field_type=concept_type,
            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
            next_node=next_node,
        )
        self.known_concepts = known_concepts
        self.match_label_to_concept_map = get_match_label_to_concept_map(known_concepts)

    def get_upstream_mention_collection_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_mention_collection_node import (
            ConceptMentionCollectionNode,
        )

        return pipeline_context[ConceptMentionCollectionNode]

    def get_upstream_synthesis_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_synthesis_node import (
            ConceptSynthesisNode,
        )

        return pipeline_context[ConceptSynthesisNode]

    def get_upstream_in_vocab_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_initial_grounding_node import (
            ConceptInitialGroundingNode,
        )

        return pipeline_context[ConceptInitialGroundingNode]

    def get_upstream_oov_grounding_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from core.models.pipeline_nodes.multi_stage.concept.concept_oov_grounding_node import (
            ConceptOovGroundingNode,
        )

        return pipeline_context[ConceptOovGroundingNode]

    async def get_chunk_candidates_by_record(
        self,
        subject_unique_id: str,
        chunk_bounds: str,
        extraction_bundle: ConceptExtractionRequestBundle,
        pipeline_context: PipelineContext,
        timestamp: datetime,
    ) -> dict[str, list[str]]:
        in_vocab_results = await get_record_grounding_result(
            stage_label="in-vocab grounding",
            subject_unique_id=subject_unique_id,
            field_name=self.field_type.name,
            chunk_bounds=chunk_bounds,
            catalog=get_rule_catalog(STAGE_INITIAL_GROUNDING, self.field_type.name),
            group_req_ids=extraction_bundle.llm_phrase_initial_grounding_req_ids,
            retry_req_ids=extraction_bundle.llm_phrase_initial_grounding_retry_req_ids,
            completed_request_map=self.get_upstream_in_vocab_grounding_map(
                pipeline_context
            ),
            timestamp=timestamp,
            allowed_labels=list(self.match_label_to_concept_map.keys()),
        )
        # An OFF OOV pass embeds no ids at all; its contribution is then empty
        # by construction rather than a special case downstream.
        oov_results = (
            await get_record_grounding_result(
                stage_label="oov grounding",
                subject_unique_id=subject_unique_id,
                field_name=self.field_type.name,
                chunk_bounds=chunk_bounds,
                catalog=get_rule_catalog(STAGE_OOV_GROUNDING, self.field_type.name),
                group_req_ids=extraction_bundle.llm_phrase_oov_grounding_req_ids,
                retry_req_ids=extraction_bundle.llm_phrase_oov_grounding_retry_req_ids,
                completed_request_map=self.get_upstream_oov_grounding_map(
                    pipeline_context
                ),
                timestamp=timestamp,
            )
            if extraction_bundle.llm_phrase_oov_grounding_req_ids
            else {}
        )
        return candidates_for_screening(in_vocab_results, oov_results)
