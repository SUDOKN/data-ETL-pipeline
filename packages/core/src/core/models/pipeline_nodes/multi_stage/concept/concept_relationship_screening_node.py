from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from packages.llm_providers.src.llm_providers.db_models.gpt_batch_request import (
    GPTBatchRequest,
)
from packages.llm_providers.src.llm_providers.models.file_objects.prompt import Prompt
from packages.core.src.core.models.pipeline_nodes.base.base_node import PipelineContext
from packages.core.src.core.models.types_and_enums import ConceptTypeEnum
from packages.core.src.core.models.pipeline_nodes.multi_stage.base.llm_phrase_relationship_screening_node import (
    LLMPhraseRelationshipScreeningNode,
)
from packages.core.src.core.models.types_and_enums import (
    ConceptTypeEnum,
)
from packages.llm_providers.src.llm_providers.field_types import BatchRequestIDType

if TYPE_CHECKING:
    from packages.core.src.core.models.pipeline_nodes.multi_stage.concept.concept_initial_grounding_node import (
        ConceptInitialGroundingNode,
    )

logger = logging.getLogger(__name__)


class ConceptRelationshipScreeningNode(
    LLMPhraseRelationshipScreeningNode[ConceptTypeEnum]
):

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        next_node: ConceptInitialGroundingNode,
        phrase_relationship_screening_prompt: Prompt,
    ):
        super().__init__(
            field_type=concept_type,
            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
            next_node=next_node,
        )

    def get_upstream_phrase_relationship_map(
        self, pipeline_context: PipelineContext
    ) -> dict[BatchRequestIDType, GPTBatchRequest]:
        from packages.core.src.core.models.pipeline_nodes.multi_stage.concept.concept_relationship_node import (
            ConceptRelationshipNode,
        )

        return pipeline_context[ConceptRelationshipNode]
