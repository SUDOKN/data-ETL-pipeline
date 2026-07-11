from __future__ import annotations
import logging

from core.models.prompt import Prompt
from core.models.llm_model import LLM_Model
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.pipeline_nodes.multi_stage.llm_phrase_relationship_screening_node import (
    LLMPhraseRelationshipScreeningNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_initial_grounding_node import (
    ConceptInitialGroundingNode,
)
from data_etl_app.models.types_and_enums import (
    ConceptTypeEnum,
)
from open_ai_key_app.models.gpt_model_params import GPTModelParams

logger = logging.getLogger(__name__)


class ConceptRelationshipScreeningNode(
    LLMPhraseRelationshipScreeningNode[ConceptTypeEnum]
):

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        next_node: ConceptInitialGroundingNode,
        phrase_relationship_screening_prompt: Prompt,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
    ):
        super().__init__(
            field_type=concept_type,
            phrase_relationship_screening_prompt=phrase_relationship_screening_prompt,
            next_node=next_node,
            llm_model=llm_model,
            model_params=model_params,
        )
