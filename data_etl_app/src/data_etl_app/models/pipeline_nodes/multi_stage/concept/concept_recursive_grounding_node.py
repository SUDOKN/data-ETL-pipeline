from __future__ import annotations
import logging

from core.models.prompt import Prompt
from core.models.llm_model import LLM_Model
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.pipeline_nodes.multi_stage.llm_phrase_recursive_grounding_node import (
    LLMPhraseRecursiveGroundingNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_reconcile_node import (
    ConceptReconcileNode,
)
from data_etl_app.models.types_and_enums import (
    ConceptTypeEnum,
)
from open_ai_key_app.models.gpt_model_params import GPTModelParams

logger = logging.getLogger(__name__)


class ConceptRecursiveGroundingNode(LLMPhraseRecursiveGroundingNode):

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        next_node: ConceptReconcileNode,
        llm_model: LLM_Model,
        model_params: GPTModelParams,
        phrase_recursive_grounding_prompt: Prompt,
        known_concepts: set[Concept],
    ):
        super().__init__(
            field_type=concept_type,
            next_node=next_node,
            llm_model=llm_model,
            model_params=model_params,
            phrase_recursive_grounding_prompt=phrase_recursive_grounding_prompt,
            known_concepts=known_concepts,
        )
