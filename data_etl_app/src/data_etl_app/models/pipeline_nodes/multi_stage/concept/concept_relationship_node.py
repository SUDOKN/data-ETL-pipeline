from __future__ import annotations
import logging

from core.models.prompt import Prompt
from core.models.llm_model import LLM_Model
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.models.pipeline_nodes.multi_stage.concept.concept_relationship_screening_node import (
    ConceptRelationshipScreeningNode,
)
from data_etl_app.models.pipeline_nodes.multi_stage.llm_phrase_relationship_node import (
    LLMPhraseRelationshipNode,
)
from open_ai_key_app.models.gpt_model_params import GPTModelParams

logger = logging.getLogger(__name__)


class ConceptRelationshipNode(LLMPhraseRelationshipNode[ConceptTypeEnum]):
    """Phase 2: LLM distills concepts from the phrases with context of the text.

    Thin wrapper over the shared ``PhraseRelationshipNode`` that narrows the constructor types and
    points the phrase_relationship phase at the upstream ``ConceptSearchNode`` results.
    """

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        phrase_relationship_prompt: Prompt,
        next_node: ConceptRelationshipScreeningNode,
    ):
        super().__init__(
            field_type=concept_type,
            phrase_relationship_prompt=phrase_relationship_prompt,
            next_node=next_node,
        )
