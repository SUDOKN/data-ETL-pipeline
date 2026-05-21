import logging

from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes.search_node import SearchNode
from data_etl_app.models.types_and_enums import (
    ConceptTypeEnum,
)
from data_etl_app.models.pipeline_nodes.concept.concept_evidence_node import (
    ConceptEvidenceNode,
)
from open_ai_key_app.models.gpt_model import LLM_Model

logger = logging.getLogger(__name__)


class ConceptSearchNode(SearchNode[ConceptTypeEnum]):
    """Phase 1: LLM Search for concepts"""

    def __init__(
        self,
        concept_type: ConceptTypeEnum,
        search_prompt: Prompt,
        llm_model: LLM_Model,
        next_node: ConceptEvidenceNode,
    ):
        super().__init__(
            field_type=concept_type,
            search_prompt=search_prompt,
            llm_model=llm_model,
            next_node=next_node,
        )
