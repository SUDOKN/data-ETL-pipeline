import logging

from core.models.prompt import Prompt
from data_etl_app.models.pipeline_nodes.search_node import SearchNode
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.models.pipeline_nodes.keyword.keyword_reconcile_node import (
    KeywordReconcileNode,
)
from open_ai_key_app.models.gpt_model import LLM_Model

logger = logging.getLogger(__name__)


class KeywordSearchNode(SearchNode[KeywordTypeEnum]):
    """Phase 1: LLM Search for keywords"""

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        search_prompt: Prompt,
        llm_model: LLM_Model,
        next_node: KeywordReconcileNode,
    ):
        super().__init__(
            field_type=field_type,
            search_prompt=search_prompt,
            llm_model=llm_model,
            next_node=next_node,
        )
