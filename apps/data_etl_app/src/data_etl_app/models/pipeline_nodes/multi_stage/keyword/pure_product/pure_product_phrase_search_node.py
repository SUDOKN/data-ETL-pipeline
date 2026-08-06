from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from llm_providers.models.file_objects.prompt import Prompt
from core.models.pipeline_nodes.multi_stage.keyword.keyword_phrase_search_node import (
    KeywordPhraseSearchNode,
)
from core.models.types_and_enums import KeywordTypeEnum

if TYPE_CHECKING:
    from data_etl_app.models.pipeline_nodes.multi_stage.keyword.pure_product.pure_product_recursive_search_node import (
        PureProductRecursiveSearchNode,
    )

logger = logging.getLogger(__name__)


class PureProductPhraseSearchNode(KeywordPhraseSearchNode):
    """Phase 1 for the pure-product branch (own/sell own products, no contract work).

    ``field_type`` is naturally ``KeywordTypeEnum.products`` here, which is also
    the canonical identity used for the search/recursive-search/relationship
    phases shared with the contract-manufacturing branch (see
    ``ContractProductPhraseSearchNode``'s custom_id override) — so no override is
    needed in this class.
    """

    def __init__(
        self,
        field_type: KeywordTypeEnum,
        search_prompt: Prompt,
        next_node: PureProductRecursiveSearchNode,
    ):
        super().__init__(
            field_type=field_type,
            search_prompt=search_prompt,
            next_node=next_node,
        )
