import json
import logging
from datetime import datetime
from typing_extensions import TypedDict

from core.models.prompt import Prompt

from data_etl_app.models.skos_concept import Concept, ConceptJSONEncoder

from core.models.db.gpt_batch_request import GPTBatchRequest
from core.services.gpt_batch_request_service import create_base_gpt_batch_request
from open_ai_key_app.models.gpt_model import (
    GPTModel,
    GPT_4o_mini,
    ModelParameters,
    DefaultModelParameters,
)

logger = logging.getLogger(__name__)


def map_known_to_unknown_deferred(
    deferred_at: datetime,
    llm_mapping_req_id: str,
    known_concepts: set[Concept],  # DO NOT MUTATE
    unmatched_keywords: set[str],
    mapping_prompt: Prompt,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> GPTBatchRequest:
    logger.info(
        f"map_known_to_unknown_deferred: Generating GPTBatchRequest for {llm_mapping_req_id}"
    )
    context = json.dumps(
        {"unknowns": list(unmatched_keywords), "knowns": list(known_concepts)},
        cls=ConceptJSONEncoder,
    )
    gpt_batch_request = create_base_gpt_batch_request(
        deferred_at=deferred_at,
        custom_id=llm_mapping_req_id,
        context=context,
        prompt=mapping_prompt,
        gpt_model=gpt_model,
        model_params=model_params,
    )
    return gpt_batch_request
