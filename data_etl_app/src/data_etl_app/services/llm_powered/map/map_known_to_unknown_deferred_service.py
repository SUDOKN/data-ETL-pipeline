import json
import logging
from datetime import datetime
from typing_extensions import TypedDict

from core.models.prompt import Prompt

from data_etl_app.models.skos_concept import Concept, ConceptJSONEncoder

from core.models.db.gpt_batch_request import GPTBatchRequest
from open_ai_key_app.models.field_types import GPTBatchRequestCustomID
from open_ai_key_app.utils.batch_gpt_util import (
    get_gpt_request_blob,
)
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
    prompt: Prompt,
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
    gpt_batch_request = GPTBatchRequest(
        created_at=deferred_at,
        request=get_gpt_request_blob(
            context=context,
            prompt=prompt.text,
            custom_id=llm_mapping_req_id,
            gpt_model=gpt_model,
            model_params=model_params,
        ),
        batch_id=None,
    )
    return gpt_batch_request
