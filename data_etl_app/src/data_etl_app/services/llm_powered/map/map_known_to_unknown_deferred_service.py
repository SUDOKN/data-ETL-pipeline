import json
import logging
from datetime import datetime
from typing_extensions import TypedDict

from core.models.prompt import Prompt

from data_etl_app.models.skos_concept import Concept, ConceptJSONEncoder

from open_ai_key_app.models.db.gpt_batch_request import GPTBatchRequest
from open_ai_key_app.models.field_types import GPTBatchRequestMongoID
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


async def map_known_to_unknown_deferred(
    deferred_at: datetime,
    custom_id: str,
    known_concepts: list[Concept],  # DO NOT MUTATE
    unmapped_unknowns: set[str],
    prompt: Prompt,
    gpt_model: GPTModel = GPT_4o_mini,
    model_params: ModelParameters = DefaultModelParameters,
) -> GPTBatchRequestMongoID:
    logger.info(
        f"map_known_to_unknown_deferred: Generating GPTBatchRequest for {custom_id}"
    )
    context = json.dumps(
        {"unknowns": list(unmapped_unknowns), "knowns": known_concepts},
        cls=ConceptJSONEncoder,
    )
    gpt_batch_request = GPTBatchRequest(
        request=get_gpt_request_blob(
            context=context,
            prompt=prompt.text,
            created_at=deferred_at,
            custom_id=custom_id,
            gpt_model=gpt_model,
            model_params=model_params,
        ),
        batch_id=None,
    )
    await gpt_batch_request.insert()
    assert (
        gpt_batch_request.id is not None
    ), f"map_known_to_unknown_deferred:{custom_id}: GPTBatchRequest.id should be set after insert()"
    return gpt_batch_request.id
