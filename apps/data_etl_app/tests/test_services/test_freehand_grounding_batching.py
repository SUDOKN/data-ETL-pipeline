"""Freehand grounding splits a chunk's screened phrases across several requests.

The batching contract is the same one screening and initial grounding already
carry: group count is computed once, upfront, from the screened phrase set; each
group is its own request with its own custom_id; and the groups are merged back
into one flat phrase→tag map for the chunk. These tests pin the two halves that a
regression would silently swallow — the merge, and the group index in the id.
"""

import json
from datetime import datetime

import pytest

from core.models.chunking_strat import EQUIPMENT_CHUNKING_STRAT
from core.models.deferred_extraction.deferred_keyword_extraction import (
    KeywordExtractionRequestBundle,
)
from core.models.extraction_results.keyword_extraction_results import (
    KeywordExtractionMetadata,
)
from core.models.pipeline_nodes.multi_stage.keyword.keyword_freehand_grounding_node import (
    KeywordFreehandGroundingNode,
)
from core.services.pipeline_nodes.multi_stage.llm_freehand_grounding_service import (
    get_freehand_grounding_result,
)
from core.services.rule_catalog_registry import set_rule_catalog_lookup
from core.services.phrase_blocks_contract import render_phrase_blocks
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.services.extraction_pipeline_factory import (
    ExtractionPipelineFactory,
)
from data_etl_app.services.prompt_assembly_service import build_rule_catalog_lookup
from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.open_ai.gpt_batch_request_blob import GPTBatchRequestBlob
from llm_providers.models.open_ai.gpt_request_body import GPTRequestBody
from llm_providers.models.llm_model import GPT_4o_mini
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    get_dummy_gpt_batch_response,
)

CHUNK_BOUNDS = "0:1000"
SUBJECT = "anchor-mfg.com"
TIMESTAMP = datetime(2026, 8, 12)


@pytest.fixture(autouse=True)
def _registered_catalogs():
    set_rule_catalog_lookup(build_rule_catalog_lookup())
    yield
    set_rule_catalog_lookup(None)


def _make_prompt(name: str) -> Prompt:
    return Prompt(
        s3_version_id="test", name=name, text=f"{name} prompt text", num_tokens=5
    )


def _equipment_metadata() -> KeywordExtractionMetadata:
    """The real metadata the equipment pipeline builds, so the group-size segment
    under test is the one the factory actually plumbs through."""
    prefill = ExtractionPipelineFactory.create_equipment_extraction_pipeline(
        chunk_strategy=EQUIPMENT_CHUNKING_STRAT,
        ontology_version_id="test-ontology-version",
        search_prompt=_make_prompt("equipment_phrase_search"),
        recursive_search_prompt=_make_prompt("equipment_phrase_recursive_search"),
        phrase_relationship_prompt=_make_prompt("equipment_phrase_relationship"),
        phrase_relationship_screening_prompt=_make_prompt(
            "equipment_phrase_relationship_screening"
        ),
        phrase_freehand_grounding_prompt=_make_prompt(
            "equipment_phrase_freehand_grounding"
        ),
        llm_model=GPT_4o_mini,
        model_params=GPTModelParams.with_defaults(),
        created_at=TIMESTAMP,
    )
    return KeywordExtractionMetadata(
        created_at=TIMESTAMP,
        chunk_strat=EQUIPMENT_CHUNKING_STRAT,
        ontology_version_id="test-ontology-version",
        llm_phrase_search=prefill.llm_phrase_search_metadata,
        llm_phrase_recursive_search=prefill.llm_phrase_recursive_search_metadata,
        llm_phrase_relationship=prefill.llm_phrase_relationship_metadata,
        llm_phrase_relationship_screening=prefill.llm_phrase_relationship_screening_metadata,
        llm_phrase_freehand_grounding=prefill.llm_phrase_freehand_grounding_metadata,
    )


def _report(outcome, explanation="because the phrase names the machine"):
    return {"outcome": outcome, "explanation": explanation}


def _categorized(name: str) -> dict:
    """Equipment's full always-reported slot set for one category."""
    return {
        "category": name,
        "FGR-Q1": _report("satisfied"),
        "FGR-Q2": _report("satisfied"),
        "FGR-Q3": _report("satisfied"),
        "FGR-Q4": _report("satisfied"),
        "FGR-QC1": _report("member_level"),
        "chosen": {"rule_id": "FGR-M1", "explanation": "the phrase names it outright"},
    }


def _completed_request(custom_id: str, phrase: str, category: str):
    # ``model_construct``: Beanie 2.0's ``Document.__init__`` reaches for the
    # collection, so a plain ``GPTBatchRequest(...)`` needs a live database.
    request = GPTBatchRequest.model_construct(
        created_at=TIMESTAMP,
        updated_at=TIMESTAMP,
        subject_unique_id=SUBJECT,
        batch_id="Eager",
        num_batches_paired_with=0,
        # The parse path now reads the sent-phrases line back off the request
        # and holds the response to it, so the blob has to exist and carry it.
        request=GPTBatchRequestBlob(
            custom_id=custom_id,
            body=GPTRequestBody(
                model="gpt-4.1",
                messages=[
                    {"role": "system", "content": "the freehand grounding prompt"},
                    {
                        "role": "user",
                        "content": f'nonce\n\n{render_phrase_blocks({phrase: "a summary"})}',
                    },
                ],
                max_completion_tokens=1000,
                response_format={"type": "json_object"},
                temperature=0.0,
                top_p=1.0,
                presence_penalty=0.0,
                frequency_penalty=0.0,
            ),
            input_tokens=0,
        ),
    )
    request.response = get_dummy_gpt_batch_response(
        deferred_at=TIMESTAMP,
        request_custom_id=custom_id,
        dummy_chat_completion_id="dummy_completion_id",
        chat_completion_choice_message=ChatCompletionChoiceMessage(
            role="assistant",
            content=json.dumps(
                {
                    "groundings": [
                        {"phrase": phrase, "categories": [_categorized(category)]}
                    ]
                }
            ),
        ),
    )
    return request


def _bundle(group_req_ids: list[str]) -> KeywordExtractionRequestBundle:
    return KeywordExtractionRequestBundle(
        llm_phrase_search_req_id=None,
        llm_phrase_recursive_search_req_ids=[],
        llm_phrase_relationship_req_ids=[],
        llm_phrase_relationship_screening_req_ids=[],
        llm_phrase_freehand_grounding_req_ids=group_req_ids,
    )


def test_group_index_makes_each_group_its_own_request():
    metadata = _equipment_metadata()
    ids = [
        KeywordFreehandGroundingNode.get_request_custom_id(
            subject_unique_id=SUBJECT,
            field_type=KeywordTypeEnum.equipments,
            chunk_bounds=CHUNK_BOUNDS,
            group_index=group_index,
            metadata=metadata,
        )
        for group_index in range(2)
    ]

    assert ">group>0>chunk>" in ids[0]
    assert ">group>1>chunk>" in ids[1]
    assert len(set(ids)) == 2
    # The cap decides which phrases share a request, so it is part of request
    # identity — a re-run at a different cap must not replay these answers.
    assert all(
        cid.endswith(
            f"|gs={ExtractionPipelineFactory.DEFAULT_FREEHAND_GROUNDING_MAX_PAIRS_PER_REQUEST}"
        )
        for cid in ids
    )


@pytest.mark.asyncio
async def test_groundings_merge_across_the_chunks_groups():
    group_ids = ["chunk-group-0", "chunk-group-1"]
    completed_request_map = {
        group_ids[0]: _completed_request(
            group_ids[0], "three Haas VF-2 machines", "CNC vertical machining center"
        ),
        group_ids[1]: _completed_request(
            group_ids[1], "a 5-axis waterjet table", "waterjet cutter"
        ),
    }

    result = await get_freehand_grounding_result(
        subject_unique_id=SUBJECT,
        field_type=KeywordTypeEnum.equipments,
        chunk_bounds=CHUNK_BOUNDS,
        extraction_bundle=_bundle(group_ids),
        completed_request_map=completed_request_map,
        timestamp=TIMESTAMP,
    )

    assert set(result) == {"three Haas VF-2 machines", "a 5-axis waterjet table"}
    assert list(result["three Haas VF-2 machines"]) == ["CNC vertical machining center"]
    assert list(result["a 5-axis waterjet table"]) == ["waterjet cutter"]


@pytest.mark.asyncio
async def test_a_chunk_with_no_embedded_groups_is_an_error():
    """Every chunk gets at least one group — the zero-screened-phrases case is a
    single dummy-completed request, not an empty list."""
    with pytest.raises(ValueError, match="llm_phrase_freehand_grounding_req_ids"):
        await get_freehand_grounding_result(
            subject_unique_id=SUBJECT,
            field_type=KeywordTypeEnum.equipments,
            chunk_bounds=CHUNK_BOUNDS,
            extraction_bundle=_bundle([]),
            completed_request_map={},
            timestamp=TIMESTAMP,
        )
