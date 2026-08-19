"""Search-stage sub-windows under ``ChunkingStrategy.search_divisor``.

The divisor splits every chunk into sub-windows for the search + recursive
stages ONLY; the chunk stays the unit for everything from phrase_relationship
down. These tests pin the seams a regression would silently swallow: the
derived geometry (absolute offsets that tile the chunk on line boundaries),
the ``>sub>{bounds}>`` segment in the custom ids (including the contract-product
identity share), one first-search request per sub-window carrying that
sub-window's text, per-sub-window round seeding, and the chunk-level parse
union that keeps the split invisible downstream. Plus the factory override that
makes the whole thing sweepable from the notebook.
"""

import json
from datetime import datetime
from typing import Any, cast

import pytest

import llm_providers.utils.chunk_util as chunk_util
from core.models.chunking_strat import (
    ChunkingStrategy,
    MATERIAL_CAP_CHUNKING_STRAT,
    derive_search_sub_bounds,
)
from core.models.deferred_extraction.deferred_keyword_extraction import (
    KeywordExtractionRequestBundle,
)
from core.models.extraction_results.keyword_extraction_results import (
    KeywordExtractionMetadata,
)
from core.models.pipeline_nodes.multi_stage.keyword.keyword_phrase_search_node import (
    KeywordPhraseSearchNode,
)
from core.models.pipeline_nodes.multi_stage.keyword.keyword_recursive_search_node import (
    KeywordRecursiveSearchNode,
)
from core.services.pipeline_nodes.multi_stage.llm_phrase_search_node_service import (
    create_missing_phrase_search_requests,
    parse_batch_request_result,
)
from data_etl_app.models.pipeline_nodes.multi_stage.keyword.contract_product.contract_product_phrase_search_node import (
    ContractProductPhraseSearchNode,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum
from data_etl_app.services.extraction_pipeline_factory import (
    ExtractionPipelineFactory,
)
from llm_providers.db_models.gpt_batch_request import GPTBatchRequest
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import GPT_4o_mini
from llm_providers.models.open_ai.gpt_batch_request_blob import GPTBatchRequestBlob
from llm_providers.models.open_ai.gpt_batch_response_blob import (
    ChatCompletionChoiceMessage,
)
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams
from llm_providers.models.open_ai.gpt_request_body import GPTRequestBody
from llm_providers.services.gpt_batch_request.gpt_batch_request_service import (
    get_dummy_gpt_batch_response,
)

SUBJECT = "anchor-mfg.com"
TIMESTAMP = datetime(2026, 8, 15)


@pytest.fixture(autouse=True, scope="module")
def _offline_gpt_batch_request_settings():
    """Same one-step offline init the session conftest does for the app's own
    document models: request creation constructs ``GPTBatchRequest(...)``
    directly, and Beanie 2.0 refuses that without ``_document_settings``."""
    from beanie.odm.settings.document import DocumentSettings

    if GPTBatchRequest._document_settings is None:
        settings_class = getattr(GPTBatchRequest, "Settings")
        settings_vars = {
            attr: getattr(settings_class, attr)
            for attr in dir(settings_class)
            if not attr.startswith("__")
        }
        GPTBatchRequest._document_settings = DocumentSettings(**settings_vars)


def _make_prompt(name: str) -> Prompt:
    return Prompt(
        s3_version_id="test", name=name, text=f"{name} prompt text", num_tokens=5
    )


def _equipment_metadata(max_recursive_rounds: int = 1) -> KeywordExtractionMetadata:
    """The real metadata the equipment pipeline builds, so the id segments under
    test are the ones the factory actually plumbs through."""
    prefill = ExtractionPipelineFactory.create_equipment_extraction_pipeline(
        chunk_strategy=ChunkingStrategy(
            overlap=0.15, max_tokens_per_chunk=5000, max_chunks=2, search_divisor=2
        ),
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
        max_recursive_search_rounds=max_recursive_rounds,
    )
    return KeywordExtractionMetadata(
        created_at=TIMESTAMP,
        chunk_strat=prefill.chunk_strategy,
        ontology_version_id="test-ontology-version",
        llm_phrase_search=prefill.llm_phrase_search_metadata,
        llm_phrase_recursive_search=prefill.llm_phrase_recursive_search_metadata,
        llm_phrase_relationship=prefill.llm_phrase_relationship_metadata,
        llm_phrase_relationship_screening=prefill.llm_phrase_relationship_screening_metadata,
        llm_phrase_freehand_grounding=prefill.llm_phrase_freehand_grounding_metadata,
    )


def _bundle(
    sub_bounds: list[str], search_req_ids: list[str] | None = None
) -> KeywordExtractionRequestBundle:
    return KeywordExtractionRequestBundle(
        search_sub_bounds=sub_bounds,
        llm_phrase_search_req_ids=search_req_ids or [],
        llm_phrase_recursive_search_req_ids={},
        llm_phrase_relationship_req_ids=[],
        llm_phrase_relationship_screening_req_ids=[],
        llm_phrase_freehand_grounding_req_ids=[],
    )


# ---------------------------------------------------------------------------
# ChunkingStrategy + geometry
# ---------------------------------------------------------------------------


def test_search_divisor_defaults_to_one_and_rejects_zero():
    strat = ChunkingStrategy(overlap=0.15, max_tokens_per_chunk=5000, max_chunks=2)
    assert strat.search_divisor == 1

    with pytest.raises(ValueError, match="search_divisor"):
        ChunkingStrategy(
            overlap=0.15, max_tokens_per_chunk=5000, max_chunks=2, search_divisor=0
        )


@pytest.mark.asyncio
async def test_divisor_one_reuses_the_chunk_bounds_without_rechunking(monkeypatch):
    def exploding_token_counter(model, text):
        raise AssertionError("divisor=1 must not re-tokenize the chunk")

    monkeypatch.setattr(chunk_util.litellm, "token_counter", exploding_token_counter)

    sub_bounds = await derive_search_sub_bounds(
        chunk_bounds="37:212",
        chunk_text="anything at all",
        chunk_strategy=ChunkingStrategy(
            overlap=0.15, max_tokens_per_chunk=5000, max_chunks=2
        ),
        llm_model=GPT_4o_mini,
    )
    assert sub_bounds == ["37:212"]


@pytest.mark.asyncio
async def test_sub_bounds_are_absolute_and_tile_the_chunk_on_line_boundaries(
    monkeypatch,
):
    """Bounds come back in the CHUNK's coordinate system (character offsets into
    the full subject text), tiling the chunk exactly: start to end, no gaps,
    every boundary on a line break."""

    def one_token_per_line(model, text):
        return max(1, len(text.splitlines())) if text else 0

    monkeypatch.setattr(chunk_util.litellm, "token_counter", one_token_per_line)

    chunk_text = "".join(f"line-{i}\n" for i in range(8))  # 8 lines, 1 token each
    chunk_start = 100
    chunk_bounds = f"{chunk_start}:{chunk_start + len(chunk_text)}"

    sub_bounds = await derive_search_sub_bounds(
        chunk_bounds=chunk_bounds,
        chunk_text=chunk_text,
        chunk_strategy=ChunkingStrategy(
            overlap=0,  # exact tiling is only checkable without overlap
            max_tokens_per_chunk=8,
            max_chunks=2,
            search_divisor=2,
        ),
        llm_model=GPT_4o_mini,
    )

    parsed = [(int(b.split(":")[0]), int(b.split(":")[1])) for b in sub_bounds]
    assert len(parsed) >= 2  # the divisor actually split
    assert parsed[0][0] == chunk_start
    assert parsed[-1][1] == chunk_start + len(chunk_text)
    for (_, prev_end), (next_start, _) in zip(parsed, parsed[1:]):
        assert next_start == prev_end  # no gap, no overlap

    line_starts = {0}
    offset = 0
    for line in chunk_text.splitlines(keepends=True):
        offset += len(line)
        line_starts.add(offset)
    for start, end in parsed:
        assert start - chunk_start in line_starts
        assert end - chunk_start in line_starts


# ---------------------------------------------------------------------------
# Custom ids
# ---------------------------------------------------------------------------


def test_search_custom_id_carries_chunk_and_sub_bounds():
    metadata = _equipment_metadata()
    custom_id = KeywordPhraseSearchNode.get_request_custom_id(
        subject_unique_id=SUBJECT,
        field_type=KeywordTypeEnum.equipments,
        chunk_bounds="0:1000",
        sub_bounds="0:493",
        metadata=metadata,
    )
    assert ">llm_search>chunk>0:1000>sub>0:493>" in custom_id


def test_contract_product_search_id_shares_the_products_identity_per_sub_window():
    """The contract branch answers with the products branch's requests; the sub
    segment must survive that identity swap or the two branches stop sharing."""
    metadata = _equipment_metadata()
    custom_id = ContractProductPhraseSearchNode.get_request_custom_id(
        subject_unique_id=SUBJECT,
        field_type=KeywordTypeEnum.contract_products,
        chunk_bounds="0:1000",
        sub_bounds="0:493",
        metadata=metadata,
    )
    assert f"{SUBJECT}>{KeywordTypeEnum.products.name}>llm_search>" in custom_id
    assert ">chunk>0:1000>sub>0:493>" in custom_id


def test_recursive_custom_id_carries_round_chunk_and_sub_bounds():
    metadata = _equipment_metadata()
    custom_id = KeywordRecursiveSearchNode.get_request_custom_id(
        subject_unique_id=SUBJECT,
        field_type=KeywordTypeEnum.equipments,
        chunk_bounds="0:1000",
        sub_bounds="493:1000",
        round_index=1,
        metadata=metadata,
    )
    assert ">llm_recursive_search>round>1>chunk>0:1000>sub>493:1000>" in custom_id


# ---------------------------------------------------------------------------
# Search stage: embed + request creation per sub-window
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_embed_writes_one_first_search_request_per_sub_window():
    node = KeywordPhraseSearchNode(
        field_type=KeywordTypeEnum.equipments,
        search_prompt=_make_prompt("equipment_phrase_search"),
        next_node=cast(Any, None),
    )
    bundle = _bundle(["0:493", "493:1000"])

    await node.embed_request_ids(
        subject_unique_id=SUBJECT,
        pipeline_context=cast(Any, None),
        metadata=_equipment_metadata(),
        chunked_request_map={"0:1000": bundle},
        timestamp=TIMESTAMP,
    )

    assert len(bundle.llm_phrase_search_req_ids) == 2
    assert ">sub>0:493>" in bundle.llm_phrase_search_req_ids[0]
    assert ">sub>493:1000>" in bundle.llm_phrase_search_req_ids[1]
    assert node.get_embedded_request_ids(
        subject_unique_id=SUBJECT, chunked_request_map={"0:1000": bundle}
    ) == set(bundle.llm_phrase_search_req_ids)


@pytest.mark.asyncio
async def test_embed_refuses_a_bundle_with_no_sub_window_geometry():
    """An empty search_sub_bounds means the deferred field predates the divisor
    — running would silently search nothing, so it must stop instead."""
    node = KeywordPhraseSearchNode(
        field_type=KeywordTypeEnum.equipments,
        search_prompt=_make_prompt("equipment_phrase_search"),
        next_node=cast(Any, None),
    )
    with pytest.raises(ValueError, match="search_sub_bounds"):
        await node.embed_request_ids(
            subject_unique_id=SUBJECT,
            pipeline_context=cast(Any, None),
            metadata=_equipment_metadata(),
            chunked_request_map={"0:1000": _bundle([])},
            timestamp=TIMESTAMP,
        )


@pytest.mark.asyncio
async def test_missing_search_requests_carry_their_own_sub_windows_text():
    subject_text = "A" * 400 + "B" * 600
    bundle = _bundle(
        ["0:400", "400:1000"],
        search_req_ids=["req>sub-a", "req>sub-b"],
    )

    batch_requests = await create_missing_phrase_search_requests(
        deferred_at=TIMESTAMP,
        field_type=KeywordTypeEnum.equipments,
        missing_search_req_ids={"req>sub-b"},
        chunked_request_map={"0:1000": bundle},
        subject_unique_id=SUBJECT,
        subject_text=subject_text,
        search_prompt=_make_prompt("equipment_phrase_search"),
        llm_model=GPT_4o_mini,
        model_params=GPTModelParams.with_defaults(),
        eager=False,
    )

    assert [request.request.custom_id for request in batch_requests] == ["req>sub-b"]
    user_content = batch_requests[0].request.body.messages[-1]["content"]
    assert "B" * 600 in user_content
    assert "A" not in user_content.split("\n\n", 1)[1]  # the sibling's text stayed out


# ---------------------------------------------------------------------------
# Recursive stage: rounds are seeded per sub-window
# ---------------------------------------------------------------------------


class _EquipmentRecursiveNode(KeywordRecursiveSearchNode):
    def get_upstream_first_search_map(self, pipeline_context):
        return pipeline_context[KeywordPhraseSearchNode]


@pytest.mark.asyncio
async def test_recursive_round_zero_is_seeded_for_every_sub_window():
    node = _EquipmentRecursiveNode(
        field_type=KeywordTypeEnum.equipments,
        second_search_prompt=_make_prompt("equipment_phrase_recursive_search"),
        next_node=cast(Any, None),
    )
    bundle = _bundle(
        ["0:493", "493:1000"], search_req_ids=["req>sub-a", "req>sub-b"]
    )

    await node.embed_request_ids(
        subject_unique_id=SUBJECT,
        pipeline_context=cast(Any, None),  # untouched on the seeding path
        metadata=_equipment_metadata(max_recursive_rounds=1),
        chunked_request_map={"0:1000": bundle},
        timestamp=TIMESTAMP,
    )

    rounds_by_sub = bundle.llm_phrase_recursive_search_req_ids
    assert set(rounds_by_sub) == {"0:493", "493:1000"}
    for sub_bounds, round_req_ids in rounds_by_sub.items():
        assert len(round_req_ids) == 1
        assert f">round>0>chunk>0:1000>sub>{sub_bounds}>" in round_req_ids[0]
    assert (
        len(
            node.get_embedded_request_ids(
                subject_unique_id=SUBJECT, chunked_request_map={"0:1000": bundle}
            )
        )
        == 2
    )


# ---------------------------------------------------------------------------
# Chunk-level parse union: the split is invisible downstream
# ---------------------------------------------------------------------------


def _completed_search_request(custom_id: str, phrases: list[str]) -> GPTBatchRequest:
    # ``model_construct``: Beanie 2.0's ``Document.__init__`` reaches for the
    # collection, so a plain ``GPTBatchRequest(...)`` needs a live database.
    request = GPTBatchRequest.model_construct(
        created_at=TIMESTAMP,
        updated_at=TIMESTAMP,
        subject_unique_id=SUBJECT,
        batch_id="Eager",
        num_batches_paired_with=0,
        request=GPTBatchRequestBlob(
            custom_id=custom_id,
            body=GPTRequestBody(
                model="gpt-4.1",
                messages=[
                    {"role": "system", "content": "the search prompt"},
                    {"role": "user", "content": "nonce\n\nsome chunk text"},
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
            content=json.dumps({"phrases": phrases}),
        ),
    )
    return request


@pytest.mark.asyncio
async def test_chunk_level_search_result_is_the_union_across_sub_windows():
    bundle = _bundle(
        ["0:493", "493:1000"], search_req_ids=["req>sub-a", "req>sub-b"]
    )
    completed = {
        "req>sub-a": _completed_search_request("req>sub-a", ["cnc machining", "steel"]),
        "req>sub-b": _completed_search_request("req>sub-b", ["steel", "welding"]),
    }

    result = await parse_batch_request_result(
        subject_unique_id=SUBJECT,
        field_type=KeywordTypeEnum.equipments,
        chunk_bounds="0:1000",
        extraction_bundle=bundle,
        all_phrase_search_req_responses_map=completed,
        deferred_at=TIMESTAMP,
    )

    assert result == {"cnc machining", "steel", "welding"}


# ---------------------------------------------------------------------------
# Factory override
# ---------------------------------------------------------------------------


class _PromptBox:
    """Any prompt attribute the factory asks for, on demand."""

    def __getattr__(self, name: str) -> Prompt:
        return _make_prompt(name)


class _OntologyStub:
    s3_version_id = "test-ontology-version"

    @staticmethod
    def get_concepts_flat(field_type):
        return set()


def test_chunk_strategy_override_applies_only_to_its_field():
    wide = ChunkingStrategy(
        overlap=0.15, max_tokens_per_chunk=20_000, max_chunks=2, search_divisor=4
    )
    pipelines = ExtractionPipelineFactory.create_pipelines(
        prompt_service=cast(Any, _PromptBox()),
        ontology=cast(Any, _OntologyStub()),
        llm_model=GPT_4o_mini,
        model_params=GPTModelParams.with_defaults(),
        created_at=TIMESTAMP,
        chunk_strategy_overrides={ConceptTypeEnum.industries: wide},
    )

    assert pipelines[ConceptTypeEnum.industries].chunk_strategy == wide
    assert (
        pipelines[ConceptTypeEnum.material_caps].chunk_strategy
        == MATERIAL_CAP_CHUNKING_STRAT
    )
