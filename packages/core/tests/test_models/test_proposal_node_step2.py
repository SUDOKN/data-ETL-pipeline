"""The Step 2 proposal pass (substep 3 of the build, 2026-09-21).

What is held:

1. OFF (the metadata's proposal node is None): nothing is embedded, the
   embedded-id set is empty and reads as complete, the result is empty.
2. ON: the request carries exactly the records the grounding call left with
   no label — declined ones and ones still unanswered — under the proposal
   stage's own token, in the grounding layout (vocabulary in the system text,
   ``subject``-keyed records); a labelled record never reaches it.
3. Its answers parse through the same structural hold: a match after all, a
   proposal, or a confirmed nothing.
"""

from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest
from llm_providers.models.file_objects.prompt import Prompt
from llm_providers.models.llm_model import LLM_Model
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams

from core.models.deferred_extraction.deferred_concept_extraction import (
    ConceptExtractionRequestBundle,
)
from core.models.extraction_results.concept_extraction_results import (
    BatchedInitialGroundingNodeMetadata,
)
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_grounding_node import (
    LLMPhraseGroundingNode,
)
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_proposal_node import (
    LLMPhraseProposalNode,
)
from core.models.rule_catalog import STAGE_GROUNDING, STAGE_PROPOSAL, RuleCatalog
from core.models.skos_concept import Concept
from core.services.phrase_blocks_contract import sent_record_ids_from_user_message
from core.services.rule_catalog_registry import set_rule_catalog_lookup

SUBJECT = "acme.example"
CHUNK = "0:400"
T0 = datetime(2026, 9, 21, 12, 0, 0)
MODEL = LLM_Model(name="gpt-4.1", max_context_tokens=128000)


@pytest.fixture(autouse=True, scope="module")
def offline_gpt_batch_request_settings():
    from beanie.odm.settings.document import DocumentSettings
    from llm_providers.db_models.gpt_batch_request import GPTBatchRequest

    settings_class = getattr(GPTBatchRequest, "Settings")  # noqa: B009
    settings_vars = {a: getattr(settings_class, a) for a in dir(settings_class) if not a.startswith("__")}
    GPTBatchRequest._document_settings = DocumentSettings(**settings_vars)


def _catalog(stage: str, name: str) -> RuleCatalog:
    def rule(rid, kind, text):
        return {
            "id": rid, "kind": kind, "reportable": True,
            "report_when": {"condition": "always", "preference": "when_chosen", "proposal": "when_chosen"}[kind],
            "text": text,
        }

    return RuleCatalog.model_validate(
        {
            "catalog_version": f"{name}.1", "prompt_name": name, "stage": stage,
            "field_types": ["process_caps"], "entity_noun": "manufacturing process",
            "entity_relationships": {"base": "perform", "third_person": "performs", "gerund": "performing"},
            "reporting": "structural", "outcome_vocab": {},
            "sections": [
                {"section_id": "evidence", "heading": "Both:", "combinator": "all",
                 "rules": [rule("GR-E1", "condition", "Quote it."), rule("GR-K1", "condition", "Of the kind.")]},
                {"section_id": "matching", "heading": "In order:", "combinator": "ordered",
                 "rules": [rule("GR-M1", "preference", "Exact."), rule("GR-M2", "preference", "Generalizing."), rule("GR-P1", "proposal", "Propose.")]},
            ],
            "published": {},
        }
    )


CATALOGS = {STAGE_GROUNDING: _catalog(STAGE_GROUNDING, "test_grounding"), STAGE_PROPOSAL: _catalog(STAGE_PROPOSAL, "test_proposal")}


@pytest.fixture(autouse=True, scope="module")
def registered_catalogs():
    set_rule_catalog_lookup(lambda stage, field: CATALOGS.get(stage))
    yield
    set_rule_catalog_lookup(None)


class _Field:
    name = "process_caps"


def _concept(name: str, definition: str) -> Concept:
    return Concept(name=name, uri=f"urn:{name}", level=1, altLabels=[], ancestors=[], children=[], definition=definition)


CONCEPTS = {_concept("Machining", "Material is removed with cutting tools."), _concept("Joining", "Materials are combined.")}

PAYLOADS = {
    "gaaaaaa1": {"focal_form": "CNC machining", "synthesis": "Acme offers CNC machining of housings."},
    "gaaaaaa2": {"focal_form": "quality", "synthesis": "Acme says it is committed to quality."},
    "gaaaaaa3": {"focal_form": "powder coating", "synthesis": "Acme applies powder coating to its housings."},
    "gaaaaaa4": {"focal_form": "shipping", "synthesis": "Acme ships worldwide."},
}


def _request(result_json: str, user_message: str) -> Any:
    return SimpleNamespace(
        response=SimpleNamespace(result=result_json),
        request=SimpleNamespace(body=SimpleNamespace(user_message=lambda: user_message)),
    )


def _entry(rid, options=(), proposals=(), explanation=None):
    return {
        "record_id": rid,
        "options": [{"option": o, "quote": q, "match": m} for o, q, m in options],
        "proposals": [{"label": l, "quote": q, "explanation": e} for l, q, e in proposals],
        "explanation": explanation,
    }


def _answer(*entries) -> str:
    return json.dumps({"groundings": list(entries)})


class _Grounding(LLMPhraseGroundingNode):
    def __init__(self) -> None:
        super().__init__(field_type=cast(Any, _Field()), next_node=cast(Any, None),
                         phrase_grounding_prompt=Prompt(text="G", s3_version_id="v", name="g", num_tokens=1), known_concepts=CONCEPTS)
        self.complete = False
        self.completed: dict[str, Any] = {}

    def get_upstream_synthesis_map(self, pipeline_context):
        return {}

    async def _chunk_record_payloads(self, **kwargs) -> dict[str, dict[str, Any]]:  # type: ignore[override]
        from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import subject_keyed_payloads
        return subject_keyed_payloads(PAYLOADS)

    async def are_all_requests_complete(self, subject_unique_id, chunked_request_map):  # type: ignore[override]
        return self.complete

    async def get_completed_request_map(self, subject_unique_id, chunked_request_map, all_requests_must_be_complete=True):  # type: ignore[override]
        return self.completed


class _Proposal(LLMPhraseProposalNode):
    def __init__(self, grounding_map: dict[str, Any]) -> None:
        super().__init__(field_type=cast(Any, _Field()), next_node=cast(Any, None),
                         phrase_grounding_prompt=Prompt(text="P", s3_version_id="v", name="p", num_tokens=1), known_concepts=CONCEPTS)
        self._upstream = grounding_map
        self.complete = False
        self.completed: dict[str, Any] = {}

    def get_upstream_synthesis_map(self, pipeline_context):
        return {}

    def get_upstream_grounding_map(self, pipeline_context):
        return self._upstream

    async def _chunk_record_payloads(self, subject_unique_id, chunk_bounds, extraction_bundle, synthesis_map, subject_text, metadata, timestamp):  # type: ignore[override]
        # the group records stubbed; the rest is the node's own derivation
        from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import subject_keyed_payloads
        assert self._grounding_map is not None
        grounding = await LLMPhraseGroundingNode.get_result(
            subject_unique_id=subject_unique_id, field_type=self.field_type, chunk_bounds=chunk_bounds,
            extraction_bundle=extraction_bundle, completed_request_map=self._grounding_map, timestamp=timestamp,
            allowed_labels=self.allowed_labels(),
        )
        labelled = {rid for rid, entry in grounding.items() if entry.tags}
        return subject_keyed_payloads({rid: p for rid, p in PAYLOADS.items() if rid not in labelled})

    async def are_all_requests_complete(self, subject_unique_id, chunked_request_map):  # type: ignore[override]
        return self.complete

    async def get_completed_request_map(self, subject_unique_id, chunked_request_map, all_requests_must_be_complete=True):  # type: ignore[override]
        return self.completed


def _stage(name: str, cap: int = 50) -> BatchedInitialGroundingNodeMetadata:
    return BatchedInitialGroundingNodeMetadata(
        llm_model=MODEL, model_params=GPTModelParams.with_defaults(), prompt_name=name,
        prompt_version_id=f"pv-{name}", created_at=T0, max_pairs_per_request=cap,
    )


def _metadata(proposal_on: bool) -> Any:
    return SimpleNamespace(
        llm_phrase_grounding=_stage("process_cap_phrase_grounding"),
        llm_phrase_proposal=_stage("process_cap_phrase_proposal") if proposal_on else None,
    )


def _bundle() -> ConceptExtractionRequestBundle:
    return ConceptExtractionRequestBundle(search_sub_bounds=[CHUNK], brute=set(), llm_phrase_recursive_tagging_reqs=None)


async def _grounded_bundle() -> tuple[ConceptExtractionRequestBundle, dict[str, Any]]:
    """A chunk after the grounding call: record 1 labelled, 2 declined, 3
    unanswered even after the retry, 4 labelled by a proposal."""
    node, bundle, ctx = _Grounding(), _bundle(), PipelineContext(subject_name="Acme", subject_text="t")
    md = _metadata(proposal_on=False)
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    (req_id,) = bundle.llm_phrase_grounding_req_ids
    (req,) = await node.create_batch_requests(
        subject_unique_id=SUBJECT, scraped_text_file=cast(Any, SimpleNamespace(text="t")),
        missing_request_ids={req_id}, metadata=md, chunked_request_map={CHUNK: bundle}, pipeline_context=ctx, timestamp=T0, eager=True,
    )
    user = req.request.body.user_message()
    node.complete = True
    node.completed = {
        req_id: _request(_answer(
            _entry("gaaaaaa1", options=[("Machining", "offers CNC machining", "GR-M2")]),
            _entry("gaaaaaa2", explanation="a claim about itself"),
            _entry("gaaaaaa4", proposals=[("Shipping", "ships worldwide", "no option")]),
        ), user)
    }
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    assert bundle.llm_phrase_grounding_retry_record_ids == ["gaaaaaa3"]
    (retry_id,) = bundle.llm_phrase_grounding_retry_req_ids
    (retry_req,) = await node.create_batch_requests(
        subject_unique_id=SUBJECT, scraped_text_file=cast(Any, SimpleNamespace(text="t")),
        missing_request_ids={retry_id}, metadata=md, chunked_request_map={CHUNK: bundle}, pipeline_context=ctx, timestamp=T0, eager=True,
    )
    node.completed[retry_id] = _request(_answer(), retry_req.request.body.user_message())  # still nothing for record 3
    return bundle, node.completed


@pytest.mark.asyncio
async def test_off_embeds_nothing_and_reads_as_complete_and_empty():
    bundle, grounding_map = await _grounded_bundle()
    node, ctx = _Proposal(grounding_map), PipelineContext(subject_name="Acme", subject_text="t")
    await node.embed_request_ids(SUBJECT, ctx, _metadata(proposal_on=False), {CHUNK: bundle}, T0)
    assert bundle.llm_phrase_proposal_req_ids == [] and bundle.llm_phrase_proposal_retry_record_ids is None
    assert node.get_embedded_request_ids(SUBJECT, {CHUNK: bundle}) == set()
    assert await node.get_result(
        subject_unique_id=SUBJECT, field_type=cast(Any, _Field()), chunk_bounds=CHUNK,
        extraction_bundle=bundle, completed_request_map={}, timestamp=T0,
    ) == {}


@pytest.mark.asyncio
async def test_on_sends_only_the_unlabelled_records_and_parses_its_answer():
    bundle, grounding_map = await _grounded_bundle()
    node, ctx, md = _Proposal(grounding_map), PipelineContext(subject_name="Acme", subject_text="t"), _metadata(proposal_on=True)
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    (req_id,) = bundle.llm_phrase_proposal_req_ids
    assert f">process_caps>llm_phrase_proposal>group>0>chunk>{CHUNK}>" in req_id and "|gs=50|ud=" in req_id
    assert bundle.llm_phrase_grounding_req_ids  # the grounding slots are untouched
    (req,) = await node.create_batch_requests(
        subject_unique_id=SUBJECT, scraped_text_file=cast(Any, SimpleNamespace(text="t")),
        missing_request_ids={req_id}, metadata=md, chunked_request_map={CHUNK: bundle}, pipeline_context=ctx, timestamp=T0, eager=True,
    )
    system, user = req.request.body.system_message(), req.request.body.user_message()
    assert system.startswith("P\n\nthe vocabulary to match against:\n") and "\nMachining\n— Material is removed" in system
    # exactly the declined record and the unanswered one; the labelled and the proposed never reach the pass
    assert sent_record_ids_from_user_message(user) == ["gaaaaaa2", "gaaaaaa3"]
    node.complete = True
    node.completed = {
        req_id: _request(_answer(
            _entry("gaaaaaa2", explanation="a claim about itself, confirmed"),
            _entry("gaaaaaa3", options=[("Joining", "applies powder coating", "GR-M2")], proposals=[("Powder Coating", "applies powder coating", "the vocabulary lacks it")]),
        ), user)
    }
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    assert bundle.llm_phrase_proposal_retry_record_ids == [] and bundle.llm_phrase_proposal_retry_req_ids == []
    result = await node.get_result(
        subject_unique_id=SUBJECT, field_type=cast(Any, _Field()), chunk_bounds=CHUNK,
        extraction_bundle=bundle, completed_request_map=node.completed, timestamp=T0, allowed_labels=node.allowed_labels(),
    )
    assert set(result) == {"gaaaaaa2", "gaaaaaa3"}
    assert result["gaaaaaa2"].tags == {}
    assert set(result["gaaaaaa3"].tags) == {"Joining", "Powder Coating"}
    assert [r.rule_id for r in result["gaaaaaa3"].tags["Powder Coating"]] == ["GR-E1", "GR-P1"]
