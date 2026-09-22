"""The Step 2 grounding node (substep 2 of the build, 2026-09-21): one call
per record group on the ``phrase_grounding`` catalog.

What is held:

1. The request layout: the system text ends with the vocabulary block (the
   dash-line outline, definitions on their own lines); the user message is
   the nonce line, then the record blocks with each phrase under ``subject``
   — and no outline.
2. Request identity: the stage's own token, the group cap segment, the
   ``|ud=`` digest over the payload as sent (subject-keyed).
3. The structural parse with the vocabulary hold: options carry the quote
   and the branch, a proposal rides beside, casing is repaired, an option
   offered with no quote is dropped and recorded, one whose quote is not
   verbatim in the record is kept and marked unverified.
4. The under-answer retry: assessed once, exactly the missing ids re-asked,
   the result reads groups then the retry.

The synthesis derivation (the fold, the group records) is stubbed: the node
under test is the grounding node, and its records come from the synthesis
stage's held answers in production.
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
from core.models.rule_catalog import STAGE_GROUNDING, RuleCatalog
from core.models.skos_concept import Concept
from core.services.phrase_blocks_contract import (
    sent_record_ids_from_user_message,
    sent_records_from_user_message,
)
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


def _catalog() -> RuleCatalog:
    def rule(rid, kind, text):
        return {
            "id": rid, "kind": kind, "reportable": True,
            "report_when": {"condition": "always", "preference": "when_chosen", "proposal": "when_chosen"}[kind],
            "text": text,
        }

    return RuleCatalog.model_validate(
        {
            "catalog_version": "test_grounding_node.1",
            "prompt_name": "test_grounding_node",
            "stage": STAGE_GROUNDING,
            "field_types": ["process_caps"],
            "entity_noun": "manufacturing process",
            "entity_relationships": {"base": "perform", "third_person": "performs", "gerund": "performing"},
            "reporting": "structural",
            "outcome_vocab": {},
            "sections": [
                {"section_id": "evidence", "heading": "Both must hold:", "combinator": "all",
                 "rules": [rule("GR-E1", "condition", "Quote it."), rule("GR-K1", "condition", "Of the kind.")]},
                {"section_id": "matching", "heading": "In order:", "combinator": "ordered",
                 "rules": [rule("GR-M1", "preference", "Exact."), rule("GR-M2", "preference", "Generalizing."), rule("GR-P1", "proposal", "Propose.")]},
            ],
            "published": {},
        }
    )


CATALOG = _catalog()


@pytest.fixture(autouse=True, scope="module")
def registered_catalog():
    set_rule_catalog_lookup(lambda stage, field: CATALOG if stage == STAGE_GROUNDING else None)
    yield
    set_rule_catalog_lookup(None)


class _Field:
    name = "process_caps"


def _concept(name: str, ancestors: list[str], definition: str, alt: list[str] | None = None) -> Concept:
    return Concept(
        name=name, uri=f"urn:{name}", level=len(ancestors) + 1, altLabels=alt or [],
        ancestors=ancestors, children=[], definition=definition,
    )


CONCEPTS = {
    _concept("Machining", [], "Material is removed from a workpiece with cutting tools."),
    _concept("CNC Machining", ["Machining"], "Computer-controlled machine tools remove material.", alt=["CNC"]),
    _concept("Joining", [], "Two or more materials are combined.", alt=["Assembly"]),
}

PAYLOADS = {
    "gaaaaaa1": {"focal_form": "CNC machining", "synthesis": "Acme offers CNC machining of housings on its services page."},
    "gaaaaaa2": {"focal_form": "assembly", "synthesis": "Acme performs final assembly of the housings it machines."},
    "gaaaaaa3": {"focal_form": "quality", "synthesis": "Acme says it is committed to quality."},
}


def _request(result_json: str, user_message: str) -> Any:
    return SimpleNamespace(
        response=SimpleNamespace(result=result_json),
        request=SimpleNamespace(body=SimpleNamespace(user_message=lambda: user_message)),
    )


class _Node(LLMPhraseGroundingNode):
    """The node with its Mongo-backed checks and the synthesis derivation faked."""

    def __init__(self) -> None:
        super().__init__(
            field_type=cast(Any, _Field()),
            next_node=cast(Any, None),
            phrase_grounding_prompt=Prompt(text="INSTRUCTIONS", s3_version_id="v", name="g", num_tokens=1),
            known_concepts=CONCEPTS,
        )
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


def _metadata(cap: int = 50) -> Any:
    return SimpleNamespace(
        llm_phrase_grounding=BatchedInitialGroundingNodeMetadata(
            llm_model=MODEL, model_params=GPTModelParams.with_defaults(),
            prompt_name="process_cap_phrase_grounding", prompt_version_id="pv-g", created_at=T0,
            max_pairs_per_request=cap,
        )
    )


def _bundle() -> ConceptExtractionRequestBundle:
    return ConceptExtractionRequestBundle(search_sub_bounds=[CHUNK], brute=set(), llm_phrase_recursive_tagging_reqs=None)


def _entry(rid, options=(), proposals=(), explanation=None):
    return {
        "record_id": rid,
        "options": [{"option": o, "quote": q, "match": m} for o, q, m in options],
        "proposals": [{"label": l, "quote": q, "explanation": e} for l, q, e in proposals],
        "explanation": explanation,
    }


def _answer(*entries) -> str:
    return json.dumps({"groundings": list(entries)})


@pytest.mark.asyncio
async def test_request_layout_and_identity():
    node, bundle, ctx = _Node(), _bundle(), PipelineContext(subject_name="Acme", subject_text="t")
    await node.embed_request_ids(SUBJECT, ctx, _metadata(), {CHUNK: bundle}, T0)
    (req_id,) = bundle.llm_phrase_grounding_req_ids
    assert f">process_caps>llm_phrase_grounding>group>0>chunk>{CHUNK}>" in req_id
    assert "|gs=50|ud=" in req_id
    assert bundle.llm_phrase_grounding_retry_record_ids is None
    # a second embed changes nothing
    before = bundle.model_dump()
    await node.embed_request_ids(SUBJECT, ctx, _metadata(), {CHUNK: bundle}, T0)
    assert bundle.model_dump() == before
    (req,) = await node.create_batch_requests(
        subject_unique_id=SUBJECT, scraped_text_file=cast(Any, SimpleNamespace(text="t")),
        missing_request_ids={req_id}, metadata=_metadata(), chunked_request_map={CHUNK: bundle},
        pipeline_context=ctx, timestamp=T0, eager=True,
    )
    system, user = req.request.body.system_message(), req.request.body.user_message()
    # the system text: instructions, then the vocabulary block with the dash lines
    assert system.startswith("INSTRUCTIONS\n\nthe vocabulary to match against:\n")
    assert "\nMachining\n— Material is removed" in system
    assert "\n  CNC Machining\n  — also known as: CNC. Computer-controlled" in system
    assert "\nJoining\n— also known as: Assembly. Two or more" in system
    # the user message: the nonce first, the records, no outline
    assert user.startswith("request nonce (ignore): ")
    assert "the vocabulary to match against" not in user and "— Material is removed" not in user
    sent = sent_records_from_user_message(user)
    assert isinstance(sent, dict) and set(sent) == set(PAYLOADS)
    assert sent["gaaaaaa1"]["subject"] == "CNC machining" and "focal_form" not in sent["gaaaaaa1"]
    assert sent_record_ids_from_user_message(user) == sorted(PAYLOADS)


@pytest.mark.asyncio
async def test_structural_parse_with_the_vocabulary_hold_then_the_retry():
    node, bundle, ctx = _Node(), _bundle(), PipelineContext(subject_name="Acme", subject_text="t")
    await node.embed_request_ids(SUBJECT, ctx, _metadata(), {CHUNK: bundle}, T0)
    (req_id,) = bundle.llm_phrase_grounding_req_ids
    (req,) = await node.create_batch_requests(
        subject_unique_id=SUBJECT, scraped_text_file=cast(Any, SimpleNamespace(text="t")),
        missing_request_ids={req_id}, metadata=_metadata(), chunked_request_map={CHUNK: bundle},
        pipeline_context=ctx, timestamp=T0, eager=True,
    )
    user = req.request.body.user_message()
    # the model: record 1 matched (casing off, quote verbatim) with a proposal beside;
    # record 2 offered with an EMPTY quote (dropped → left for the retry);
    # record 3 not answered at all
    node.complete = True
    node.completed = {
        req_id: _request(
            _answer(
                _entry("gaaaaaa1",
                       options=[("cnc machining", "offers CNC machining of housings", "GR-M1")],
                       proposals=[("Housing Machining", "CNC machining of housings", "no option names the part")]),
                _entry("gaaaaaa2", options=[("Joining", "", "GR-M1")]),
            ),
            user,
        )
    }
    await node.embed_request_ids(SUBJECT, ctx, _metadata(), {CHUNK: bundle}, T0)
    assert bundle.llm_phrase_grounding_retry_record_ids == ["gaaaaaa2", "gaaaaaa3"]
    (retry_id,) = bundle.llm_phrase_grounding_retry_req_ids
    assert f">chunk>{CHUNK}>retry>1>group>0>" not in retry_id
    assert f">llm_phrase_grounding>retry>1>group>0>chunk>{CHUNK}>" in retry_id
    assert retry_id in node.get_embedded_request_ids(SUBJECT, {CHUNK: bundle})
    (retry_req,) = await node.create_batch_requests(
        subject_unique_id=SUBJECT, scraped_text_file=cast(Any, SimpleNamespace(text="t")),
        missing_request_ids={retry_id}, metadata=_metadata(), chunked_request_map={CHUNK: bundle},
        pipeline_context=ctx, timestamp=T0, eager=True,
    )
    retry_user = retry_req.request.body.user_message()
    assert sent_record_ids_from_user_message(retry_user) == ["gaaaaaa2", "gaaaaaa3"]
    node.completed[retry_id] = _request(
        _answer(
            _entry("gaaaaaa2", options=[("Assembly", "performs the final assembley", "GR-M1")]),  # a misspelt copy: kept, unverified
            _entry("gaaaaaa3", explanation="a claim about itself, not an operation on the work"),
        ),
        retry_user,
    )
    result = await node.get_result(
        subject_unique_id=SUBJECT, field_type=cast(Any, _Field()), chunk_bounds=CHUNK,
        extraction_bundle=bundle, completed_request_map=node.completed, timestamp=T0,
        allowed_labels=node.allowed_labels(),
    )
    assert set(result) == {"gaaaaaa1", "gaaaaaa2", "gaaaaaa3"}
    one = result["gaaaaaa1"]
    assert set(one.tags) == {"CNC Machining", "Housing Machining"}  # casing repaired; the proposal beside
    assert [(r.rule_id, r.outcome) for r in one.tags["CNC Machining"]] == [("GR-E1", "satisfied"), ("GR-M1", "chosen")]
    assert one.tags["CNC Machining"][0].explanation == "offers CNC machining of housings"
    assert [r.rule_id for r in one.tags["Housing Machining"]] == ["GR-E1", "GR-P1"]
    # the retry's answer under the other name "Assembly": a vocabulary label
    # (ruling 25), stored under the spelling given — as today's pass stores an
    # alias tag — and folded to its concept downstream (the unit-screening
    # substep's job; the census's "alias twins" watch)
    assert set(result["gaaaaaa2"].tags) == {"Assembly"}
    assert [r.outcome for r in result["gaaaaaa2"].tags["Assembly"]] == ["unverified", "chosen"]
    assert result["gaaaaaa3"].tags == {} and (result["gaaaaaa3"].explanation or "").startswith("a claim")
    # the generic reader (no vocabulary): the same answers, nothing repaired or folded
    plain = await node.get_result(
        subject_unique_id=SUBJECT, field_type=cast(Any, _Field()), chunk_bounds=CHUNK,
        extraction_bundle=bundle, completed_request_map=node.completed, timestamp=T0,
    )
    assert set(plain["gaaaaaa1"].tags) == {"cnc machining", "Housing Machining"}
    assert set(plain["gaaaaaa2"].tags) == {"Assembly"}
