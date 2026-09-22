"""Step 2 descent in depth waves (substep 5 of the build, 2026-09-22): the
walkthrough of 2026-09-22 replayed against the node, embed pass by embed
pass, with the model's answers faked and everything else real.

Vocabulary (depths): Machining 1 > Conventional Machining 2 > CNC Machining 3
(leaf); Surface Finishing 1 > Coating 2 > Painting 3, Powder Coating 3;
Joining 1 (other name Assembly) > Mechanical Joining 2 (leaf).

Records: R1 "CNC machining" matched CNC Machining directly (depth 3); R2
"machining" matched Machining (depth 1); R4 "coating and painting" matched
Coating (2) AND Painting (3); R5 "assembly" matched Joining; R7 "Cerakote"
matched Coating (2) and proposed "Cerakote Coating".

What is held: wave 1's units (depth-1 labels only, no proposals); descents
only from accepted parents over their accepted records; a false child
recorded and not descended; wave 2 = reached ∪ direct depth-2, deduplicated;
V13 removes Painting on R4 after Coating failed on R4; the leaf step behind
the flag; the proposal wave (key 0) after the last depth wave holds EVERY
proposal from every stage; the trail; the deepest-accepted reconcile.
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
from core.models.extraction_results.extraction_node_metadata import (
    BatchedScreeningNodeMetadata,
    DescentNodeMetadata,
)
from core.models.extraction_schemas.descent import PROPOSAL_WAVE
from core.models.extraction_schemas.synthesis import GroupRecord
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_descent_node import (
    LLMPhraseDescentNode,
    _ChunkInputs,
)
from core.models.rule_catalog import STAGE_DESCENT, STAGE_UNIT_SCREENING, RuleCatalog
from core.models.skos_concept import Concept
from core.services.phrase_blocks_contract import sent_record_ids_from_user_message
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    sent_units_from_user_message,
)
from core.services.pipeline_nodes.multi_stage.stage_derivations import deepest_accepted_labels
from core.services.rule_catalog_registry import set_rule_catalog_lookup

SUBJECT = "acme.example"
CHUNK = "0:400"
T0 = datetime(2026, 9, 22, 12, 0, 0)
MODEL = LLM_Model(name="gpt-4.1", max_context_tokens=128000)


@pytest.fixture(autouse=True, scope="module")
def offline_gpt_batch_request_settings():
    from beanie.odm.settings.document import DocumentSettings
    from llm_providers.db_models.gpt_batch_request import GPTBatchRequest

    settings_class = getattr(GPTBatchRequest, "Settings")  # noqa: B009
    settings_vars = {a: getattr(settings_class, a) for a in dir(settings_class) if not a.startswith("__")}
    GPTBatchRequest._document_settings = DocumentSettings(**settings_vars)


def _rule(rid, kind, text, report_when):
    return {"id": rid, "kind": kind, "reportable": True, "report_when": report_when, "text": text}


def _screening_catalog() -> RuleCatalog:
    return RuleCatalog.model_validate({
        "catalog_version": "test_descent_screening.1", "prompt_name": "test_descent_screening",
        "stage": STAGE_UNIT_SCREENING, "field_types": ["process_caps"], "entity_noun": "manufacturing process",
        "entity_relationships": {"base": "perform", "third_person": "performs", "gerund": "performing"},
        "reporting": "structural", "outcome_vocab": {},
        "sections": [
            {"section_id": "conditions", "heading": "Conditions:", "combinator": "all",
             "rules": [_rule("SCR-0", "condition", "Of the kind.", "always"), _rule("SCR-2", "condition", "Own.", "always")]},
            {"section_id": "guards", "heading": "Guards:", "combinator": "any", "rules": [_rule("SCR-G1", "guard", "Current.", "on_violation")]},
        ],
        "published": {},
    })


def _descent_catalog() -> RuleCatalog:
    return RuleCatalog.model_validate({
        "catalog_version": "test_descent.1", "prompt_name": "test_descent",
        "stage": STAGE_DESCENT, "field_types": ["process_caps"], "entity_noun": "manufacturing process",
        "entity_relationships": {"base": "perform", "third_person": "performs", "gerund": "performing"},
        "reporting": "structural", "outcome_vocab": {},
        "sections": [
            {"section_id": "evidence", "heading": "Must hold:", "combinator": "all", "rules": [_rule("RGR-E1", "condition", "Quote the feature.", "always")]},
            {"section_id": "matching", "heading": "In order:", "combinator": "ordered",
             "rules": [_rule("RGR-M1", "preference", "Exact.", "when_chosen"), _rule("RGR-M2", "preference", "Generalizing.", "when_chosen"), _rule("RGR-P1", "proposal", "Propose.", "when_chosen")]},
        ],
        "published": {},
    })


CATALOGS = {STAGE_UNIT_SCREENING: _screening_catalog(), STAGE_DESCENT: _descent_catalog()}


@pytest.fixture(autouse=True, scope="module")
def registered_catalogs():
    set_rule_catalog_lookup(lambda stage, field: CATALOGS.get(stage))
    yield
    set_rule_catalog_lookup(None)


@pytest.fixture(autouse=True)
def no_parse_error_writes(monkeypatch):
    import core.services.pipeline_nodes.multi_stage.llm_descent_node_service as d
    import core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service as s

    async def _no_record(**kwargs):
        return None

    monkeypatch.setattr(d, "record_response_parse_error_capped", _no_record)
    monkeypatch.setattr(s, "record_response_parse_error_capped", _no_record)


class _Field:
    name = "process_caps"
    recursive_grounding_placeholders = ("{{parent_process_cap}}", "{{types_of_process_cap}}")


def _c(name, ancestors, definition, alt=()):
    return Concept(name=name, uri=f"urn:{name}", level=len(ancestors) + 1, altLabels=list(alt), ancestors=list(ancestors), children=[], definition=definition)


CONCEPTS = {
    _c("Machining", [], "Material is removed with cutting tools."),
    _c("Conventional Machining", ["Machining"], "Machining with contact cutting tools."),
    _c("CNC Machining", ["Machining", "Conventional Machining"], "Computer-controlled machine tools remove material."),
    _c("Surface Finishing", [], "The surface is treated."),
    _c("Coating", ["Surface Finishing"], "A layer is applied to the surface."),
    _c("Painting", ["Surface Finishing", "Coating"], "Paint is applied."),
    _c("Powder Coating", ["Surface Finishing", "Coating"], "Dry powder is applied and cured."),
    _c("Joining", [], "Materials are combined.", alt=["Assembly"]),
    _c("Mechanical Joining", ["Joining"], "Parts are fastened together."),
}

RECORDS = {
    "gaaaaaa1": GroupRecord(focal_form="CNC machining", synthesis="Acme offers CNC machining of housings."),
    "gaaaaaa2": GroupRecord(focal_form="machining", synthesis="Acme describes its machining services, milling and turning on CNC equipment."),
    "gaaaaaa4": GroupRecord(focal_form="coating and painting", synthesis="Acme outsources all coating and painting to a partner."),
    "gaaaaaa5": GroupRecord(focal_form="assembly", synthesis="Acme performs final assembly of housings with fasteners."),
    "gaaaaaa7": GroupRecord(focal_form="Cerakote", synthesis="Cerakote is a finishing option Acme offers."),
}


class _Node(LLMPhraseDescentNode):
    def __init__(self) -> None:
        super().__init__(
            field_type=cast(Any, _Field()), next_node=cast(Any, None),
            phrase_descent_prompt=Prompt(text="D {{parent_process_cap}} ::\n{{types_of_process_cap}}", s3_version_id="v", name="d", num_tokens=1),
            phrase_unit_screening_prompt=Prompt(text="S", s3_version_id="v", name="s", num_tokens=1),
            known_concepts=CONCEPTS,
        )
        self.completed: dict[str, Any] = {}

    async def _chunk_inputs(self, *args, **kwargs) -> _ChunkInputs:  # type: ignore[override]
        inputs = _ChunkInputs()
        inputs.group_records = RECORDS
        inputs.direct_by_depth = {
            1: {"Machining": {"gaaaaaa2"}, "Joining": {"gaaaaaa5"}},
            2: {"Coating": {"gaaaaaa4", "gaaaaaa7"}},
            3: {"CNC Machining": {"gaaaaaa1"}, "Painting": {"gaaaaaa4"}},
        }
        inputs.add_proposal("Cerakote Coating", "gaaaaaa7", "grounding")
        return inputs

    async def get_completed_request_map(self, subject_unique_id, chunked_request_map, all_requests_must_be_complete=True):  # type: ignore[override]
        return self.completed


def _metadata(leaf_step: bool = True) -> Any:
    return SimpleNamespace(
        llm_phrase_unit_screening=BatchedScreeningNodeMetadata(
            llm_model=MODEL, model_params=GPTModelParams.with_defaults(), prompt_name="process_cap_phrase_unit_screening",
            prompt_version_id="pv-u", created_at=T0, max_pairs_per_request=50,
        ),
        llm_phrase_descent=DescentNodeMetadata(
            llm_model=MODEL, model_params=GPTModelParams.with_defaults(), prompt_name="process_cap_phrase_descent",
            prompt_version_id="pv-d", created_at=T0, leaf_step=leaf_step,
        ),
        llm_phrase_proposal=None,
    )


def _request(result_json: str, user_message: str, custom_id: str) -> Any:
    return SimpleNamespace(response=SimpleNamespace(result=result_json),
                           request=SimpleNamespace(custom_id=custom_id, body=SimpleNamespace(user_message=lambda: user_message)))


def _screen(*units) -> str:
    return json.dumps({"screenings": [
        {"option": o, "accepted": [{"record_id": r, "evidence": "named", "quote": q} for r, q in acc],
         "not_accepted": [{"record_id": r, "failed_rule": f, "quote": q} for r, f, q in rej]}
        for o, acc, rej in units
    ]})


def _descent(*entries) -> str:
    return json.dumps({"groundings": [
        {"record_id": rid, "options": [{"option": o, "quote": q, "match": "RGR-M1"} for o, q in opts],
         "proposals": [{"label": l, "quote": q, "explanation": "sibling"} for l, q in props], "explanation": expl}
        for rid, opts, props, expl in entries
    ]})


async def _create(node, bundle, ctx, md, ids):
    return await node.create_batch_requests(
        subject_unique_id=SUBJECT, scraped_text_file=cast(Any, SimpleNamespace(text="t")), missing_request_ids=set(ids),
        metadata=md, chunked_request_map={CHUNK: bundle}, pipeline_context=ctx, timestamp=T0, eager=True,
    )


def _complete(node, req, answer: str) -> None:
    node.completed[req.request.custom_id] = _request(answer, req.request.body.user_message(), req.request.custom_id)


@pytest.mark.asyncio
async def test_the_walkthrough_wave_by_wave():
    node, bundle, ctx, md = _Node(), ConceptExtractionRequestBundle(search_sub_bounds=[CHUNK], brute=set(), llm_phrase_recursive_tagging_reqs=None), PipelineContext(subject_name="Acme", subject_text="t"), _metadata()
    screens, descents = bundle.llm_phrase_unit_screening_req_ids, bundle.llm_phrase_descent_reqs

    # --- wave 1: depth-1 labels only; the proposal waits for the proposal wave
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    assert list(screens) == [1] and len(screens[1]) == 1 and 1 not in descents
    (w1,) = await _create(node, bundle, ctx, md, screens[1])
    user = w1.request.body.user_message()
    assert [u["option"] for u in sent_units_from_user_message(user) or []] == ["Joining", "Machining"]
    assert sent_record_ids_from_user_message(user) == ["gaaaaaa2", "gaaaaaa5"]
    _complete(node, w1, _screen(("Joining", [("gaaaaaa5", "final assembly")], []), ("Machining", [("gaaaaaa2", "machining services")], [])))

    # --- descents after wave 1: Machining over R2, Joining over R5
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    assert 2 not in screens and sorted((r.parent, r.leaf) for r in descents[1]) == [("Joining", False), ("Machining", False)]
    reqs = await _create(node, bundle, ctx, md, [r.req_id for r in descents[1]])
    by_parent = {("Joining" if ">parent>Joining>" in r.request.custom_id else "Machining"): r for r in reqs}
    system = by_parent["Machining"].request.body.system_message()
    assert system.startswith("D Machining ::\nConventional Machining\n— Machining with contact")  # the children's dash-line outline
    assert sent_record_ids_from_user_message(by_parent["Machining"].request.body.user_message()) == ["gaaaaaa2"]
    # Machining → Conventional Machining on R2 plus a FALSE child (Painting is real, not a child here)
    _complete(node, by_parent["Machining"], _descent(("gaaaaaa2", [("Conventional Machining", "on CNC equipment"), ("Painting", "milling")], [], None)))
    _complete(node, by_parent["Joining"], _descent(("gaaaaaa5", [("Mechanical Joining", "with fasteners")], [], None)))

    # --- wave 2: reached (Conventional Machining R2, Mechanical Joining R5) ∪ direct depth-2 (Coating R4, R7)
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    (w2,) = await _create(node, bundle, ctx, md, screens[2])
    units2 = {u["option"]: u["records"] for u in sent_units_from_user_message(w2.request.body.user_message()) or []}
    assert units2 == {"Coating": ["gaaaaaa4", "gaaaaaa7"], "Conventional Machining": ["gaaaaaa2"], "Mechanical Joining": ["gaaaaaa5"]}
    _complete(node, w2, _screen(
        ("Coating", [("gaaaaaa7", "finishing option Acme offers")], [("gaaaaaa4", "SCR-2", "outsources all coating")]),
        ("Conventional Machining", [("gaaaaaa2", "on CNC equipment")], []),
        ("Mechanical Joining", [("gaaaaaa5", "with fasteners")], []),
    ))

    # --- descents after wave 2: Conventional Machining {R2}, Coating {R7 only}, and the LEAF STEP for Mechanical Joining
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    assert sorted((r.parent, r.leaf) for r in descents[2]) == [("Coating", False), ("Conventional Machining", False), ("Mechanical Joining", True)]
    reqs = await _create(node, bundle, ctx, md, [r.req_id for r in descents[2]])
    by_parent = {next(p for p in ("Coating", "Conventional Machining", "Mechanical Joining") if f">parent>{p}>" in r.request.custom_id): r for r in reqs}
    assert sent_record_ids_from_user_message(by_parent["Coating"].request.body.user_message()) == ["gaaaaaa7"]  # R4 was rejected
    assert by_parent["Mechanical Joining"].request.body.system_message().endswith("(the vocabulary holds nothing narrower)")
    _complete(node, by_parent["Conventional Machining"], _descent(("gaaaaaa2", [("CNC Machining", "CNC equipment")], [], None)))
    _complete(node, by_parent["Coating"], _descent(("gaaaaaa7", [], [("Ceramic Coating", "Cerakote")], None)))
    _complete(node, by_parent["Mechanical Joining"], _descent(("gaaaaaa5", [], [("Fastened Assembly", "with fasteners")], None)))

    # --- wave 3: CNC Machining {R1 direct, R2 reached} deduplicated; Painting on R4 REMOVED (Coating failed on R4)
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    (w3,) = await _create(node, bundle, ctx, md, screens[3])
    units3 = {u["option"]: u["records"] for u in sent_units_from_user_message(w3.request.body.user_message()) or []}
    assert units3 == {"CNC Machining": ["gaaaaaa1", "gaaaaaa2"]}
    _complete(node, w3, _screen(("CNC Machining", [("gaaaaaa1", "CNC machining"), ("gaaaaaa2", "CNC equipment")], [])))

    # --- descents after wave 3: CNC Machining is a leaf → the leaf step
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    assert [(r.parent, r.leaf) for r in descents[3]] == [("CNC Machining", True)]
    (leaf,) = await _create(node, bundle, ctx, md, [r.req_id for r in descents[3]])
    _complete(node, leaf, _descent(("gaaaaaa1", [], [], "nothing narrower"), ("gaaaaaa2", [], [], "nothing narrower")))

    # --- every depth wave done → the PROPOSAL wave holds every proposal from every stage
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    assert PROPOSAL_WAVE in screens and len(screens[PROPOSAL_WAVE]) == 1
    (wp,) = await _create(node, bundle, ctx, md, screens[PROPOSAL_WAVE])
    unitsp = {u["option"]: u["records"] for u in sent_units_from_user_message(wp.request.body.user_message()) or []}
    assert unitsp == {"Ceramic Coating": ["gaaaaaa7"], "Cerakote Coating": ["gaaaaaa7"], "Fastened Assembly": ["gaaaaaa5"]}
    assert all("meaning" not in u for u in sent_units_from_user_message(wp.request.body.user_message()) or [])
    _complete(node, wp, _screen(("Ceramic Coating", [("gaaaaaa7", "Cerakote")], []), ("Cerakote Coating", [("gaaaaaa7", "Cerakote")], []), ("Fastened Assembly", [], [("gaaaaaa5", "SCR-0", "with fasteners")])))
    before = bundle.model_dump()
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    assert bundle.model_dump() == before  # nothing more to embed
    assert node.get_embedded_request_ids(SUBJECT, {CHUNK: bundle}) == set(node.completed)

    # --- the trail and the reconcile
    trail = await node.get_result(SUBJECT, cast(Any, _Field()), CHUNK, bundle, node.completed, T0, ctx, "t", md)
    assert trail.waves[1].units == {"Joining": ["gaaaaaa5"], "Machining": ["gaaaaaa2"]}
    assert trail.waves[1].false_children == {"Machining": {"gaaaaaa2": ["Painting"]}}
    assert trail.waves[2].screening["gaaaaaa4"]["Coating"].failed_rule == "SCR-2"
    assert trail.waves[3].removed_under_failed_ancestor == {"Painting": ["gaaaaaa4"]}
    assert set(trail.waves[2].leaf) == {"Mechanical Joining"} and set(trail.waves[3].leaf) == {"CNC Machining"}
    assert trail.proposal_sources == {"Ceramic Coating": ["descent:Coating"], "Cerakote Coating": ["grounding"], "Fastened Assembly": ["leaf:Mechanical Joining"]}
    assert trail.proposal_screening["gaaaaaa5"]["Fastened Assembly"].passed is False
    accepted: dict[str, list[str]] = {}
    for wave in trail.waves.values():
        for rid, by_label in wave.screening.items():
            accepted.setdefault(rid, []).extend(l for l, v in by_label.items() if v.passed)
    assert deepest_accepted_labels(accepted, node.ancestors_of) == {
        "gaaaaaa1": ["CNC Machining"], "gaaaaaa2": ["CNC Machining"], "gaaaaaa5": ["Mechanical Joining"], "gaaaaaa7": ["Coating"],
    }


@pytest.mark.asyncio
async def test_leaf_step_off_issues_no_leaf_requests():
    node, bundle, ctx, md = _Node(), ConceptExtractionRequestBundle(search_sub_bounds=[CHUNK], brute=set(), llm_phrase_recursive_tagging_reqs=None), PipelineContext(subject_name="Acme", subject_text="t"), _metadata(leaf_step=False)
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    (w1,) = await _create(node, bundle, ctx, md, bundle.llm_phrase_unit_screening_req_ids[1])
    _complete(node, w1, _screen(("Joining", [("gaaaaaa5", "final assembly")], []), ("Machining", [], [("gaaaaaa2", "SCR-0", "x")])))
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    (j,) = await _create(node, bundle, ctx, md, [r.req_id for r in bundle.llm_phrase_descent_reqs[1]])
    _complete(node, j, _descent(("gaaaaaa5", [("Mechanical Joining", "with fasteners")], [], None)))
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    (w2,) = await _create(node, bundle, ctx, md, bundle.llm_phrase_unit_screening_req_ids[2])
    _complete(node, w2, _screen(("Coating", [("gaaaaaa7", "finishing option Acme offers")], [("gaaaaaa4", "SCR-2", "outsources")]), ("Mechanical Joining", [("gaaaaaa5", "with fasteners")], [])))
    await node.embed_request_ids(SUBJECT, ctx, md, {CHUNK: bundle}, T0)
    assert [(r.parent, r.leaf) for r in bundle.llm_phrase_descent_reqs[2]] == [("Coating", False)]  # no leaf step for Mechanical Joining
