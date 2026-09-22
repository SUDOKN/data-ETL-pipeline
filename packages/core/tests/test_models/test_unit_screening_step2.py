"""Step 2 unit screening (substep 4 of the build, 2026-09-21).

Held here:

1. ``units_for_screening`` with the alias fold (ruling 25): a record tagged
   under an other name and one tagged under the name make ONE unit.
2. ``pack_units``: at most the cap of DISTINCT records per request; an
   oversized unit is cut into sub-units that never share a request; the
   packing is deterministic; no units = one empty group.
3. The node, one wave: the request ids carry the stage token and the wave;
   the request is the manufacturer line, the records ONCE under ``subject``,
   then the UNITS block with the vocabulary's meaning where there is one;
   a valid answer parses to verdicts carrying evidence, failed rule and
   quote; a breach of the unit hold raises (the re-dispatch path).
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

from core.models.deferred_extraction.deferred_phrase_extraction_requests import (
    LLMPhraseExtractionRequestBundle,
)
from core.models.extraction_results.extraction_node_metadata import (
    BatchedScreeningNodeMetadata,
)
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.grounding import RecordGroundingEntry
from core.models.extraction_schemas.synthesis import GroupRecord
from core.models.pipeline_nodes.base.base_node import PipelineContext
from core.models.pipeline_nodes.multi_stage.base.llm_phrase_unit_screening_node import (
    LLMPhraseUnitScreeningNode,
)
from core.models.rule_catalog import STAGE_UNIT_SCREENING, RuleCatalog
from core.services.phrase_blocks_contract import sent_records_from_user_message
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    sent_units_from_user_message,
)
from core.services.pipeline_nodes.multi_stage.stage_derivations import (
    pack_units,
    units_for_screening,
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
    def rule(rid, kind, text, report_when):
        return {"id": rid, "kind": kind, "reportable": True, "report_when": report_when, "text": text}

    return RuleCatalog.model_validate(
        {
            "catalog_version": "test_unit_screening_node.1", "prompt_name": "test_unit_screening_node",
            "stage": STAGE_UNIT_SCREENING, "field_types": ["process_caps"], "entity_noun": "manufacturing process",
            "entity_relationships": {"base": "perform", "third_person": "performs", "gerund": "performing"},
            "reporting": "structural", "outcome_vocab": {},
            "sections": [
                {"section_id": "conditions", "heading": "Conditions:", "combinator": "all",
                 "rules": [rule("SCR-0", "condition", "Of the kind.", "always"), rule("SCR-2", "condition", "The manufacturer's own.", "always")]},
                {"section_id": "guards", "heading": "Guards:", "combinator": "any",
                 "rules": [rule("SCR-G1", "guard", "Current.", "on_violation")]},
            ],
            "published": {},
        }
    )


CATALOG = _catalog()


@pytest.fixture(autouse=True, scope="module")
def registered_catalog():
    set_rule_catalog_lookup(lambda stage, field: CATALOG if stage == STAGE_UNIT_SCREENING else None)
    yield
    set_rule_catalog_lookup(None)


def _entry(*labels: str) -> RecordGroundingEntry:
    return RecordGroundingEntry(tags={l: [AppliedRule(rule_id="GR-E1", outcome="satisfied", explanation="q")] for l in labels})


def test_units_fold_aliases_to_the_name():
    fold = {"assembly": "Joining", "joining": "Joining", "cnc": "CNC Machining"}
    units = units_for_screening(
        {"r1": _entry("Assembly"), "r2": _entry("Joining", "Powder Coating"), "r3": _entry("CNC")},
        fold=lambda label: fold.get(label.casefold(), label),
    )
    assert units == {"CNC Machining": ["r3"], "Joining": ["r1", "r2"], "Powder Coating": ["r2"]}


def test_pack_units_respects_the_record_cap_and_splits_oversized_units():
    units = {"A": ["r1", "r2", "r3"], "B": ["r1", "r4"], "C": ["r5", "r6", "r7", "r8", "r9"], "D": ["r9"]}
    groups = pack_units(units, max_records=4)
    for group in groups:
        records = {rid for _, ids in group for rid in ids}
        assert len(records) <= 4
        labels = [label for label, _ in group]
        assert len(labels) == len(set(labels))  # sub-units of one label never share a request
    pieces = sorted((label, tuple(ids)) for group in groups for label, ids in group)
    assert pieces == [("A", ("r1", "r2", "r3")), ("B", ("r1", "r4")), ("C", ("r5", "r6", "r7", "r8")), ("C", ("r9",)), ("D", ("r9",))]
    assert groups == pack_units(units, max_records=4)  # deterministic
    assert pack_units({}, 50) == [[]]
    with pytest.raises(ValueError):
        pack_units(units, 0)


class _Field:
    name = "process_caps"


RECORDS = {
    "gaaaaaa1": GroupRecord(focal_form="CNC machining", synthesis="Acme offers CNC machining of housings."),
    "gaaaaaa2": GroupRecord(focal_form="assembly", synthesis="Acme performs final assembly of the housings."),
    "gaaaaaa3": GroupRecord(focal_form="powder coating", synthesis="A partner applies powder coating to Acme's housings."),
}
MEANINGS = {"CNC Machining": "Computer-controlled machine tools remove material.", "Joining": "Materials are combined."}


class _Node(LLMPhraseUnitScreeningNode):
    def __init__(self) -> None:
        super().__init__(field_type=cast(Any, _Field()), next_node=cast(Any, None),
                         phrase_unit_screening_prompt=Prompt(text="S", s3_version_id="v", name="s", num_tokens=1))
        self.completed: dict[str, Any] = {}

    def get_upstream_synthesis_map(self, pipeline_context):
        return {}

    async def _chunk_request_payloads(self, subject_unique_id, chunk_bounds, extraction_bundle, pipeline_context, subject_text, metadata, timestamp):  # type: ignore[override]
        from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import build_unit_request_payload
        from core.models.pipeline_nodes.multi_stage.base.llm_phrase_unit_screening_node import unit_screening_metadata_of

        units = await self.get_chunk_units(subject_unique_id, chunk_bounds, extraction_bundle, pipeline_context, timestamp)
        cap = unit_screening_metadata_of(metadata).max_pairs_per_request
        return [build_unit_request_payload(RECORDS, group, meaning_of=self.meaning_of) for group in pack_units(units, cap)]

    async def get_chunk_units(self, subject_unique_id, chunk_bounds, extraction_bundle, pipeline_context, timestamp):  # type: ignore[override]
        return units_for_screening(
            {"gaaaaaa1": _entry("CNC Machining"), "gaaaaaa2": _entry("Assembly"), "gaaaaaa3": _entry("Powder Coating")},
            fold=lambda label: {"assembly": "Joining"}.get(label.casefold(), label),
        )

    def meaning_of(self, label: str):
        return MEANINGS.get(label)


def _metadata(cap: int = 50) -> Any:
    return SimpleNamespace(
        llm_phrase_unit_screening=BatchedScreeningNodeMetadata(
            llm_model=MODEL, model_params=GPTModelParams.with_defaults(), prompt_name="process_cap_phrase_unit_screening",
            prompt_version_id="pv-u", created_at=T0, max_pairs_per_request=cap,
        )
    )


def _request(result_json: str, user_message: str) -> Any:
    return SimpleNamespace(response=SimpleNamespace(result=result_json),
                           request=SimpleNamespace(body=SimpleNamespace(user_message=lambda: user_message)))


def _answer(*entries) -> str:
    return json.dumps({"screenings": list(entries)})


def _unit(option, accepted=(), not_accepted=()):
    return {"option": option,
            "accepted": [{"record_id": r, "evidence": e, "quote": q} for r, e, q in accepted],
            "not_accepted": [{"record_id": r, "failed_rule": f, "quote": q} for r, f, q in not_accepted]}


@pytest.mark.asyncio
async def test_one_wave_request_layout_parse_and_hold(monkeypatch):
    # the parse-error recorder writes the failing request to Mongo; the fake
    # request here has no document, so the recorder is a no-op in this test
    import core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service as svc

    async def _no_record(**kwargs):
        return None

    monkeypatch.setattr(svc, "record_response_parse_error_capped", _no_record)
    node, bundle = _Node(), LLMPhraseExtractionRequestBundle(search_sub_bounds=[CHUNK])
    ctx = PipelineContext(subject_name="Acme Example", subject_text="t")
    await node.embed_request_ids(SUBJECT, ctx, _metadata(), {CHUNK: bundle}, T0)
    (req_id,) = bundle.llm_phrase_unit_screening_req_ids[1]
    assert f">process_caps>llm_phrase_unit_screening>wave>1>group>0>chunk>{CHUNK}>" in req_id and "|gs=50|ud=" in req_id
    assert node.get_embedded_request_ids(SUBJECT, {CHUNK: bundle}) == {req_id}
    before = bundle.model_dump()
    await node.embed_request_ids(SUBJECT, ctx, _metadata(), {CHUNK: bundle}, T0)
    assert bundle.model_dump() == before
    (req,) = await node.create_batch_requests(
        subject_unique_id=SUBJECT, scraped_text_file=cast(Any, SimpleNamespace(text="t")), missing_request_ids={req_id},
        metadata=_metadata(), chunked_request_map={CHUNK: bundle}, pipeline_context=ctx, timestamp=T0, eager=True,
    )
    user = req.request.body.user_message()
    assert "\nthe name of the manufacturer in question: Acme Example\n" in user
    sent = sent_records_from_user_message(user)
    assert isinstance(sent, dict) and set(sent) == set(RECORDS) and sent["gaaaaaa2"]["subject"] == "assembly"
    units = sent_units_from_user_message(user) or []
    assert [u["option"] for u in units] == ["CNC Machining", "Joining", "Powder Coating"]  # the alias folded into Joining
    assert units[0]["meaning"] == MEANINGS["CNC Machining"] and "meaning" not in units[2]
    assert units[1]["records"] == ["gaaaaaa2"]
    # the model's answer, held to the request's own units and records
    node.completed = {
        req_id: _request(
            _answer(
                _unit("CNC Machining", accepted=[("gaaaaaa1", "named", "offers CNC machining")]),
                _unit("Joining", accepted=[("gaaaaaa2", "inferred", "performs final assembly")]),
                _unit("Powder Coating", not_accepted=[("gaaaaaa3", "SCR-2", "A partner applies powder coating")]),
            ),
            user,
        )
    }
    result = await node.get_result(subject_unique_id=SUBJECT, field_type=cast(Any, _Field()), chunk_bounds=CHUNK,
                                   extraction_bundle=bundle, completed_request_map=node.completed, timestamp=T0)
    assert result["gaaaaaa1"]["CNC Machining"].passed and result["gaaaaaa1"]["CNC Machining"].evidence == "named"
    assert result["gaaaaaa2"]["Joining"].evidence == "inferred" and result["gaaaaaa2"]["Joining"].quote == "performs final assembly"
    v = result["gaaaaaa3"]["Powder Coating"]
    assert not v.passed and v.failed_rule == "SCR-2" and v.applied_rules[0].rule_id == "SCR-2"
    # a unit left unanswered is not fatal (2026-09-22): its records get no
    # verdict — they read as "screening dropped" and never ship
    node.completed[req_id] = _request(_answer(_unit("CNC Machining", accepted=[("gaaaaaa1", "named", "offers CNC machining")])), user)
    partial = await node.get_result(subject_unique_id=SUBJECT, field_type=cast(Any, _Field()), chunk_bounds=CHUNK,
                                    extraction_bundle=bundle, completed_request_map=node.completed, timestamp=T0)
    assert set(partial) == {"gaaaaaa1"} and "Joining" not in partial["gaaaaaa1"]


@pytest.mark.asyncio
async def test_a_small_cap_packs_the_wave_into_several_requests():
    node, bundle = _Node(), LLMPhraseExtractionRequestBundle(search_sub_bounds=[CHUNK])
    ctx = PipelineContext(subject_name="Acme Example", subject_text="t")
    await node.embed_request_ids(SUBJECT, ctx, _metadata(cap=1), {CHUNK: bundle}, T0)
    ids = bundle.llm_phrase_unit_screening_req_ids[1]
    assert len(ids) == 3 and all(">wave>1>group>" in i for i in ids)


def test_a_paraphrased_quote_keeps_the_verdict_and_is_marked_unverified():
    """2026-09-22, the first Step 2 run: gpt-4.1 restated the record instead
    of quoting it on 2 of 14 requests, and the strict hold stopped both
    subjects. The quote is evidence for the census, not a gate: a paraphrase
    keeps the acceptance with ``quote_verified=False``; an acceptance with no
    quote is kept unverified; the unit and record holds stay exact."""
    from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
        parse_unit_screening_result,
    )

    catalog = _catalog()
    units = {"Joining": ["gaaaaaa1", "gaaaaaa2"]}
    sent = {
        "gaaaaaa1": {"subject": "assembly", "synthesis": "performs final assembly of housings"},
        "gaaaaaa2": {"subject": "welding", "synthesis": "welds frames in-house"},
    }
    response = _answer(_unit("Joining", accepted=[("gaaaaaa1", "named", "The company performs the final assembly step."), ("gaaaaaa2", "inferred", "")]))
    result = parse_unit_screening_result(response, catalog=catalog, units=units, sent_records=sent)
    assert result["gaaaaaa1"]["Joining"].passed is True and result["gaaaaaa1"]["Joining"].quote_verified is False
    assert result["gaaaaaa2"]["Joining"].passed is True and result["gaaaaaa2"]["Joining"].quote_verified is None
    verbatim = _answer(_unit("Joining", accepted=[("gaaaaaa1", "named", "performs final assembly"), ("gaaaaaa2", "named", "welds frames")]))
    assert parse_unit_screening_result(verbatim, catalog=catalog, units=units, sent_records=sent)["gaaaaaa1"]["Joining"].quote_verified is True


def test_record_axis_slips_are_repaired_or_dropped_never_fatal():
    """2026-09-22, the second Step 2 run: a one-character slip in a record id
    (``g5877cn2c`` for ``g587cn2c``) failed a whole request and killed the
    subject. A slip is repaired when exactly one unanswered sent record is
    one edit away; an id that matches nothing is dropped; a sent record left
    unanswered gets no verdict; a record in both lists reads as not accepted;
    a unit never sent is ignored — all logged, none fatal."""
    from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
        parse_unit_screening_result,
    )

    catalog = _catalog()
    units = {"Joining": ["gaaaaaa1", "gaaaaaa2", "gaaaaaa3"], "Coating": ["gaaaaaa4"]}
    sent = {rid: {"subject": "s", "synthesis": "performs final assembly and welds frames"} for rid in ("gaaaaaa1", "gaaaaaa2", "gaaaaaa3", "gaaaaaa4")}
    response = _answer(
        _unit("Joining", accepted=[("gaaaaaaa1", "named", "final assembly"), ("gaaaaaa3", "named", "welds frames")],
              not_accepted=[("gaaaaaa3", "SCR-0", "welds frames"), ("zzzzzzzz", "SCR-0", "x")]),
        _unit("Never Sent", accepted=[("gaaaaaa1", "named", "x")]),
    )
    result = parse_unit_screening_result(response, catalog=catalog, units=units, sent_records=sent)
    assert result["gaaaaaa1"]["Joining"].passed is True  # repaired from the extra-character id
    assert result["gaaaaaa3"]["Joining"].passed is False  # in both lists: not accepted
    assert "gaaaaaa2" not in result  # unanswered: no verdict
    assert "gaaaaaa4" not in result and "zzzzzzzz" not in result and not any("Never Sent" in v for v in result.values())
