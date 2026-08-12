"""End-to-end: a real GPT response string, the real catalogs, the real parsers.

Covers the wiring that the unit tests in packages/core cannot: that the catalog
lookup registered by the app resolves, and that a bad response actually raises out
of the parse path rather than being quietly accepted.

Until 2026-08-11 a large part of this file was about quoted evidence — spans located
in the phrase and its relationship summary, with the anchor-mfg.com and tooling
failures pinned verbatim as regression cases. That field is gone, and with it the
parse-time source texts the parsers used to read back off the request.

On 2026-08-12 the rest of the shape checks went the same way, in the other
direction: the wire schema is generated from the catalog now, so a missing rule, an
unknown id, a duplicate, an out-of-vocabulary outcome and a ladder with the wrong
number of branches are all defects that cannot be BUILT. Those tests moved to
packages/core/tests/test_services/test_applied_rule_validation.py, where they assert
against the schema rather than the parser. What is left here is what the catalog
still decides at parse time — whether the report hangs together — plus the
end-to-end wiring, plus the regression case that drove the change.
"""

import json

import pytest
from pydantic import ValidationError

from core.services.applied_rule_validation import AppliedRuleValidationError
from core.services.pipeline_nodes.multi_stage.llm_freehand_grounding_service import (
    parse_llm_phrase_freehand_grounding_result,
)
from core.services.pipeline_nodes.multi_stage.llm_initial_grounding_service import (
    parse_llm_phrase_initial_grounding_result,
)
from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (
    parse_llm_phrase_relationship_screening_result,
)
from core.services.rule_catalog_registry import (
    RuleCatalogNotRegisteredError,
    get_rule_catalog,
    set_rule_catalog_lookup,
)

from core.services.pipeline_nodes.single_stage.llm_binary_classification_service import (
    parse_binary_classification_result_from_gpt_response,
)

from data_etl_app.models.types_and_enums import (
    BinaryClassificationTypeEnum,
    ConceptTypeEnum,
    KeywordTypeEnum,
)
from data_etl_app.services.prompt_assembly_service import build_rule_catalog_lookup

PHRASE = "we serve the aerospace and defense markets"


@pytest.fixture(autouse=True)
def _registered_catalogs():
    set_rule_catalog_lookup(build_rule_catalog_lookup())
    yield
    set_rule_catalog_lookup(None)


def _report(outcome, explanation="because the phrase names aerospace"):
    """One always-reported rule's slot. The rule id is the key that holds it."""
    return {"outcome": outcome, "explanation": explanation}


def _fired(rule_id, explanation="because the phrase names aerospace"):
    """A guard that fired. No outcome: "violated" is the only one it can carry."""
    return {"rule_id": rule_id, "explanation": explanation}


def _conforming():
    return {
        "SCR-1": _report("satisfied"),
        "SCR-2": _report("satisfied"),
        "SCR-3": _report("satisfied"),
    }


def _judged(slots=None, identified_entity="Aerospace", phrase=PHRASE, extra=None):
    entry = {
        "outcome": "judged",
        "phrase": phrase,
        "identified_entity": identified_entity,
        "guards": [],
        **(_conforming() if slots is None else slots),
    }
    entry.update(extra or {})
    return entry


def _parse_entries(*entries):
    return parse_llm_phrase_relationship_screening_result(
        gpt_response=json.dumps({"screenings": list(entries)}),
        field_type=ConceptTypeEnum.industries,
    )


def _parse(slots=None, identified_entity="Aerospace", extra=None):
    return _parse_entries(
        _judged(slots, identified_entity=identified_entity, extra=extra)
    )


def test_every_stage_and_field_type_resolves_to_a_catalog():
    """(stage, field_type) must identify exactly one prompt — the property the
    product freehand split exists to preserve."""
    for concept_type in ConceptTypeEnum:
        for stage in (
            "phrase_relationship_screening",
            "phrase_initial_grounding",
            "phrase_recursive_grounding",
        ):
            assert get_rule_catalog(stage, concept_type.value)

    for keyword_type, stage in [
        ("products", "phrase_relationship_screening"),
        ("contract_products", "phrase_relationship_screening"),
        ("equipments", "phrase_relationship_screening"),
        ("products", "phrase_freehand_grounding"),
        ("contract_products", "phrase_freehand_grounding"),
        ("equipments", "phrase_freehand_grounding"),
    ]:
        assert get_rule_catalog(stage, keyword_type)

    for binary_type in BinaryClassificationTypeEnum:
        assert get_rule_catalog("binary_classification", binary_type.value)


def test_a_conforming_response_parses():
    result = _parse()
    verdict = result[PHRASE]
    assert verdict.passed
    assert verdict.identified_entity == "Aerospace"
    assert [rule.rule_id for rule in verdict.applied_rules] == [
        "SCR-1",
        "SCR-2",
        "SCR-3",
    ]


def test_the_explanation_is_stored_as_the_model_wrote_it():
    """It is the whole of the justification now, and nothing rewrites or relocates
    it between the wire and the record."""
    slots = _conforming()
    slots["SCR-1"] = _report(
        "satisfied", 'the phrase says "aerospace and defense markets"'
    )

    stored = _parse(slots)[PHRASE].applied_rules[0]
    assert stored.explanation == 'the phrase says "aerospace and defense markets"'


def test_an_empty_explanation_is_rejected():
    """A reported rule with no reason at all — one of the two checks the schema
    cannot absorb, since strict mode has no `minLength`. The content is still not
    checked: the prompt asks it to cite the phrase and its summary, and a paraphrase
    that ignores that is for an annotator to weigh."""
    slots = _conforming()
    slots["SCR-2"] = _report("satisfied", "")
    with pytest.raises(AppliedRuleValidationError, match="empty explanation"):
        _parse(slots)


# --- the 2026-08-11 regression --------------------------------------------------


def test_the_report_that_aborted_the_anchor_mfg_run_cannot_be_built():
    """The exact defect, in its original wire vocabulary.

    contract_products screening, group 3, chunk 20273:44984: the phrase 'design'
    came back with identified_entity null AND a single `SCR-1 failed`, matching
    neither the null-entity shortcut nor a full report. It aborted the manufacturer
    after `products` had already completed. Both halves of the old encoding are
    gone — there is no top-level `applied_rules` list and no nullable
    `identified_entity` on a judged entry — so this is a decode failure now.
    """
    body = json.dumps(
        {
            "screenings": [
                {
                    "phrase": "design",
                    "identified_entity": None,
                    "applied_rules": [
                        {
                            "rule_id": "SCR-1",
                            "outcome": "failed",
                            "explanation": "no product category is stated",
                        }
                    ],
                }
            ]
        }
    )
    with pytest.raises(ValueError, match="Invalid response from GPT"):
        parse_llm_phrase_relationship_screening_result(
            gpt_response=body, field_type=ConceptTypeEnum.industries
        )


def test_a_judged_entry_cannot_stop_partway_through_its_conditions():
    """The same defect expressed in the new vocabulary: the model picks `judged`
    and then omits SCR-2 and SCR-3. Required properties, so there is no document."""
    entry = _judged()
    del entry["SCR-2"]
    del entry["SCR-3"]
    with pytest.raises(ValueError, match="Invalid response from GPT"):
        _parse_entries(entry)


# --- the no-candidate branch ----------------------------------------------------


def test_a_phrase_that_identified_nothing_takes_the_other_branch():
    """Locked #21 stands — no candidate means no conditions to report — but it is a
    branch of the union now rather than an empty list, so it cannot be half-taken.

    The derived verdict must come out FALSE. Read as a conjunction over an empty
    set it would be vacuously true, which would turn the commonest reject in the
    corpus into a silent pass.
    """
    verdict = _parse_entries(
        {
            "outcome": "no_candidate",
            "phrase": PHRASE,
            "explanation": "the phrase names markets served, not an industry",
        }
    )[PHRASE]
    assert verdict.identified_entity is None
    assert verdict.applied_rules == []
    assert not verdict.passed


def test_the_no_candidate_branch_records_why():
    """The field the failing report was reaching for. Before it existed, ~45% of
    screened phrases were rejected with nothing recorded about them at all."""
    verdict = _parse_entries(
        {
            "outcome": "no_candidate",
            "phrase": PHRASE,
            "explanation": "the phrase names markets served, not an industry",
        }
    )[PHRASE]
    assert (
        verdict.no_candidate_explanation
        == "the phrase names markets served, not an industry"
    )


def test_a_judged_entry_stores_no_no_candidate_explanation():
    assert _parse()[PHRASE].no_candidate_explanation is None


# --- what the catalog still decides at parse time --------------------------------


def test_violated_guard_is_accepted_and_forces_a_reject():
    verdict = _parse(extra={"guards": [_fired("SCR-G2")]})[PHRASE]
    assert verdict.applied_rules[-1].rule_id == "SCR-G2"
    assert verdict.applied_rules[-1].outcome == "violated"
    assert not verdict.passed


def test_no_screening_prompt_offers_an_outcome_the_parser_rejects():
    """The rendered prompt lists each rule's whole vocabulary as allowed. Anything
    in there that the parser refuses costs a whole group request when the model
    takes it at face value."""
    for stage, field in [
        ("phrase_relationship_screening", concept.value) for concept in ConceptTypeEnum
    ]:
        catalog = get_rule_catalog(stage, field)
        for rule in catalog.walk_rules():
            if not rule.reportable:
                continue
            for outcome in catalog.valid_outcomes(rule.id):
                if rule.report_when == "on_violation":
                    assert outcome == "violated", f"{rule.id} offers {outcome!r}"
                if rule.report_when == "when_chosen":
                    assert outcome == "chosen", f"{rule.id} offers {outcome!r}"


def test_the_model_cannot_declare_a_verdict_at_all():
    """`passed` is derived, so the wire schema no longer carries it and a model
    still emitting one is a prompt/schema mismatch rather than something to trust
    over the rules. extra="forbid" is what makes that loud."""
    with pytest.raises(ValueError, match="Invalid response from GPT"):
        _parse(extra={"passed": False})


def test_a_rule_carrying_an_evidence_field_is_rejected():
    """The field is gone from the schema, not merely unused. A model still sending
    one is answering an older prompt — which, with prompts pinned per catalog
    version, means the wrong prompt was published rather than something to ignore."""
    slots = _conforming()
    slots["SCR-1"] = {
        **_report("satisfied"),
        "evidence": [{"source": "phrase", "quote": ["aerospace"]}],
    }
    with pytest.raises(ValueError, match="Invalid response from GPT"):
        _parse(slots)


def test_a_failed_attribution_condition_derives_a_reject():
    """The wrong-party case: the industry is real and served, but not by the
    manufacturer. SCR-3 carries it as a plain failed condition — no inverted
    polarity, no member of the conjunction that means reject when satisfied."""
    slots = _conforming()
    slots["SCR-3"] = _report("failed")
    assert not _parse(slots)[PHRASE].passed


def test_incoherent_condition_chain_is_rejected():
    """"no party is shown to serve it" and "it is the manufacturer that serves it"
    cannot both be true. The verdict is right either way, so only the chain check
    catches this — which is why it is one of the two checks left at parse time."""
    slots = _conforming()
    slots["SCR-2"] = _report("failed")
    with pytest.raises(AppliedRuleValidationError, match="cannot hold when"):
        _parse(slots)

    slots["SCR-3"] = _report("not_triggered")
    assert not _parse(slots)[PHRASE].passed


def test_a_rejected_phrase_still_names_the_candidate_it_judged():
    """Deliberate: which candidate was weighed and lost is the part a human
    annotator needs to correct the call. It is non-nullable on the judged branch
    for exactly that reason — null now means the other branch."""
    slots = _conforming()
    slots["SCR-3"] = _report("failed")
    assert _parse(slots)[PHRASE].identified_entity == "Aerospace"


def test_a_judged_entry_cannot_leave_the_candidate_unnamed():
    """What used to be a "reported a pass but named no identified_entity" raise at
    parse time is a non-nullable field now."""
    with pytest.raises(ValueError, match="Invalid response from GPT"):
        _parse(identified_entity=None)


def test_violations_are_collected_across_every_phrase_in_the_response():
    """The censoring fix, end to end. The 2026-08-11 run raised on phrase 2 of 15
    and never validated the other 13, so "one defect per request" was an artefact
    of where the scan stopped rather than a rate. Both phrases here are defective
    and both must be named by the one error."""
    other_phrase = "we also machine parts for defense"
    blank = _conforming()
    blank["SCR-2"] = _report("satisfied", "")

    with pytest.raises(AppliedRuleValidationError) as excinfo:
        _parse_entries(
            _judged(blank, phrase=PHRASE),
            _judged(blank, phrase=other_phrase),
        )

    message = str(excinfo.value)
    assert message.startswith("2 rule-report violations:")
    assert PHRASE in message
    assert other_phrase in message


def test_unregistered_catalog_fails_loudly_rather_than_skipping_validation():
    """A silent skip would let unvalidated reports reach the database. It fails
    earlier than it used to: the catalog is needed to BUILD the schema now, not
    merely to check a report against it afterwards."""
    set_rule_catalog_lookup(None)
    with pytest.raises(RuleCatalogNotRegisteredError):
        _parse()


# --- grounding: the option-shaped wire schema -----------------------------------


def _grounding_response(options):
    return json.dumps({"groundings": [{"phrase": PHRASE, "options": options}]})


def _qualified_option(name="Aerospace"):
    return {
        "option": name,
        "IGR-Q1": _report("satisfied"),
        "IGR-Q2": _report("satisfied"),
        "chosen": _fired("IGR-M1"),
    }


def test_initial_grounding_parses_the_option_shaped_response():
    """Initial and recursive grounding choose from a supplied list, so the wire
    field is "option". Freehand names its own and calls it a "category"."""
    result = parse_llm_phrase_initial_grounding_result(
        gpt_response=_grounding_response([_qualified_option()]),
        field_type=ConceptTypeEnum.industries,
    )
    assert list(result[PHRASE]) == ["Aerospace"]


def test_initial_grounding_flattens_the_ladder_branch_into_the_stored_record():
    """`chosen` is one field on the wire and an ordinary AppliedRule in storage,
    with the outcome the schema fixed put back."""
    result = parse_llm_phrase_initial_grounding_result(
        gpt_response=_grounding_response([_qualified_option()]),
        field_type=ConceptTypeEnum.industries,
    )
    stored = result[PHRASE]["Aerospace"]
    assert ("IGR-M1", "chosen") in [(r.rule_id, r.outcome) for r in stored]


def test_initial_grounding_rejects_the_freehand_category_shape():
    """The two schemas are deliberately not interchangeable — a stage answering in
    the other one's vocabulary is a prompt/schema mismatch, not something to coerce."""
    body = json.dumps(
        {
            "groundings": [
                {
                    "phrase": PHRASE,
                    "categories": [
                        {**_qualified_option(), "category": "Aerospace"},
                    ],
                }
            ]
        }
    )
    with pytest.raises(ValueError, match="Invalid response from GPT"):
        parse_llm_phrase_initial_grounding_result(
            gpt_response=body,
            field_type=ConceptTypeEnum.industries,
        )


# --- freehand grounding: the category-shaped wire schema ------------------------

EQUIPMENT_PHRASE = "our shop runs three Haas VF-2 machines"


def _freehand_response(categories):
    return json.dumps(
        {"groundings": [{"phrase": EQUIPMENT_PHRASE, "categories": categories}]}
    )


def _categorized(name="CNC vertical machining center"):
    """Equipment's full always-reported set. FGR-Q4 (the granularity band) and
    FGR-QC1 (the level self-report) joined it on 2026-08-12, and both are reported
    whichever outcome they reach, so a fixture missing either no longer parses."""
    return {
        "category": name,
        "FGR-Q1": _report("satisfied"),
        "FGR-Q2": _report("satisfied"),
        "FGR-Q3": _report("satisfied"),
        "FGR-Q4": _report("satisfied"),
        "FGR-QC1": _report("member_level"),
        "chosen": _fired("FGR-M1"),
    }


def test_freehand_grounding_parses_the_category_shaped_response():
    result = parse_llm_phrase_freehand_grounding_result(
        gpt_response=_freehand_response([_categorized()]),
        field_type=KeywordTypeEnum.equipments,
    )
    assert list(result[EQUIPMENT_PHRASE]) == ["CNC vertical machining center"]


def test_freehand_grounding_takes_more_than_one_category_per_phrase():
    """A phrase naming two distinct categories yields two. The old "exactly one tag
    per phrase" wording contradicted the array schema and made FGR-Q3 vacuous."""
    result = parse_llm_phrase_freehand_grounding_result(
        gpt_response=_freehand_response(
            [_categorized(), _categorized("waterjet cutter")]
        ),
        field_type=KeywordTypeEnum.equipments,
    )
    assert list(result[EQUIPMENT_PHRASE]) == [
        "CNC vertical machining center",
        "waterjet cutter",
    ]


def test_freehand_grounding_rejects_the_option_shape():
    body = json.dumps(
        {
            "groundings": [
                {
                    "phrase": EQUIPMENT_PHRASE,
                    "options": [{**_categorized(), "option": "x"}],
                }
            ]
        }
    )
    with pytest.raises(ValueError, match="Invalid response from GPT"):
        parse_llm_phrase_freehand_grounding_result(
            gpt_response=body,
            field_type=KeywordTypeEnum.equipments,
        )


# --- binary classification: whole-text screening, one unit per request ----------


def _binary_body(slots=None, identified_entity="CNC machining", confidence=90):
    return {
        "identified_entity": identified_entity,
        "confidence": confidence,
        "guards": [],
        **(_mfg_conforming() if slots is None else slots),
    }


def _mfg_conforming():
    return {
        "MFG-1": _report("satisfied"),
        "MFG-2": _report("satisfied"),
        "MFG-3": _report("satisfied"),
    }


def _parse_binary(slots=None, identified_entity="CNC machining", confidence=90):
    return parse_binary_classification_result_from_gpt_response(
        json.dumps(_binary_body(slots, identified_entity, confidence)),
        field_type=BinaryClassificationTypeEnum.is_manufacturer,
    )


def test_binary_answer_is_derived_from_the_rules():
    result = _parse_binary()
    assert result.answer is True
    assert result.identified_entity == "CNC machining"
    assert result.confidence == 90
    assert [rule.rule_id for rule in result.applied_rules] == [
        "MFG-1",
        "MFG-2",
        "MFG-3",
    ]


def test_binary_reason_is_synthesized_from_the_full_trail():
    """Reason survives on the stored shape for its human readers, but it is now a
    rendering of the report rather than a second channel the model writes."""
    slots = _mfg_conforming()
    slots["MFG-1"] = _report("satisfied", 'the text says "we machine parts in-house"')

    reason = _parse_binary(slots).reason
    assert '[MFG-1 satisfied] the text says "we machine parts in-house"' in reason
    assert "[MFG-2 satisfied]" in reason
    assert "[MFG-3 satisfied]" in reason


def test_binary_failed_condition_derives_a_no():
    slots = {
        "MFG-1": _report("satisfied"),
        "MFG-2": _report("failed"),
        "MFG-3": _report("not_triggered"),
    }
    result = _parse_binary(slots)
    assert result.answer is False
    # A reject still names the candidate it got furthest with.
    assert result.identified_entity == "CNC machining"


def test_binary_violated_guard_derives_a_no():
    body = _binary_body()
    body["guards"] = [_fired("MFG-G2")]
    result = parse_binary_classification_result_from_gpt_response(
        json.dumps(body), field_type=BinaryClassificationTypeEnum.is_manufacturer
    )
    assert result.answer is False


def test_binary_positive_answer_requires_a_named_entity():
    """Still a parse-time check here, unlike screening: binary has no second branch
    to move the null case into, so `identified_entity` stays nullable and the
    cross-check against the derived answer stays with it."""
    with pytest.raises(ValueError, match="named no identified_entity"):
        _parse_binary(identified_entity=None)


def test_binary_has_no_empty_rules_shortcut():
    """Screening's no-candidate branch earns its tokens across a fifteen-phrase
    batch; binary is one unit per request and the report is the only diagnostic
    there is, so a no-candidate text reports MFG-1 failed like any other reject."""
    slots = {
        "MFG-1": _report("failed"),
        "MFG-2": _report("not_triggered"),
        "MFG-3": _report("not_triggered"),
    }
    result = _parse_binary(slots, identified_entity=None)
    assert result.answer is False
    assert result.identified_entity is None


def test_binary_incoherent_condition_chain_is_rejected():
    slots = {
        "MFG-1": _report("satisfied"),
        "MFG-2": _report("failed"),
        "MFG-3": _report("satisfied"),
    }
    with pytest.raises(AppliedRuleValidationError, match="cannot hold when"):
        _parse_binary(slots)


def test_binary_model_cannot_declare_answer_or_reason():
    """Both are derived; a model still sending them is answering an older prompt."""
    for extra_field in ("answer", "reason"):
        body = _binary_body()
        body[extra_field] = True if extra_field == "answer" else "because"
        with pytest.raises(ValueError, match="Invalid response from GPT"):
            parse_binary_classification_result_from_gpt_response(
                json.dumps(body),
                field_type=BinaryClassificationTypeEnum.is_manufacturer,
            )
