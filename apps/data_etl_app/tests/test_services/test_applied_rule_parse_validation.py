"""End-to-end: a real GPT response string, the real catalogs, the real parsers.

Covers the wiring that the unit tests in packages/core cannot: that the catalog
lookup registered by the app resolves, and that a bad response actually raises out
of the parse path rather than being quietly accepted.

Until 2026-08-11 a large part of this file was about quoted evidence — spans located
in the phrase and its relationship summary, with the anchor-mfg.com and tooling
failures pinned verbatim as regression cases. That field is gone, and with it the
parse-time source texts the parsers used to read back off the request. What remains
is everything the catalog still decides: which rules must appear, what outcomes they
may carry, and whether the report hangs together.
"""

import json

import pytest

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

from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum
from data_etl_app.services.prompt_assembly_service import build_rule_catalog_lookup

PHRASE = "we serve the aerospace and defense markets"


@pytest.fixture(autouse=True)
def _registered_catalogs():
    set_rule_catalog_lookup(build_rule_catalog_lookup())
    yield
    set_rule_catalog_lookup(None)


def _response(applied_rules, identified_entity="Aerospace", extra=None):
    entry = {
        "phrase": PHRASE,
        "identified_entity": identified_entity,
        "applied_rules": applied_rules,
    }
    entry.update(extra or {})
    return json.dumps({"screenings": [entry]})


def _rule(rule_id, outcome, explanation="because the phrase names aerospace"):
    return {"rule_id": rule_id, "outcome": outcome, "explanation": explanation}


def _conforming():
    return [
        _rule("SCR-1", "satisfied"),
        _rule("SCR-2", "satisfied"),
        _rule("SCR-3", "satisfied"),
    ]


def _parse(applied_rules, identified_entity="Aerospace", extra=None):
    return parse_llm_phrase_relationship_screening_result(
        gpt_response=_response(
            applied_rules, identified_entity=identified_entity, extra=extra
        ),
        field_type=ConceptTypeEnum.industries,
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


def test_a_conforming_response_parses():
    result = _parse(_conforming())
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
    rules = _conforming()
    rules[0]["explanation"] = 'the phrase says "aerospace and defense markets"'

    stored = _parse(rules)[PHRASE].applied_rules[0]
    assert stored.explanation == 'the phrase says "aerospace and defense markets"'


def test_an_empty_explanation_is_rejected():
    """A reported rule with no reason at all. The content is not checked — the
    prompt asks it to cite the phrase and its summary, and a paraphrase that
    ignores that is for an annotator to weigh."""
    rules = _conforming()
    rules[1]["explanation"] = ""
    with pytest.raises(AppliedRuleValidationError, match="empty explanation"):
        _parse(rules)


def test_omitting_a_required_rule_is_rejected():
    with pytest.raises(AppliedRuleValidationError, match="did not report"):
        _parse(_conforming()[:2])


def test_guard_reported_as_clear_is_rejected():
    """Guards are reported only when violated, so "violated" is the only outcome
    their vocabulary offers and the only one the prompt lists as allowed."""
    rules = _conforming() + [_rule("SCR-G2", "clear")]
    with pytest.raises(AppliedRuleValidationError, match="cannot have outcome"):
        _parse(rules)


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


def test_violated_guard_is_accepted():
    rules = _conforming() + [_rule("SCR-G2", "violated")]
    verdict = _parse(rules)[PHRASE]
    assert verdict.applied_rules[-1].rule_id == "SCR-G2"
    assert not verdict.passed


def test_the_model_cannot_declare_a_verdict_at_all():
    """`passed` is derived, so the wire schema no longer carries it and a model
    still emitting one is a prompt/schema mismatch rather than something to trust
    over the rules. extra="forbid" is what makes that loud."""
    with pytest.raises(ValueError, match="Invalid response from GPT"):
        _parse(_conforming(), extra={"passed": False})


def test_a_rule_carrying_an_evidence_field_is_rejected():
    """The field is gone from the schema, not merely unused. A model still sending
    one is answering an older prompt — which, with prompts pinned per catalog
    version, means the wrong prompt was published rather than something to ignore."""
    rules = _conforming()
    rules[0]["evidence"] = [{"source": "phrase", "quote": ["aerospace"]}]
    with pytest.raises(ValueError, match="Invalid response from GPT"):
        _parse(rules)


def test_a_failed_attribution_condition_derives_a_reject():
    """The wrong-party case: the industry is real and served, but not by the
    manufacturer. SCR-3 carries it as a plain failed condition — no inverted
    polarity, no member of the conjunction that means reject when satisfied."""
    rules = [
        _rule("SCR-1", "satisfied"),
        _rule("SCR-2", "satisfied"),
        _rule("SCR-3", "failed"),
    ]
    assert not _parse(rules)[PHRASE].passed


def test_incoherent_condition_chain_is_rejected():
    """"no party is shown to serve it" and "it is the manufacturer that serves it"
    cannot both be true. The verdict is right either way, so only the chain check
    catches this."""
    rules = [
        _rule("SCR-1", "satisfied"),
        _rule("SCR-2", "failed"),
        _rule("SCR-3", "satisfied"),
    ]
    with pytest.raises(AppliedRuleValidationError, match="cannot hold when"):
        _parse(rules)

    rules[2] = _rule("SCR-3", "not_triggered")
    assert not _parse(rules)[PHRASE].passed


def test_a_pass_without_a_named_entity_is_rejected():
    """A pass is a claim about a named candidate, so the name has to be there."""
    with pytest.raises(ValueError, match="named no identified_entity"):
        _parse(_conforming(), identified_entity=None)


def test_a_rejected_phrase_may_still_name_the_candidate_it_judged():
    """The converse is allowed on purpose: which candidate was weighed and lost is
    the part a human annotator needs to correct the call."""
    rules = [
        _rule("SCR-1", "satisfied"),
        _rule("SCR-2", "satisfied"),
        _rule("SCR-3", "failed"),
    ]
    assert _parse(rules)[PHRASE].identified_entity == "Aerospace"


def test_unregistered_catalog_fails_loudly_rather_than_skipping_validation():
    """A silent skip would let unvalidated reports reach the database."""
    set_rule_catalog_lookup(None)
    with pytest.raises(RuleCatalogNotRegisteredError):
        _parse(_conforming())


# --- grounding: the option-shaped wire schema -----------------------------------


def _grounding_response(options):
    return json.dumps(
        {"groundings": [{"phrase": PHRASE, "options": options}]}
    )


def _option(name, applied_rules):
    return {"option": name, "applied_rules": applied_rules}


def _qualified():
    return [
        _rule("IGR-Q1", "satisfied"),
        _rule("IGR-Q2", "satisfied"),
        _rule("IGR-M1", "chosen"),
    ]


def test_initial_grounding_parses_the_option_shaped_response():
    """Initial and recursive grounding choose from a supplied list, so the wire
    field is "option". Freehand names its own and calls it a "category"."""
    result = parse_llm_phrase_initial_grounding_result(
        gpt_response=_grounding_response([_option("Aerospace", _qualified())]),
        field_type=ConceptTypeEnum.industries,
    )
    assert list(result[PHRASE]) == ["Aerospace"]


def test_initial_grounding_rejects_the_freehand_category_shape():
    """The two schemas are deliberately not interchangeable — a stage answering in
    the other one's vocabulary is a prompt/schema mismatch, not something to coerce."""
    body = json.dumps(
        {"groundings": [{"phrase": PHRASE, "categories": [{"category": "Aerospace",
                                                           "applied_rules": _qualified()}]}]}
    )
    with pytest.raises(ValueError, match="Invalid response from GPT"):
        parse_llm_phrase_initial_grounding_result(
            gpt_response=body,
            field_type=ConceptTypeEnum.industries,
        )


def test_grounding_is_validated_against_the_real_catalog():
    """Two branches of the matching ladder chosen at once — a defect only the real
    catalog's report policy can name."""
    rules = _qualified() + [_rule("IGR-M2", "chosen")]
    with pytest.raises(AppliedRuleValidationError, match="exactly one chosen rule"):
        parse_llm_phrase_initial_grounding_result(
            gpt_response=_grounding_response([_option("Aerospace", rules)]),
            field_type=ConceptTypeEnum.industries,
        )


def test_a_phrase_that_identified_nothing_may_report_no_rules():
    """SCR-1 failed / SCR-2 not_triggered / SCR-3 not_triggered says exactly what an
    empty list says, on the commonest negative case.

    The derived verdict must come out FALSE here. Read as a conjunction over an
    empty set it would be vacuously true, which would turn the commonest reject in
    the corpus into a silent pass."""
    verdict = _parse([], identified_entity=None)[PHRASE]
    assert verdict.identified_entity is None
    assert verdict.applied_rules == []
    assert not verdict.passed


def test_rules_offered_alongside_a_null_entity_are_still_validated():
    """The shortcut is for having nothing to say, not a way past the checks."""
    with pytest.raises(AppliedRuleValidationError, match="did not report"):
        _parse([_rule("SCR-1", "satisfied")], identified_entity=None)


def test_a_named_candidate_must_still_report_its_rules():
    with pytest.raises(AppliedRuleValidationError, match="did not report"):
        _parse([], identified_entity="Aerospace")


def test_violations_are_collected_across_every_phrase_in_the_response():
    """The censoring fix, end to end. The 2026-08-11 run raised on phrase 2 of 15
    and never validated the other 13, so "one defect per request" was an artefact
    of where the scan stopped rather than a rate. Both phrases here are defective
    and both must be named by the one error."""
    def _entry(phrase):
        return {
            "phrase": phrase,
            "identified_entity": "Aerospace",
            # SCR-3 omitted: an always-reported rule that is simply missing.
            "applied_rules": [
                _rule("SCR-1", "satisfied"),
                _rule("SCR-2", "satisfied"),
            ],
        }

    other_phrase = "we also machine parts for defense"
    with pytest.raises(AppliedRuleValidationError) as excinfo:
        parse_llm_phrase_relationship_screening_result(
            gpt_response=json.dumps(
                {"screenings": [_entry(PHRASE), _entry(other_phrase)]}
            ),
            field_type=ConceptTypeEnum.industries,
        )

    message = str(excinfo.value)
    assert message.startswith("2 rule-report violations:")
    assert PHRASE in message
    assert other_phrase in message


# --- freehand grounding: the category-shaped wire schema ------------------------

EQUIPMENT_PHRASE = "our shop runs three Haas VF-2 machines"


def _freehand_response(categories):
    return json.dumps(
        {"groundings": [{"phrase": EQUIPMENT_PHRASE, "categories": categories}]}
    )


def _categorized():
    return [
        _rule("FGR-Q1", "satisfied"),
        _rule("FGR-Q2", "satisfied"),
        _rule("FGR-Q3", "satisfied"),
        _rule("FGR-M1", "chosen"),
    ]


def test_freehand_grounding_parses_the_category_shaped_response():
    result = parse_llm_phrase_freehand_grounding_result(
        gpt_response=_freehand_response(
            [{"category": "CNC vertical machining center",
              "applied_rules": _categorized()}]
        ),
        field_type=KeywordTypeEnum.equipments,
    )
    assert list(result[EQUIPMENT_PHRASE]) == ["CNC vertical machining center"]


def test_freehand_grounding_takes_more_than_one_category_per_phrase():
    """A phrase naming two distinct categories yields two. The old "exactly one tag
    per phrase" wording contradicted the array schema and made FGR-Q3 vacuous."""
    second = _categorized()
    result = parse_llm_phrase_freehand_grounding_result(
        gpt_response=_freehand_response(
            [
                {"category": "CNC vertical machining center",
                 "applied_rules": _categorized()},
                {"category": "waterjet cutter", "applied_rules": second},
            ]
        ),
        field_type=KeywordTypeEnum.equipments,
    )
    assert list(result[EQUIPMENT_PHRASE]) == [
        "CNC vertical machining center",
        "waterjet cutter",
    ]


def test_freehand_grounding_rejects_the_option_shape():
    body = json.dumps(
        {"groundings": [{"phrase": EQUIPMENT_PHRASE,
                         "options": [{"option": "CNC vertical machining center",
                                      "applied_rules": _categorized()}]}]}
    )
    with pytest.raises(ValueError, match="Invalid response from GPT"):
        parse_llm_phrase_freehand_grounding_result(
            gpt_response=body,
            field_type=KeywordTypeEnum.equipments,
        )


def test_freehand_grounding_has_no_formatting_rule_left_to_report():
    """FGR-F1 is gone: with the output defined as a category, stripping brand and
    model is FGR-Q2's lower bound rather than a formatting step. Reporting it now
    fails as an unknown rule."""
    rules = _categorized() + [_rule("FGR-F1", "applied")]
    with pytest.raises(AppliedRuleValidationError, match="unknown rule"):
        parse_llm_phrase_freehand_grounding_result(
            gpt_response=_freehand_response(
                [{"category": "CNC vertical machining center", "applied_rules": rules}]
            ),
            field_type=KeywordTypeEnum.equipments,
        )
