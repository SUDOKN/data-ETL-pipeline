import pytest
from pydantic import ValidationError

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.catalog_wire_schema import (
    flatten_rule_slots,
    response_model_for,
)
from core.models.rule_catalog import RuleCatalog
from core.services.applied_rule_validation import (
    AppliedRuleValidationError,
    passed_implied_by,
    validate_applied_rules,
)

CATALOG = RuleCatalog.model_validate(
    {
        "catalog_version": "test.1",
        "prompt_name": "test_catalog",
        "stage": "phrase_recursive_grounding",
        "field_types": ["industries"],
        "entity_noun": "industry",
        "entity_relationships": {
            "base": "serve",
            "third_person": "serves",
            "gerund": "serving",
        },
        "outcome_vocab": {
            "condition": ["satisfied", "failed", "not_triggered"],
            # Guards and preferences carry exactly the outcome they are reported on;
            # RuleCatalog rejects a vocabulary that offers more.
            "guard": ["violated"],
            "preference": ["chosen"],
        },
        "sections": [
            {
                "section_id": "qualification",
                "heading": "h",
                "combinator": "all",
                "rules": [
                    {
                        "id": "Q1",
                        "kind": "condition",
                        "reportable": True,
                        "report_when": "always",
                        "text": "t",
                        "combinator": "note",
                        "children": [
                            {
                                "id": "Q1a",
                                "kind": "note",
                                "reportable": False,
                                "report_when": "never",
                                "text": "guidance",
                            }
                        ],
                    }
                ],
            },
            {
                "section_id": "matching",
                "heading": "h",
                "combinator": "ordered",
                "rules": [
                    {
                        "id": "M1",
                        "kind": "preference",
                        "reportable": True,
                        "report_when": "when_chosen",
                        "text": "t",
                    },
                    {
                        "id": "M2",
                        "kind": "preference",
                        "reportable": True,
                        "report_when": "when_chosen",
                        "text": "t",
                    },
                ],
            },
            {
                "section_id": "guards",
                "heading": "h",
                "combinator": "any",
                "rules": [
                    {
                        "id": "G1",
                        "kind": "guard",
                        "reportable": True,
                        "report_when": "on_violation",
                        "text": "t",
                    }
                ],
            },
        ],
        "published": {},
    }
)


def _rule(rule_id, outcome, explanation="because"):
    return AppliedRule(rule_id=rule_id, outcome=outcome, explanation=explanation)


def _valid():
    return [_rule("Q1", "satisfied"), _rule("M1", "chosen")]


def _check(rules):
    validate_applied_rules(catalog=CATALOG, applied_rules=rules, where="test unit")


def test_a_conforming_report_passes():
    _check(_valid())


# --- what a report must contain -------------------------------------------------


def test_an_empty_explanation_is_rejected():
    """The explanation is the whole of the justification since the structured
    evidence array was dropped (2026-08-11), so a blank one is a rule reported with
    no reason at all. Its CONTENT is deliberately unchecked — the prompt asks it to
    cite the phrase and its relationship summary and to quote what it relied on, and
    a paraphrase that ignores that is a weakness for an annotator to weigh, not a
    parse failure that costs the whole group request."""
    with pytest.raises(AppliedRuleValidationError, match="empty explanation"):
        _check([_rule("Q1", "satisfied", explanation=""), _rule("M1", "chosen")])

    with pytest.raises(AppliedRuleValidationError, match="empty explanation"):
        _check([_rule("Q1", "satisfied", explanation="   "), _rule("M1", "chosen")])


def test_a_paraphrasing_explanation_is_accepted():
    """Pins the limit deliberately accepted when evidence went away: nothing here
    can tell a quoting explanation from an inventing one. If that ever needs to be
    caught again, the lever is a structured field, not a check on this prose."""
    _check(
        [
            _rule("Q1", "satisfied", explanation="the phrase is about shipbuilding"),
            _rule("M1", "chosen", explanation="it seemed closest"),
        ]
    )


def test_not_triggered_rule_is_accepted():
    """A condition with nothing to evaluate still reports, and its explanation says
    why rather than citing anything."""
    _check([_rule("Q1", "not_triggered"), _rule("M1", "chosen")])


# --- every defect in a unit is reported, not merely the first --------------------


def test_every_violation_in_a_unit_is_reported_not_just_the_first():
    """The measurement property: three independent defects, three lines, one raise.
    Under a fail-fast scan only the first was ever visible, so the defect rate read
    off a run was a function of where the scan stopped."""
    rules = [
        _rule("Q1", "satisfied", explanation=""),
        _rule("M1", "chosen", explanation="   "),
        _rule("G1", "violated", explanation=""),
    ]
    with pytest.raises(AppliedRuleValidationError) as excinfo:
        _check(rules)

    message = str(excinfo.value)
    assert message.startswith("3 rule-report violations:")
    assert message.count("empty explanation") == 3


def test_a_lone_violation_keeps_its_bare_message():
    """No count header for one problem, so the common case reads exactly as it did
    when each check raised where it stood."""
    with pytest.raises(AppliedRuleValidationError) as excinfo:
        _check([_rule("Q1", "satisfied", explanation=""), _rule("M1", "chosen")])

    assert not str(excinfo.value).startswith("1 rule-report")
    assert str(excinfo.value).startswith("test_catalog: test unit:")


# --- the catalog's reporting policy, now enforced by the wire schema -------------
#
# Everything below used to be a check in `check_applied_rules` with a test above:
# an omitted always-reported rule, an unknown or duplicate id, an outcome outside
# its kind's vocabulary, a note reported, a ladder with none or several branches.
# All of them were cardinality and vocabulary constraints the catalog already
# declared, re-checked by hand because the wire format was a flat list that could
# express none of them. They are now properties of the generated schema, so these
# assert the defect cannot be BUILT rather than that it is caught.


def _wire_model():
    return response_model_for(CATALOG)


def _judged(**overrides):
    """A conforming grounding option in the new wire shape."""
    option = {
        "option": "Shipbuilding",
        "Q1": {"outcome": "satisfied", "explanation": "because"},
        "chosen": {"rule_id": "M1", "explanation": "because"},
        "guards": [],
    }
    option.update(overrides)
    return {"groundings": [{"phrase": "p", "options": [option]}]}


def test_the_new_wire_shape_round_trips_to_the_stored_one():
    parsed = _wire_model().model_validate(_judged())
    option = parsed.groundings[0].options[0]
    assert [(r.rule_id, r.outcome) for r in flatten_rule_slots(CATALOG, option)] == [
        ("Q1", "satisfied"),
        ("M1", "chosen"),
    ]


def test_an_always_reported_rule_cannot_be_omitted():
    """The 2026-08-11 failure, in its general form: a report that stops early.
    `Q1` is a required property, so there is no such document to decode."""
    payload = _judged()
    del payload["groundings"][0]["options"][0]["Q1"]
    with pytest.raises(ValidationError, match="Q1"):
        _wire_model().model_validate(payload)


def test_exactly_one_branch_of_the_ladder_is_chosen():
    """`chosen` is a single required field, so neither none nor several is a
    document that exists — the count check it replaced had to run after the fact."""
    payload = _judged()
    del payload["groundings"][0]["options"][0]["chosen"]
    with pytest.raises(ValidationError, match="chosen"):
        _wire_model().model_validate(payload)


def test_an_outcome_outside_its_kinds_vocabulary_cannot_be_built():
    with pytest.raises(ValidationError, match="satisfied"):
        _wire_model().model_validate(
            _judged(Q1={"outcome": "chosen", "explanation": "because"})
        )


def test_an_unknown_rule_id_cannot_be_built():
    """Rule ids are property names and guard/branch ids are enums, so there is
    nowhere to put one the catalog does not know. Property names also make a
    duplicate report inexpressible, which was a check of its own."""
    with pytest.raises(ValidationError):
        _wire_model().model_validate(
            _judged(chosen={"rule_id": "NOPE", "explanation": "because"})
        )
    with pytest.raises(ValidationError, match="extra_forbidden"):
        _wire_model().model_validate(
            _judged(NOPE={"outcome": "satisfied", "explanation": "because"})
        )


def test_a_note_has_no_slot_to_be_reported_in():
    """Q1a is guidance folded into Q1. Only reportable rules get slots."""
    with pytest.raises(ValidationError, match="extra_forbidden"):
        _wire_model().model_validate(
            _judged(Q1a={"outcome": "satisfied", "explanation": "because"})
        )


def test_a_guard_carries_no_outcome_on_the_wire():
    """`violated` is the only outcome a guard can reach, so the schema fixes it and
    `flatten_rule_slots` puts it back — asking the model to type a constant spends
    completion tokens on a field with no information in it."""
    parsed = _wire_model().model_validate(
        _judged(guards=[{"rule_id": "G1", "explanation": "because"}])
    )
    option = parsed.groundings[0].options[0]
    assert ("G1", "violated") in [
        (r.rule_id, r.outcome) for r in flatten_rule_slots(CATALOG, option)
    ]


# --- outcome vocabularies may only offer outcomes the parser accepts -------------


def _catalog_with_vocab(vocab):
    return RuleCatalog.model_validate(
        {
            "catalog_version": "test.1",
            "prompt_name": "test_catalog",
            "stage": "phrase_relationship_screening",
            "field_types": ["industries"],
            "entity_noun": "industry",
            "entity_relationships": {
                "base": "serve",
                "third_person": "serves",
                "gerund": "serving",
            },
            "outcome_vocab": vocab,
            "sections": [
                {
                    "section_id": "guards",
                    "heading": "h",
                    "combinator": "any",
                    "rules": [
                        {
                            "id": "G1",
                            "kind": "guard",
                            "reportable": True,
                            "report_when": "on_violation",
                            "text": "t",
                        }
                    ],
                }
            ],
            "published": {},
        }
    )


def test_guard_vocabulary_cannot_offer_unreachable_outcomes():
    """The rendered prompt lists a rule's whole vocabulary as allowed. Offering
    "clear" would teach the model an outcome that fails the whole group request."""
    with pytest.raises(ValueError, match="only outcome they can carry"):
        _catalog_with_vocab({"guard": ["clear", "violated", "not_triggered"]})

    assert _catalog_with_vocab({"guard": ["violated"]})


# --- the verdict is READ OFF the rules, never declared alongside them ------------

SCREENING_CATALOG = RuleCatalog.model_validate(
    {
        "catalog_version": "test.1",
        "prompt_name": "test_screening",
        "stage": "phrase_relationship_screening",
        "field_types": ["industries"],
        "entity_noun": "industry",
        "entity_relationships": {
            "base": "serve",
            "third_person": "serves",
            "gerund": "serving",
        },
        "outcome_vocab": {
            "condition": ["satisfied", "failed", "not_triggered"],
            "guard": ["violated"],
        },
        "sections": [
            {
                "section_id": "pass_conditions",
                "heading": "h",
                "combinator": "all",
                "rules": [
                    {
                        "id": "C1",
                        "kind": "condition",
                        "reportable": True,
                        "report_when": "always",
                        "text": "t",
                    },
                    {
                        "id": "C2",
                        "kind": "condition",
                        "reportable": True,
                        "report_when": "always",
                        "text": "t",
                    },
                    {
                        "id": "C3",
                        "kind": "condition",
                        "reportable": True,
                        "report_when": "always",
                        "text": "t",
                    },
                ],
            },
            {
                "section_id": "guards",
                "heading": "h",
                "combinator": "any",
                "rules": [
                    {
                        "id": "G1",
                        "kind": "guard",
                        "reportable": True,
                        "report_when": "on_violation",
                        "text": "t",
                    }
                ],
            },
        ],
        "published": {},
    }
)


def _screen(rules):
    validate_applied_rules(
        catalog=SCREENING_CATALOG, applied_rules=rules, where="test phrase"
    )


def _implies(rules):
    """The verdict the parser will store for these rules. The model declares none,
    so this is the only place a pass/reject comes from."""
    return passed_implied_by(SCREENING_CATALOG, rules)


def _passing():
    return [
        _rule("C1", "satisfied"),
        _rule("C2", "satisfied"),
        _rule("C3", "satisfied"),
    ]


def test_every_condition_satisfied_implies_a_pass():
    _screen(_passing())
    assert _implies(_passing())


def _failing_at(position):
    """A coherent chain that breaks at *position*: satisfied up to it, failed there,
    and nothing downstream left to evaluate."""
    outcomes = ["satisfied"] * position + ["failed"]
    outcomes += ["not_triggered"] * (3 - len(outcomes))
    return [_rule(f"C{i + 1}", outcome) for i, outcome in enumerate(outcomes)]


def test_a_condition_that_did_not_hold_implies_a_reject():
    assert not _implies(_failing_at(0))


def test_not_triggered_condition_does_not_count_as_holding():
    """The pass_conditions section is a conjunction; a condition with nothing to
    evaluate did not hold, so it blocks exactly as a failed one does."""
    rules = [
        _rule("C1", "satisfied"),
        _rule("C2", "not_triggered"),
        _rule("C3", "not_triggered"),
    ]
    assert not _implies(rules)


def test_every_condition_reads_the_same_way():
    """No member of the conjunction inverts: "satisfied" means the same thing for
    all three, which is why the attribution carve-out is a plain condition rather
    than a nested exception with its own polarity."""
    for position in range(3):
        _screen(_failing_at(position))
        assert not _implies(_failing_at(position))


def test_a_condition_cannot_hold_behind_one_that_did_not():
    """The chain: each condition's subject is what the previous one established, so
    "C2 failed, C3 satisfied" asserts both that nothing was established and that it
    holds. The derived verdict is right either way; the record is not."""
    rules = [_rule("C1", "satisfied"), _rule("C2", "failed"), _rule("C3", "satisfied")]
    with pytest.raises(AppliedRuleValidationError, match="cannot hold when"):
        _screen(rules)

    rules[2] = _rule("C3", "not_triggered")
    _screen(rules)


def test_the_chain_is_checked_from_the_first_break_only():
    """C1 breaks the chain, so C3 is judged against C1 rather than against C2 —
    otherwise a not_triggered link would silently repair it."""
    rules = [
        _rule("C1", "failed"),
        _rule("C2", "not_triggered"),
        _rule("C3", "satisfied"),
    ]
    with pytest.raises(AppliedRuleValidationError, match=r"'C1'.*was 'failed'"):
        _screen(rules)


def test_a_wholly_not_triggered_chain_is_fine():
    """What "None of the above" looks like in grounding, and a phrase offering no
    candidate at all in screening."""
    rules = [
        _rule("C1", "not_triggered"),
        _rule("C2", "not_triggered"),
        _rule("C3", "not_triggered"),
    ]
    _screen(rules)
    assert not _implies(rules)


def test_violated_guard_forces_a_reject():
    rules = _passing() + [_rule("G1", "violated")]
    _screen(rules)
    assert not _implies(rules)


def test_no_rules_at_all_is_a_reject_not_a_vacuous_pass():
    """Read as a conjunction over an empty set, "every condition satisfied" is
    trivially true — the wrong answer. The only validated report that arrives with
    no rules is the null-entity shortcut in the screening parser, which is a phrase
    that offered no candidate at all."""
    assert not _implies([])


# --- the sentinel literal is one string in two files ----------------------------


def _catalog_with_sentinel(sentinel, branch_text):
    return RuleCatalog.model_validate(
        {
            "catalog_version": "test.1",
            "prompt_name": "test_catalog",
            "stage": "phrase_recursive_grounding",
            "field_types": ["industries"],
            "entity_noun": "industry",
            "entity_relationships": {
                "base": "serve",
                "third_person": "serves",
                "gerund": "serving",
            },
            "outcome_vocab": {"preference": ["chosen"]},
            "sections": [
                {
                    "section_id": "matching",
                    "heading": "h",
                    "combinator": "ordered",
                    "rules": [
                        {
                            "id": "M1",
                            "kind": "preference",
                            "reportable": True,
                            "report_when": "when_chosen",
                            "text": "an option is chosen",
                        },
                        {
                            "id": "M2",
                            "kind": "preference",
                            "reportable": True,
                            "report_when": "when_chosen",
                            "text": branch_text,
                        },
                    ],
                }
            ],
            "published": {},
            "sentinel_tag": sentinel,
        }
    )


def test_sentinel_must_be_a_reserved_label():
    with pytest.raises(ValueError, match="not a reserved grounding label"):
        _catalog_with_sentinel("None of these", 'return "None of these"')


def test_sentinel_must_be_spelled_verbatim_in_its_branch():
    """The failure this exists for: the prompt tells the model to say one thing and
    the parser looks for another, so the sentinel stops being recognised and flows
    into the results as a discovered label."""
    with pytest.raises(ValueError, match="exactly one preference rule"):
        _catalog_with_sentinel("None of the above", 'return "None of these" instead')

    assert _catalog_with_sentinel(
        "None of the above", 'nothing is recorded except "None of the above"'
    )
