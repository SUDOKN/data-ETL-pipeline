import pytest

from core.models.extraction_schemas.applied_rule import AppliedRule
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
        _rule("Q1", "satisfied"),
        _rule("Q1", "failed"),
        _rule("M1", "chosen", explanation=""),
        _rule("NOPE", "satisfied"),
    ]
    with pytest.raises(AppliedRuleValidationError) as excinfo:
        _check(rules)

    message = str(excinfo.value)
    assert message.startswith("3 rule-report violations:")
    assert "reported 'Q1' more than once" in message
    assert "empty explanation" in message
    assert "unknown rule 'NOPE'" in message


def test_a_lone_violation_keeps_its_bare_message():
    """No count header for one problem, so the common case reads exactly as it did
    when each check raised where it stood."""
    with pytest.raises(AppliedRuleValidationError) as excinfo:
        _check([_rule("Q1", "satisfied", explanation=""), _rule("M1", "chosen")])

    assert not str(excinfo.value).startswith("1 rule-report")
    assert str(excinfo.value).startswith("test_catalog: test unit:")


def test_an_unknown_rule_id_does_not_suppress_the_checks_after_it():
    """An unknown id skips the per-rule checks that need its kind, and nothing
    else. The missing-rule and one-chosen checks still run over what remains."""
    with pytest.raises(AppliedRuleValidationError) as excinfo:
        _check([_rule("NOPE", "satisfied")])

    message = str(excinfo.value)
    assert "unknown rule 'NOPE'" in message
    assert "did not report" in message
    assert "expected exactly one chosen rule" in message


# --- the catalog's reporting policy ----------------------------------------------


def test_always_reported_rule_cannot_be_omitted():
    with pytest.raises(AppliedRuleValidationError, match=r"did not report \['Q1'\]"):
        _check([_rule("M1", "chosen")])


def test_exactly_one_branch_of_the_ladder_is_chosen():
    with pytest.raises(AppliedRuleValidationError, match="exactly one chosen rule"):
        _check([_rule("Q1", "satisfied"), _rule("M1", "chosen"), _rule("M2", "chosen")])

    with pytest.raises(AppliedRuleValidationError, match="exactly one chosen rule"):
        _check([_rule("Q1", "satisfied")])


def test_preference_reported_with_a_non_chosen_outcome_is_rejected():
    with pytest.raises(AppliedRuleValidationError, match="cannot have outcome"):
        _check([_rule("Q1", "satisfied"), _rule("M1", "skipped")])


def test_guard_is_reported_only_when_violated():
    """"clear" is not in the guard vocabulary any more, so the outcome check catches
    it first; the report-policy check behind it still stands for a guard vocabulary
    built in code rather than loaded from a catalog."""
    with pytest.raises(AppliedRuleValidationError, match="cannot have outcome"):
        _check([_rule("Q1", "satisfied"), _rule("M1", "chosen"), _rule("G1", "clear")])

    _check([_rule("Q1", "satisfied"), _rule("M1", "chosen"), _rule("G1", "violated")])


def test_note_cannot_be_reported():
    with pytest.raises(AppliedRuleValidationError, match="must not be reported"):
        _check([_rule("Q1", "satisfied"), _rule("M1", "chosen"), _rule("Q1a", "satisfied")])


def test_unknown_rule_is_rejected():
    with pytest.raises(AppliedRuleValidationError, match="unknown rule"):
        _check([_rule("Q1", "satisfied"), _rule("M1", "chosen"), _rule("Q9", "satisfied")])


def test_outcome_must_belong_to_the_rules_kind():
    with pytest.raises(AppliedRuleValidationError, match="cannot have outcome"):
        _check([_rule("Q1", "chosen"), _rule("M1", "chosen")])


def test_duplicate_rule_is_rejected():
    with pytest.raises(AppliedRuleValidationError, match="more than once"):
        _check([_rule("Q1", "satisfied"), _rule("Q1", "failed"), _rule("M1", "chosen")])


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
