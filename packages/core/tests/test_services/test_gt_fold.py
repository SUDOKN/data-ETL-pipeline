"""The fold's contract: both toggle directions, multi-author latest-wins,
unreviewed-node exclusion — and the rollup that is this instrument's point."""

import copy
from datetime import datetime, timezone

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.screening import ScreeningVerdict
from core.models.ground_truth.audits import (
    AuditVerdict,
    EntityFieldAudit,
    TextFieldAudit,
)
from core.models.ground_truth.rule_tree import AuditedRule, AuditedSection
from core.models.rule_catalog import RuleKind
from core.models.ground_truth.stage_blocks import (
    ChunkGT,
    ExtractedPhraseGT,
    GroundingDerivation,
    HumanScreeningDerivation,
    OovGroundingGT,
    RelationshipGT,
    ScreeningGT,
    ScreeningLLMCopy,
    TagGroundingGT,
)
from core.services.ground_truth.gt_fold import (
    compute_document_truth,
    compute_phrase_truth,
    effective_passed,
    effective_screening,
    effective_tag_groundings,
    effective_text,
    rule_agreement_rollup,
)


def _rule(
    rule_id: str,
    kind: RuleKind = "condition",
    outcome: str = "satisfied",
    reported: bool = True,
):
    return AuditedRule(
        rule_id=rule_id,
        kind=kind,
        outcome=outcome,
        explanation="because" if reported else None,
        reported=reported,
    )


def _sections(*rules):
    return [
        AuditedSection(
            section_id="conditions", combinator="all", applied_rules=list(rules)
        )
    ]


def _audit(
    author="alice@example.com",
    type=AuditVerdict.AGREE,
    corrected_text=None,
    note=None,
):
    return TextFieldAudit(
        type=type,
        corrected_text=corrected_text,
        note=note,
        author_email=author,
        at=datetime(2026, 8, 15, tzinfo=timezone.utc),
        source="api",
    )


def _entity_audit(author="alice@example.com", type=AuditVerdict.AGREE, note=None):
    if type is AuditVerdict.DISAGREE and note is None:
        note = "the derivation carries the replacement"
    return EntityFieldAudit(
        type=type,
        note=note,
        author_email=author,
        at=datetime(2026, 8, 15, tzinfo=timezone.utc),
        source="api",
    )


# --- effective_passed: settled #6, both toggle directions --------------------


def test_flipping_a_condition_to_failed_forces_fail():
    sections = _sections(_rule("SCR-1"), _rule("SCR-2", outcome="failed"))
    assert effective_passed(sections) is False


def test_flipping_the_condition_back_does_not_force_pass_while_a_guard_stands():
    guard = AuditedRule(
        rule_id="SCR-G1",
        kind="guard",
        outcome="violated",
        explanation="fired",
        reported=True,
    )
    sections = _sections(_rule("SCR-1"), guard)
    assert effective_passed(sections) is False


def test_all_conditions_satisfied_and_guards_silent_passes():
    silent_guard = AuditedRule(
        rule_id="SCR-G1", kind="guard", outcome="not_violated", reported=False
    )
    assert effective_passed(_sections(_rule("SCR-1"), silent_guard)) is True


def test_no_evaluated_condition_rejects():
    unevaluated = AuditedRule(
        rule_id="SCR-1", kind="condition", outcome=None, reported=False
    )
    assert effective_passed(_sections(unevaluated)) is False


_MINI_CATALOG = {
    "catalog_version": "mini.1",
    "prompt_name": "mini_screening",
    "stage": "phrase_relationship_screening",
    "field_types": ["equipments"],
    "entity_noun": "entity",
    "entity_relationships": {
        "base": "make",
        "third_person": "makes",
        "gerund": "making",
    },
    "outcome_vocab": {
        "condition": ["satisfied", "failed", "not_triggered"],
        "guard": ["violated"],
    },
    "sections": [
        {
            "section_id": "conditions",
            "heading": "Conditions",
            "combinator": "all",
            "rules": [
                {
                    "id": "SCR-1",
                    "kind": "condition",
                    "reportable": True,
                    "report_when": "always",
                    "text": "the entity is theirs",
                }
            ],
        },
        {
            "section_id": "guards",
            "heading": "Guards",
            "combinator": "any",
            "rules": [
                {
                    "id": "SCR-G1",
                    "kind": "guard",
                    "reportable": True,
                    "report_when": "on_violation",
                    "text": "not the customer's",
                }
            ],
        },
    ],
    "published": {},
}


def test_fold_reproduces_the_stored_cache_on_a_pure_llm_copy():
    from core.models.rule_catalog import RuleCatalog
    from core.services.ground_truth.catalog_template_inflation import (
        inflate_screening_llm_copy,
    )

    catalog = RuleCatalog.model_validate(_MINI_CATALOG)
    judged = inflate_screening_llm_copy(
        catalog,
        ScreeningVerdict(
            passed=True,
            identified_entity="mill",
            applied_rules=[
                AppliedRule(rule_id="SCR-1", outcome="satisfied", explanation="x")
            ],
        ),
    )
    assert effective_passed(judged.sections) is judged.passed

    no_candidate = inflate_screening_llm_copy(
        catalog,
        ScreeningVerdict(
            passed=False,
            identified_entity=None,
            applied_rules=[],
            no_candidate_explanation="nothing named",
        ),
    )
    assert effective_passed(no_candidate.sections) is no_candidate.passed


# --- effective text: latest wins ---------------------------------------------


def test_effective_text_latest_wins_across_authors():
    audits = [
        _audit("alice@example.com", AuditVerdict.DISAGREE, "a lathe"),
        _audit("bob@example.com", AuditVerdict.AGREE),
    ]
    view = effective_text("a mill", audits)
    assert view.text == "a mill" and view.reviewed and view.verdict is AuditVerdict.AGREE


def test_effective_text_disagree_replaces_agree_but_appends():
    replaced = effective_text(
        "a mill", [_audit(type=AuditVerdict.DISAGREE, corrected_text="a lathe")]
    )
    assert replaced.text == "a lathe" and replaced.addendum is None

    amended = effective_text(
        "a mill", [_audit(type=AuditVerdict.AGREE_BUT, corrected_text="5-axis")]
    )
    assert amended.text == "a mill" and amended.addendum == "5-axis"


def test_unreviewed_text_is_marked_not_agreed():
    view = effective_text("a mill", [])
    assert not view.reviewed and view.verdict is None and view.text == "a mill"
    assert view.note is None


def test_effective_text_carries_the_latest_audits_note():
    replaced = effective_text(
        "a mill",
        [
            _audit(
                type=AuditVerdict.DISAGREE,
                corrected_text="a lathe",
                note="the text describes turning, not milling",
            )
        ],
    )
    assert replaced.text == "a lathe"
    assert replaced.note == "the text describes turning, not milling"

    amended = effective_text(
        "a mill",
        [
            _audit(type=AuditVerdict.AGREE_BUT, corrected_text="5-axis", note="why"),
            _audit("bob@example.com", AuditVerdict.AGREE),
        ],
    )
    # Latest wins applies to the note too: bob's agree carries none.
    assert amended.note is None and amended.verdict is AuditVerdict.AGREE


# --- effective screening: multi-author latest wins ----------------------------


def _screening_gt_with_derivations(*outcomes: str) -> ScreeningGT:
    llm_sections = _sections(_rule("SCR-1"))
    gt = ScreeningGT(
        llm_result=ScreeningLLMCopy(
            passed=True, identified_entity="mill", sections=llm_sections
        ),
        audits=[],
    )
    for i, outcome in enumerate(outcomes):
        sections = copy.deepcopy(llm_sections)
        sections[0].applied_rules[0].outcome = outcome
        sections[0].applied_rules[0].audits = [
            _audit(f"author{i}@example.com", AuditVerdict.AGREE)
        ]
        gt.audits.append(
            HumanScreeningDerivation(
                identified_entity="mill",
                audits=[_entity_audit(f"author{i}@example.com")],
                sections=sections,
            )
        )
    return gt


def test_effective_screening_latest_derivation_wins():
    gt = _screening_gt_with_derivations("satisfied", "failed")
    truth = effective_screening(gt)
    assert truth.reviewed and truth.passed is False

    gt = _screening_gt_with_derivations("failed", "satisfied")
    assert effective_screening(gt).passed is True


def test_unreviewed_screening_folds_the_llm_copy():
    gt = _screening_gt_with_derivations()
    truth = effective_screening(gt)
    assert not truth.reviewed and truth.passed is True
    assert truth.identified_entity == "mill"


# --- grounding ----------------------------------------------------------------


def test_replaced_tag_appears_under_its_replacement():
    tags = {
        "Vertical Mill": TagGroundingGT(
            llm_result=_sections(_rule("FGR-1")),
            audits=[
                GroundingDerivation(
                    tag="Milling Machine",
                    audits=[_entity_audit(type=AuditVerdict.DISAGREE)],
                    sections=_sections(_rule("FGR-1")),
                )
            ],
        )
    }
    effective = effective_tag_groundings(tags)
    assert set(effective) == {"Milling Machine"}
    assert effective["Milling Machine"].replaced_from == "Vertical Mill"
    assert effective["Milling Machine"].held and effective["Milling Machine"].reviewed


def test_unreviewed_tag_folds_the_llm_sections():
    tags = {"Vertical Mill": TagGroundingGT(llm_result=_sections(_rule("FGR-1")))}
    effective = effective_tag_groundings(tags)
    assert effective["Vertical Mill"].held and not effective["Vertical Mill"].reviewed


# --- document truth ------------------------------------------------------------


def _chunk_gt() -> ChunkGT:
    return ChunkGT(
        extracted_phrases={
            "cnc mill": ExtractedPhraseGT(
                search_round=1,
                llm_relationship=RelationshipGT(
                    llm_result="a machine they run",
                    audits=[
                        _audit(type=AuditVerdict.DISAGREE, corrected_text="a machine they sell")
                    ],
                ),
                llm_screening=_screening_gt_with_derivations("failed"),
                oov_grounding=OovGroundingGT(
                    tags={
                        "Vertical Mill": TagGroundingGT(
                            llm_result=_sections(_rule("FGR-1"))
                        )
                    }
                ),
            )
        },
        missed_phrases=[],
    )


def test_document_truth_reads_as_the_effective_record():
    truth = compute_document_truth({"0:1000": _chunk_gt()})
    phrase = truth["0:1000"].phrases["cnc mill"]
    assert phrase.relationship.text == "a machine they sell"
    assert phrase.screening is not None
    assert phrase.screening.passed is False and phrase.screening.reviewed
    assert not phrase.tags["Vertical Mill"].reviewed
    assert phrase.llm_declined is False


def test_declined_grounding_is_distinguishable_from_no_grounding_at_all():
    """Both fold to empty ``tags`` — only ``llm_declined`` tells an explicit
    sentinel answer apart from a stage that never answered the phrase."""
    declined = ExtractedPhraseGT(
        search_round=1,
        llm_relationship=RelationshipGT(llm_result="a registration they hold"),
        oov_grounding=OovGroundingGT(tags={}, llm_declined=True),
    )
    unreached = ExtractedPhraseGT(
        search_round=1,
        llm_relationship=RelationshipGT(llm_result="a registration they hold"),
    )
    declined_truth = compute_phrase_truth(declined)
    assert declined_truth.llm_declined is True and declined_truth.tags == {}
    assert compute_phrase_truth(unreached).llm_declined is False


def test_human_tag_on_a_declined_phrase_keeps_the_declination_visible():
    """The contradiction is part of the record: the flag survives beside the
    human-asserted link instead of being erased by it."""
    phrase_gt = ExtractedPhraseGT(
        search_round=1,
        llm_relationship=RelationshipGT(llm_result="a registration they hold"),
        oov_grounding=OovGroundingGT(
            tags={
                "Registration": TagGroundingGT(
                    llm_result=None,
                    audits=[
                        GroundingDerivation(
                            tag="Registration",
                            audits=[_entity_audit()],
                            sections=_sections(_rule("FGR-1")),
                        )
                    ],
                )
            },
            llm_declined=True,
        ),
    )
    truth = compute_phrase_truth(phrase_gt)
    assert truth.llm_declined is True
    assert truth.tags["Registration"].reviewed and truth.tags["Registration"].held


# --- the rollup ------------------------------------------------------------------


def test_rollup_counts_verdicts_and_overrides_per_rule():
    chunks = {"0:1000": _chunk_gt()}
    rollup = rule_agreement_rollup(chunks)
    # The derivation flipped SCR-1 to failed WITH an agree audit on the rule:
    # one agree tally, one override (effective differs from the LLM's).
    assert rollup["SCR-1"].agree == 1
    assert rollup["SCR-1"].overridden == 1
    assert rollup["SCR-1"].disagree == 0


def test_unreviewed_rules_are_excluded_from_the_rollup():
    chunks = {"0:1000": _chunk_gt()}
    rollup = rule_agreement_rollup(chunks)
    # The grounding tag was never reviewed: FGR-1 must not appear at all —
    # matching-by-default is not agreement.
    assert "FGR-1" not in rollup


def test_per_document_rollups_sum_across_documents():
    a = rule_agreement_rollup({"0:1000": _chunk_gt()})
    b = rule_agreement_rollup({"0:1000": _chunk_gt()})
    summed = {
        rule_id: a[rule_id].agree + b[rule_id].agree for rule_id in a.keys() & b.keys()
    }
    assert summed["SCR-1"] == 2
