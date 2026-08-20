"""The audit-submission contract, one rejection branch per test.

Read top to bottom, this file IS the contract: what a derivation may keep,
what it must replace, what silence means, and who gets to say so.
"""

import copy
from datetime import datetime, timezone
from typing import Any

import pytest

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.screening import ScreeningVerdict
from core.models.ground_truth.audits import (
    AuditVerdict,
    EntityFieldAudit,
    TextFieldAudit,
)
from core.models.ground_truth.stage_blocks import (
    ChunkGT,
    ExtractedPhraseGT,
    GroundingDerivation,
    HumanScreeningDerivation,
    MissedPhraseEntry,
    RelationshipGT,
    ScreeningGT,
)
from core.models.rule_catalog import RuleCatalog
from core.services.ground_truth.audit_submission_validation import (
    AuditSubmissionError,
    append_text_audit,
    submit_grounding_derivation,
    submit_missed_phrase,
    submit_screening_derivation,
)
from core.services.ground_truth.catalog_template_inflation import (
    inflate_applied_rules,
    inflate_screening_llm_copy,
)

_CATALOG = RuleCatalog.model_validate(
    {
        "catalog_version": "synthetic_screening.1",
        "prompt_name": "synthetic_screening",
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
            "preference": ["chosen"],
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
                        "children": [
                            {
                                "id": "SCR-1-note",
                                "kind": "note",
                                "reportable": False,
                                "report_when": "never",
                                "text": "judge by use",
                            }
                        ],
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
            {
                "section_id": "matching",
                "heading": "Matching",
                "combinator": "ordered",
                "rules": [
                    {
                        "id": "SCR-M1",
                        "kind": "preference",
                        "reportable": True,
                        "report_when": "when_chosen",
                        "text": "exact",
                    },
                    {
                        "id": "SCR-M2",
                        "kind": "preference",
                        "reportable": True,
                        "report_when": "when_chosen",
                        "text": "generalize",
                    },
                ],
            },
        ],
        "published": {},
    }
)

ALICE = "alice@example.com"
BOB = "bob@example.com"


def _audit(author=ALICE, type=AuditVerdict.AGREE, corrected_text=None):
    return TextFieldAudit(
        type=type,
        corrected_text=corrected_text,
        author_email=author,
        at=datetime(2026, 8, 15, tzinfo=timezone.utc),
        source="api",
    )


def _entity_audit(author=ALICE, type=AuditVerdict.AGREE, note=None):
    return EntityFieldAudit(
        type=type,
        note=note,
        author_email=author,
        at=datetime(2026, 8, 15, tzinfo=timezone.utc),
        source="api",
    )


def _screening_gt() -> ScreeningGT:
    verdict = ScreeningVerdict(
        passed=True,
        identified_entity="cnc mill",
        applied_rules=[
            AppliedRule(rule_id="SCR-1", outcome="satisfied", explanation="stated")
        ],
    )
    return ScreeningGT(
        llm_result=inflate_screening_llm_copy(_CATALOG, verdict), audits=[]
    )


def _fill(sections, outcomes: dict[str, str]):
    """Set outcomes on a skeleton/copy; returns the same (mutated) sections."""
    for section in sections:
        for rule in section.applied_rules:
            if rule.rule_id in outcomes:
                rule.outcome = outcomes[rule.rule_id]
                rule.explanation = "human derivation"
    return sections


def _fresh_sections(outcomes={"SCR-1": "satisfied", "SCR-M1": "chosen"}):
    return _fill(
        inflate_applied_rules(_CATALOG, [], synthesize_all_on_empty=True), outcomes
    )


def _copy_of_llm(gt: ScreeningGT):
    return copy.deepcopy(gt.llm_result.sections)


# --- append_text_audit: PH-7 ------------------------------------------------


def test_same_author_resubmission_pops_stack_top_only():
    audits = []
    append_text_audit(audits, _audit(ALICE))
    append_text_audit(audits, _audit(BOB))
    append_text_audit(audits, _audit(ALICE, AuditVerdict.AGREE_BUT, "an addendum"))
    # Alice's first entry survives — only a same-author STACK-TOP is popped.
    assert [a.author_email for a in audits] == [ALICE, BOB, ALICE]

    append_text_audit(audits, _audit(ALICE, AuditVerdict.AGREE_BUT, "newer addendum"))
    assert [a.author_email for a in audits] == [ALICE, BOB, ALICE]
    assert audits[-1].corrected_text == "newer addendum"


# --- screening: keeping the entity ------------------------------------------


def test_agreeing_derivation_mirroring_the_copy_appends():
    gt = _screening_gt()
    derivation = HumanScreeningDerivation(
        identified_entity="cnc mill",
        audits=[_entity_audit(ALICE)],
        sections=_copy_of_llm(gt),
    )
    submit_screening_derivation(
        gt, derivation, author_email=ALICE, catalog=_CATALOG
    )
    assert gt.audits == [derivation]


def test_derivation_with_no_audit_anywhere_asserts_nothing():
    gt = _screening_gt()
    silent = HumanScreeningDerivation(
        identified_entity="cnc mill", audits=[], sections=_copy_of_llm(gt)
    )
    with pytest.raises(AuditSubmissionError, match="asserts nothing"):
        submit_screening_derivation(gt, silent, author_email=ALICE, catalog=_CATALOG)


def test_foreign_authored_entry_inside_a_submission_is_rejected():
    gt = _screening_gt()
    derivation = HumanScreeningDerivation(
        identified_entity="cnc mill",
        audits=[_entity_audit(BOB)],
        sections=_copy_of_llm(gt),
    )
    with pytest.raises(AuditSubmissionError, match="one submission, one author"):
        submit_screening_derivation(
            gt, derivation, author_email=ALICE, catalog=_CATALOG
        )


def test_changed_outcome_without_an_audit_is_a_silent_edit():
    gt = _screening_gt()
    sections = _fill(_copy_of_llm(gt), {"SCR-1": "failed"})
    derivation = HumanScreeningDerivation(
        identified_entity="cnc mill", audits=[_entity_audit(ALICE)], sections=sections
    )
    with pytest.raises(AuditSubmissionError, match="no silent edits"):
        submit_screening_derivation(
            gt, derivation, author_email=ALICE, catalog=_CATALOG
        )


def test_changed_outcome_with_an_audit_is_recorded():
    gt = _screening_gt()
    sections = _fill(_copy_of_llm(gt), {"SCR-1": "failed"})
    sections[0].applied_rules[0].audits.append(
        _audit(ALICE, AuditVerdict.DISAGREE, "the mill is the customer's")
    )
    derivation = HumanScreeningDerivation(
        identified_entity="cnc mill", audits=[_entity_audit(ALICE)], sections=sections
    )
    submit_screening_derivation(gt, derivation, author_email=ALICE, catalog=_CATALOG)
    assert gt.audits[-1].sections[0].applied_rules[0].outcome == "failed"


def test_disagree_audit_on_unchanged_entity_is_incoherent():
    gt = _screening_gt()
    derivation = HumanScreeningDerivation(
        identified_entity="cnc mill",
        audits=[
            _entity_audit(ALICE, AuditVerdict.DISAGREE, note="it is something else")
        ],
        sections=_copy_of_llm(gt),
    )
    with pytest.raises(AuditSubmissionError, match="incoherent"):
        submit_screening_derivation(
            gt, derivation, author_email=ALICE, catalog=_CATALOG
        )


# --- screening: replacing the entity ----------------------------------------


def test_entity_replacement_requires_latest_disagree():
    gt = _screening_gt()
    base: dict[str, Any] = dict(
        identified_entity="vertical mill", sections=_fresh_sections()
    )

    with pytest.raises(AuditSubmissionError, match="requires a latest audit"):
        submit_screening_derivation(
            gt,
            HumanScreeningDerivation(**base, audits=[_entity_audit(ALICE)]),
            author_email=ALICE,
            catalog=_CATALOG,
        )

    # The replacement value lives on the derivation itself — the disagree
    # audit carries only the rationale note (there is no corrected_text
    # duplicate left to mismatch).
    good = HumanScreeningDerivation(
        **base,
        audits=[
            _entity_audit(
                ALICE, AuditVerdict.DISAGREE, note="the text says vertical mill"
            )
        ],
    )
    submit_screening_derivation(gt, good, author_email=ALICE, catalog=_CATALOG)
    assert gt.audits[-1].identified_entity == "vertical mill"


def test_fresh_derivation_must_evaluate_every_all_section_rule():
    gt = _screening_gt()
    derivation = HumanScreeningDerivation(
        identified_entity="vertical mill",
        audits=[
            _entity_audit(
                ALICE, AuditVerdict.DISAGREE, note="the text says vertical mill"
            )
        ],
        sections=_fresh_sections({"SCR-M1": "chosen"}),  # SCR-1 left unevaluated
    )
    with pytest.raises(AuditSubmissionError, match="left 'SCR-1' unevaluated"):
        submit_screening_derivation(
            gt, derivation, author_email=ALICE, catalog=_CATALOG
        )


@pytest.mark.parametrize(
    "chosen", [{}, {"SCR-M1": "chosen", "SCR-M2": "chosen"}], ids=["none", "two"]
)
def test_fresh_ordered_ladder_chooses_exactly_one(chosen):
    gt = _screening_gt()
    derivation = HumanScreeningDerivation(
        identified_entity="vertical mill",
        audits=[
            _entity_audit(
                ALICE, AuditVerdict.DISAGREE, note="the text says vertical mill"
            )
        ],
        sections=_fresh_sections({"SCR-1": "satisfied", **chosen}),
    )
    with pytest.raises(AuditSubmissionError, match="exactly one branch"):
        submit_screening_derivation(
            gt, derivation, author_email=ALICE, catalog=_CATALOG
        )


def test_no_candidate_cannot_be_asserted_against_an_entity():
    gt = _screening_gt()
    derivation = HumanScreeningDerivation(
        identified_entity=None,
        audits=[_entity_audit(ALICE)],
        sections=_copy_of_llm(gt),
    )
    with pytest.raises(AuditSubmissionError, match="the fold decides passed"):
        submit_screening_derivation(
            gt, derivation, author_email=ALICE, catalog=_CATALOG
        )


def test_agreeing_with_a_no_candidate_copy_is_legal():
    verdict = ScreeningVerdict(
        passed=False,
        identified_entity=None,
        applied_rules=[],
        no_candidate_explanation="nothing named",
    )
    gt = ScreeningGT(
        llm_result=inflate_screening_llm_copy(_CATALOG, verdict), audits=[]
    )
    derivation = HumanScreeningDerivation(
        identified_entity=None,
        audits=[_entity_audit(ALICE)],
        sections=copy.deepcopy(gt.llm_result.sections),
    )
    submit_screening_derivation(gt, derivation, author_email=ALICE, catalog=_CATALOG)
    assert gt.audits[-1].identified_entity is None


# --- screening: catalog fidelity --------------------------------------------


def test_shape_drift_is_rejected():
    gt = _screening_gt()
    sections = _copy_of_llm(gt)[:2]  # matching section dropped
    derivation = HumanScreeningDerivation(
        identified_entity="cnc mill", audits=[_entity_audit(ALICE)], sections=sections
    )
    with pytest.raises(AuditSubmissionError, match="does not mirror"):
        submit_screening_derivation(
            gt, derivation, author_email=ALICE, catalog=_CATALOG
        )


def test_forged_kind_and_foreign_vocab_are_rejected():
    gt = _screening_gt()
    forged = _copy_of_llm(gt)
    forged[0].applied_rules[0].kind = "quality"
    with pytest.raises(AuditSubmissionError, match="claims kind"):
        submit_screening_derivation(
            gt,
            HumanScreeningDerivation(
                identified_entity="cnc mill", audits=[_entity_audit(ALICE)], sections=forged
            ),
            author_email=ALICE,
            catalog=_CATALOG,
        )

    off_vocab = _fill(_copy_of_llm(gt), {"SCR-1": "maybe"})
    with pytest.raises(AuditSubmissionError, match="not in the 'condition' vocabulary"):
        submit_screening_derivation(
            gt,
            HumanScreeningDerivation(
                identified_entity="cnc mill",
                audits=[_entity_audit(ALICE)],
                sections=off_vocab,
            ),
            author_email=ALICE,
            catalog=_CATALOG,
        )


def test_same_author_derivation_resubmission_pops():
    gt = _screening_gt()
    for entity_audit in [_entity_audit(ALICE), _entity_audit(BOB), _entity_audit(ALICE)]:
        submit_screening_derivation(
            gt,
            HumanScreeningDerivation(
                identified_entity="cnc mill",
                audits=[entity_audit],
                sections=_copy_of_llm(gt),
            ),
            author_email=entity_audit.author_email,
            catalog=_CATALOG,
        )
    assert [d.audits[0].author_email for d in gt.audits] == [ALICE, BOB, ALICE]


# --- grounding ---------------------------------------------------------------


def test_human_asserted_tag_derives_fresh():
    audits_list = []
    incomplete = GroundingDerivation(
        tag="Vertical Mill",
        audits=[_entity_audit(ALICE)],
        sections=_fresh_sections({"SCR-M1": "chosen"}),
    )
    with pytest.raises(AuditSubmissionError, match="unevaluated"):
        submit_grounding_derivation(
            audits_list,
            incomplete,
            author_email=ALICE,
            llm_tag="Vertical Mill",
            llm_sections=None,
            catalog=_CATALOG,
        )

    complete = GroundingDerivation(
        tag="Vertical Mill", audits=[_entity_audit(ALICE)], sections=_fresh_sections()
    )
    submit_grounding_derivation(
        audits_list,
        complete,
        author_email=ALICE,
        llm_tag="Vertical Mill",
        llm_sections=None,
        catalog=_CATALOG,
    )
    assert audits_list == [complete]


def test_tag_replacement_requires_disagree_coherence():
    gt = _screening_gt()
    llm_sections = gt.llm_result.sections
    replacement = GroundingDerivation(
        tag="Horizontal Mill", audits=[_entity_audit(ALICE)], sections=_fresh_sections()
    )
    with pytest.raises(AuditSubmissionError, match="requires a latest audit"):
        submit_grounding_derivation(
            [],
            replacement,
            author_email=ALICE,
            llm_tag="Vertical Mill",
            llm_sections=llm_sections,
            catalog=_CATALOG,
        )


def test_sentinel_tags_never_reach_ground_truth():
    with pytest.raises(AuditSubmissionError, match="reserved non-labels"):
        submit_grounding_derivation(
            [],
            GroundingDerivation(
                tag="None of the above",
                audits=[_entity_audit(ALICE)],
                sections=_fresh_sections(),
            ),
            author_email=ALICE,
            llm_tag="None of the above",
            llm_sections=None,
            catalog=_CATALOG,
        )


def test_unchanged_tag_silent_edit_is_rejected():
    gt = _screening_gt()
    llm_sections = gt.llm_result.sections
    edited = _fill(copy.deepcopy(llm_sections), {"SCR-1": "failed"})
    with pytest.raises(AuditSubmissionError, match="no silent edits"):
        submit_grounding_derivation(
            [],
            GroundingDerivation(
                tag="Vertical Mill", audits=[_entity_audit(ALICE)], sections=edited
            ),
            author_email=ALICE,
            llm_tag="Vertical Mill",
            llm_sections=llm_sections,
            catalog=_CATALOG,
        )


# --- missed phrases -----------------------------------------------------------

_CHUNK_TEXT = "They run a CNC mill and do welding for aerospace."


def _chunk() -> ChunkGT:
    return ChunkGT(
        extracted_phrases={
            "cnc mill": ExtractedPhraseGT(
                search_round=1,
                llm_relationship=RelationshipGT(llm_result="a machine they run"),
            )
        },
        missed_phrases=[],
    )


def _missed(phrase="aerospace", author=ALICE) -> MissedPhraseEntry:
    return MissedPhraseEntry(
        phrase=phrase,
        author_email=author,
        at=datetime(2026, 8, 15, tzinfo=timezone.utc),
        source="api",
        relationship_text="an industry they serve",
        screening=HumanScreeningDerivation(
            identified_entity=phrase, audits=[], sections=_fresh_sections()
        ),
        groundings=[
            GroundingDerivation(tag="Aerospace", sections=_fresh_sections())
        ],
    )


def _submit_missed(chunk, entry):
    submit_missed_phrase(
        chunk,
        entry,
        chunk_text=_CHUNK_TEXT,
        screening_catalog=_CATALOG,
        grounding_catalog=_CATALOG,
    )


def test_missed_phrase_must_be_a_whole_word_in_the_chunk():
    # "welding" is in the text; "weld" is not a whole word of it.
    with pytest.raises(AuditSubmissionError, match="whole word"):
        _submit_missed(_chunk(), _missed(phrase="weld"))


def test_missed_phrase_matches_case_insensitively():
    chunk = _chunk()
    _submit_missed(chunk, _missed(phrase="AEROSPACE"))
    assert chunk.missed_phrases[0].phrase == "AEROSPACE"


def test_extracted_phrase_cannot_be_asserted_missed():
    with pytest.raises(AuditSubmissionError, match="audit it there"):
        _submit_missed(_chunk(), _missed(phrase="cnc mill"))


def test_missed_screening_must_be_fresh_complete():
    entry = _missed()
    entry.screening.sections[2].applied_rules[0].outcome = None  # unchoose M1
    with pytest.raises(AuditSubmissionError, match="exactly one branch"):
        _submit_missed(_chunk(), entry)


def test_same_author_same_phrase_replaces_other_authors_append():
    chunk = _chunk()
    _submit_missed(chunk, _missed())
    _submit_missed(chunk, _missed(author=BOB))
    _submit_missed(chunk, _missed())  # alice re-asserts
    assert [(e.author_email, e.phrase) for e in chunk.missed_phrases] == [
        (BOB, "aerospace"),
        (ALICE, "aerospace"),
    ]


def test_foreign_audit_inside_a_missed_assertion_is_rejected():
    entry = _missed()
    entry.groundings[0].audits.append(_entity_audit(BOB))
    with pytest.raises(AuditSubmissionError, match="authored by"):
        _submit_missed(_chunk(), entry)
