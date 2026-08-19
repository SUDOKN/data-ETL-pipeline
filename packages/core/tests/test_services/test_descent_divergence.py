"""The in-vocab correction grammar (P3 verdict 6): keep-path node audits with
per-catalog section routing, divergence paths, and the fold's reset/divergence
precedence.

Synthetic IGR/RGR catalogs with disjoint rule ids but colliding section ids —
the measured shape of the real pair — and a dict-backed ontology.
"""

from datetime import datetime, timezone
from typing import Optional

import pytest

from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.ground_truth.audits import AuditVerdict, TextFieldAudit
from core.models.ground_truth.stage_blocks import (
    ExtractedPhraseGT,
    GroundingDerivation,
    HumanDescentPath,
    InVocabGroundingGT,
    InVocabNodeGT,
    OovGroundingGT,
    RelationshipGT,
    TagGroundingGT,
)
from core.models.rule_catalog import (
    STAGE_INITIAL_GROUNDING,
    STAGE_RECURSIVE_GROUNDING,
    RuleCatalog,
)
from core.services.ground_truth.audit_submission_validation import (
    AuditSubmissionError,
    submit_descent_divergence,
    submit_in_vocab_node_audit,
)
from core.services.ground_truth.catalog_template_inflation import (
    inflate_applied_rules,
)
from core.services.ground_truth.gt_fold import (
    compute_phrase_truth,
    effective_in_vocab,
    rule_agreement_rollup,
)

ALICE = "alice@example.com"
BOB = "bob@example.com"


def _grounding_catalog(prefix: str, stage: str) -> RuleCatalog:
    return RuleCatalog.model_validate(
        {
            "catalog_version": f"synthetic_{prefix.lower()}.1",
            "prompt_name": f"synthetic_{prefix.lower()}",
            "stage": stage,
            "field_types": ["industries"],
            "entity_noun": "concept",
            "entity_relationships": {
                "base": "serve",
                "third_person": "serves",
                "gerund": "serving",
            },
            "outcome_vocab": {
                "condition": ["satisfied", "failed", "not_triggered"],
                "guard": ["violated"],
                "preference": ["chosen"],
            },
            "sections": [
                {
                    "section_id": "match_qualification",
                    "heading": "MQ",
                    "combinator": "all",
                    "rules": [
                        {
                            "id": f"{prefix}-C1",
                            "kind": "condition",
                            "reportable": True,
                            "report_when": "always",
                            "text": "the text supports the link",
                        },
                        {
                            "id": f"{prefix}-G1",
                            "kind": "guard",
                            "reportable": True,
                            "report_when": "on_violation",
                            "text": "no third-party attribution",
                        },
                    ],
                },
                {
                    "section_id": "matching",
                    "heading": "Matching",
                    "combinator": "ordered",
                    "rules": [
                        {
                            "id": f"{prefix}-M1",
                            "kind": "preference",
                            "reportable": True,
                            "report_when": "when_chosen",
                            "text": "exact",
                        },
                        {
                            "id": f"{prefix}-M2",
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


IGR = _grounding_catalog("IGR", STAGE_INITIAL_GROUNDING)
RGR = _grounding_catalog("RGR", STAGE_RECURSIVE_GROUNDING)

# name -> children; None-returning lookups mean "not a concept".
_ONTOLOGY: dict[str, list[str]] = {
    "Manufacturing": ["Automotive", "Aerospace", "Defense"],
    "Automotive": ["EV", "Trucks"],
    "Aerospace": ["Satellites"],
    "Defense": ["Naval"],
    "EV": [],
    "Trucks": [],
    "Satellites": [],
    "Healthcare": [],
}


def _children(name: str) -> Optional[list[str]]:
    return _ONTOLOGY.get(name)


def _audit(author=ALICE, type=AuditVerdict.AGREE, corrected_text=None):
    return TextFieldAudit(
        type=type,
        corrected_text=corrected_text,
        author_email=author,
        at=datetime(2026, 8, 16, tzinfo=timezone.utc),
        source="api",
    )


def _fill(sections, outcomes: dict[str, str]):
    for section in sections:
        for rule in section.applied_rules:
            if rule.rule_id in outcomes:
                rule.outcome = outcomes[rule.rule_id]
                rule.explanation = "human derivation"
    return sections


def _fresh(catalog: RuleCatalog, prefix: str):
    return _fill(
        inflate_applied_rules(catalog, [], synthesize_all_on_empty=True),
        {f"{prefix}-C1": "satisfied", f"{prefix}-M1": "chosen"},
    )


def _llm_sections(catalog: RuleCatalog, prefix: str):
    rows = [
        AppliedRule(
            rule_id=f"{prefix}-C1", outcome="satisfied", explanation="stated"
        ),
        AppliedRule(rule_id=f"{prefix}-M1", outcome="chosen", explanation="exact"),
    ]
    return inflate_applied_rules(catalog, rows)


def _node(parent: Optional[str], group: str, sections) -> InVocabNodeGT:
    return InVocabNodeGT(
        parent_group_id=parent, group_id=group, sections=sections, audits=[]
    )


def _tree() -> InVocabGroundingGT:
    """Manufacturing → (Automotive → EV, Aerospace). Level-1 node carries IGR
    rows; descent nodes carry RGR rows."""
    return InVocabGroundingGT(
        levels={
            1: [_node(None, "Manufacturing", _llm_sections(IGR, "IGR"))],
            2: [
                _node("Manufacturing", "Automotive", _llm_sections(RGR, "RGR")),
                _node("Manufacturing", "Aerospace", _llm_sections(RGR, "RGR")),
            ],
            3: [_node("Automotive", "EV", _llm_sections(RGR, "RGR"))],
        }
    )


def _mixed_node() -> InVocabNodeGT:
    """A descent node whose sections concatenate both catalogs (the P2.1
    mixed-provenance shape)."""
    return _node(
        "Manufacturing",
        "Automotive",
        _llm_sections(IGR, "IGR") + _llm_sections(RGR, "RGR"),
    )


def _hop(tag: str, author=ALICE) -> GroundingDerivation:
    return GroundingDerivation(tag=tag, audits=[], sections=_fresh(RGR, "RGR"))


def _path(
    hops: list[GroundingDerivation],
    author=ALICE,
    anchor_level=1,
    anchor_parent=None,
    anchor="Manufacturing",
    replaces: Optional[str] = "Automotive",
    stopped=True,
) -> HumanDescentPath:
    return HumanDescentPath(
        author_email=author,
        at=datetime(2026, 8, 16, tzinfo=timezone.utc),
        source="api",
        anchor_level=anchor_level,
        anchor_parent_group_id=anchor_parent,
        anchor_group_id=anchor,
        replaces_group_id=replaces,
        hops=hops,
        stopped=stopped,
    )


# --- the path model ----------------------------------------------------------


def test_path_with_no_hops_asserts_nothing():
    with pytest.raises(ValueError, match="asserts nothing"):
        _path(hops=[])


def test_path_must_stop_explicitly():
    with pytest.raises(ValueError, match="explicit stop"):
        _path(hops=[_hop("Defense")], stopped=False)


def test_unhappy_hop_is_rejected_by_the_model():
    bad = GroundingDerivation(
        tag="Defense",
        audits=[],
        sections=_fill(
            inflate_applied_rules(RGR, [], synthesize_all_on_empty=True),
            {"RGR-C1": "failed", "RGR-M1": "chosen"},
        ),
    )
    with pytest.raises(ValueError, match="ground to satisfaction"):
        _path(hops=[bad])


def test_old_documents_load_without_human_paths():
    gt = InVocabGroundingGT(levels={})
    assert gt.human_paths == []


# --- keep-path node audits (verdict 6c) --------------------------------------


def _keep_derivation(node: InVocabNodeGT, author=ALICE) -> GroundingDerivation:
    sections = [s.model_copy(deep=True) for s in node.sections]
    return GroundingDerivation(
        tag=node.group_id, audits=[_audit(author)], sections=sections
    )


def test_mixed_node_keep_audit_routes_each_section_to_its_catalog():
    node = _mixed_node()
    derivation = _keep_derivation(node)
    # Flip one RGR outcome, with an audit on that rule — a legitimate judgment.
    for section in derivation.sections:
        for rule in section.applied_rules:
            if rule.rule_id == "RGR-C1":
                rule.outcome = "failed"
                rule.audits.append(
                    _audit(ALICE, AuditVerdict.DISAGREE, "failed")
                )

    submit_in_vocab_node_audit(
        node, derivation, author_email=ALICE, catalogs=[IGR, RGR]
    )

    assert node.audits == [derivation]


def test_unaudited_outcome_diff_is_rejected():
    node = _mixed_node()
    derivation = _keep_derivation(node)
    for section in derivation.sections:
        for rule in section.applied_rules:
            if rule.rule_id == "RGR-C1":
                rule.outcome = "failed"  # silent edit

    with pytest.raises(AuditSubmissionError, match="audit"):
        submit_in_vocab_node_audit(
            node, derivation, author_email=ALICE, catalogs=[IGR, RGR]
        )


def test_tag_change_on_a_descent_node_is_redirected_to_paths():
    node = _mixed_node()
    derivation = _keep_derivation(node)
    derivation.tag = "Aerospace"

    with pytest.raises(AuditSubmissionError, match="divergence path"):
        submit_in_vocab_node_audit(
            node, derivation, author_email=ALICE, catalogs=[IGR, RGR]
        )


def test_disagree_on_a_kept_tag_is_incoherent():
    node = _mixed_node()
    derivation = _keep_derivation(node)
    derivation.audits.append(_audit(ALICE, AuditVerdict.DISAGREE, "Aerospace"))

    with pytest.raises(AuditSubmissionError, match="incoherent"):
        submit_in_vocab_node_audit(
            node, derivation, author_email=ALICE, catalogs=[IGR, RGR]
        )


def test_a_section_spanning_catalogs_cannot_route():
    igr_sections = _llm_sections(IGR, "IGR")
    rgr_sections = _llm_sections(RGR, "RGR")
    # Graft an RGR rule into an IGR section: no single catalog knows it all.
    igr_sections[0].applied_rules.append(rgr_sections[0].applied_rules[0])
    node = _node("Manufacturing", "Automotive", igr_sections)
    derivation = _keep_derivation(node)

    with pytest.raises(AuditSubmissionError, match="cannot route"):
        submit_in_vocab_node_audit(
            node, derivation, author_email=ALICE, catalogs=[IGR, RGR]
        )


def test_same_author_node_audit_pops_stack_top():
    node = _mixed_node()
    submit_in_vocab_node_audit(
        node, _keep_derivation(node), author_email=ALICE, catalogs=[IGR, RGR]
    )
    again = _keep_derivation(node)
    submit_in_vocab_node_audit(
        node, again, author_email=ALICE, catalogs=[IGR, RGR]
    )

    assert node.audits == [again]


# --- divergence submission (verdict 6b) --------------------------------------


def test_divergence_replaces_a_stored_child_and_appends():
    gt = _tree()
    path = _path(hops=[_hop("Defense"), _hop("Naval")])

    submit_descent_divergence(
        gt,
        path,
        author_email=ALICE,
        recursive_catalog=RGR,
        ontology_children=_children,
    )

    assert gt.human_paths == [path]


def test_path_author_must_match_submitter():
    gt = _tree()
    with pytest.raises(AuditSubmissionError, match="authored by"):
        submit_descent_divergence(
            gt,
            _path(hops=[_hop("Defense")], author=BOB),
            author_email=ALICE,
            recursive_catalog=RGR,
            ontology_children=_children,
        )


def test_stranger_audit_inside_a_hop_is_rejected():
    gt = _tree()
    hop = _hop("Defense")
    hop.audits.append(_audit(BOB))
    with pytest.raises(AuditSubmissionError, match="bob@example.com"):
        submit_descent_divergence(
            gt,
            _path(hops=[hop]),
            author_email=ALICE,
            recursive_catalog=RGR,
            ontology_children=_children,
        )


def test_missing_anchor_is_rejected():
    gt = _tree()
    with pytest.raises(AuditSubmissionError, match="last stored node"):
        submit_descent_divergence(
            gt,
            _path(hops=[_hop("Defense")], anchor="Ghost"),
            author_email=ALICE,
            recursive_catalog=RGR,
            ontology_children=_children,
        )


def test_replaced_child_must_be_stored_and_is_named_when_not():
    gt = _tree()
    with pytest.raises(AuditSubmissionError, match="Automotive.*Aerospace"):
        submit_descent_divergence(
            gt,
            _path(hops=[_hop("Defense")], replaces="Defense"),
            author_email=ALICE,
            recursive_catalog=RGR,
            ontology_children=_children,
        )


def test_continuation_from_stop_needs_a_childless_anchor():
    gt = _tree()
    # EV (level 3) has no stored children: continuing past the stop is fine.
    continuation = _path(
        hops=[_hop("Trucks")],  # placeholder child; see ontology below
        anchor_level=3,
        anchor_parent="Automotive",
        anchor="EV",
        replaces=None,
    )
    # EV has no children in the ontology — use a tree whose leaf can descend.
    _ONTOLOGY["EV"] = ["BatteryPacks"]
    _ONTOLOGY["BatteryPacks"] = []
    continuation.hops[0].tag = "BatteryPacks"
    try:
        submit_descent_divergence(
            gt,
            continuation,
            author_email=ALICE,
            recursive_catalog=RGR,
            ontology_children=_children,
        )
        assert gt.human_paths == [continuation]

        # An anchor WITH stored children must name the replaced one.
        with pytest.raises(AuditSubmissionError, match="name the one"):
            submit_descent_divergence(
                gt,
                _path(hops=[_hop("Defense")], replaces=None),
                author_email=ALICE,
                recursive_catalog=RGR,
                ontology_children=_children,
            )
    finally:
        _ONTOLOGY["EV"] = []
        _ONTOLOGY.pop("BatteryPacks")


def test_sentinel_hop_is_not_a_stop():
    gt = _tree()
    with pytest.raises(AuditSubmissionError, match="stop signal"):
        submit_descent_divergence(
            gt,
            _path(hops=[_hop("None of the above")]),
            author_email=ALICE,
            recursive_catalog=RGR,
            ontology_children=_children,
        )


def test_hops_must_be_real_ontology_edges():
    gt = _tree()
    # Satellites is a concept, but not a child of Manufacturing.
    with pytest.raises(AuditSubmissionError, match="not a child of"):
        submit_descent_divergence(
            gt,
            _path(hops=[_hop("Satellites")]),
            author_email=ALICE,
            recursive_catalog=RGR,
            ontology_children=_children,
        )
    # Chaining: hop 2 descends from hop 1's tag, which must be a known concept.
    with pytest.raises(AuditSubmissionError, match="not a concept"):
        submit_descent_divergence(
            gt,
            _path(hops=[_hop("Defense"), _hop("Naval"), _hop("Frigates")]),
            author_email=ALICE,
            recursive_catalog=RGR,
            ontology_children=lambda name: (
                {"Manufacturing": ["Defense"], "Defense": ["Naval"]}.get(name)
            ),
        )


def test_incomplete_fresh_hop_is_rejected_beyond_the_happy_path():
    gt = _tree()
    hop = GroundingDerivation(
        tag="Defense",
        audits=[],
        sections=_fill(
            inflate_applied_rules(RGR, [], synthesize_all_on_empty=True),
            {"RGR-C1": "satisfied"},  # happy, but no ordered branch chosen
        ),
    )
    with pytest.raises(AuditSubmissionError, match="exactly one branch"):
        submit_descent_divergence(
            gt,
            _path(hops=[hop]),
            author_email=ALICE,
            recursive_catalog=RGR,
            ontology_children=_children,
        )


def test_same_author_same_anchor_pops_different_author_appends():
    gt = _tree()
    first = _path(hops=[_hop("Defense")])
    submit_descent_divergence(
        gt, first, author_email=ALICE, recursive_catalog=RGR,
        ontology_children=_children,
    )
    second = _path(hops=[_hop("Defense"), _hop("Naval")])
    submit_descent_divergence(
        gt, second, author_email=ALICE, recursive_catalog=RGR,
        ontology_children=_children,
    )
    assert gt.human_paths == [second]  # PH-7 stack-top pop

    bobs = _path(hops=[_hop("Defense", author=BOB)], author=BOB)
    submit_descent_divergence(
        gt, bobs, author_email=BOB, recursive_catalog=RGR,
        ontology_children=_children,
    )
    assert gt.human_paths == [second, bobs]


# --- the fold precedence (verdicts 6a/6b) ------------------------------------


def test_divergence_fold_swaps_the_replaced_subtree_only():
    gt = _tree()
    submit_descent_divergence(
        gt,
        _path(hops=[_hop("Defense"), _hop("Naval")]),
        author_email=ALICE,
        recursive_catalog=RGR,
        ontology_children=_children,
    )

    levels = effective_in_vocab(gt)

    level2 = {truth.group_id: truth for truth in levels[2]}
    assert "Automotive" not in level2  # replaced subtree root gone
    assert "Aerospace" in level2  # sibling stands
    assert level2["Defense"].replaced_from == "Automotive"
    assert level2["Defense"].reviewed is True and level2["Defense"].held is True
    level3 = {truth.group_id: truth for truth in levels[3]}
    assert "EV" not in level3  # descendant of the replaced child gone
    assert level3["Naval"].parent_group_id == "Defense"
    assert level3["Naval"].replaced_from is None


def test_reset_fold_overrides_the_whole_descent_wholesale():
    replacement = GroundingDerivation(
        tag="Healthcare",
        audits=[_audit(ALICE, AuditVerdict.DISAGREE, "Healthcare")],
        sections=_fresh(IGR, "IGR"),
    )
    phrase = ExtractedPhraseGT(
        search_round=1,
        llm_relationship=RelationshipGT(llm_result="serves carmakers"),
        oov_grounding=OovGroundingGT(
            tags={
                "Manufacturing": TagGroundingGT(
                    llm_result=_llm_sections(IGR, "IGR"),
                    audits=[replacement],
                )
            }
        ),
        in_vocab_grounding=_tree(),
    )

    truth = compute_phrase_truth(phrase)

    assert truth.tags["Healthcare"].replaced_from == "Manufacturing"
    assert truth.in_vocab == {}  # the entire stored descent is superseded


def test_unreviewed_document_fold_is_unchanged():
    levels = effective_in_vocab(_tree())
    assert {t.group_id for t in levels[2]} == {"Automotive", "Aerospace"}
    assert all(t.reviewed is False for lvl in levels.values() for t in lvl)


def test_rollup_still_counts_audits_on_superseded_nodes():
    gt = _tree()
    # Alice audits a rule on the Automotive node, then diverges away from it.
    node = gt.levels[2][0]
    derivation = _keep_derivation(node)
    for section in derivation.sections:
        for rule in section.applied_rules:
            if rule.rule_id == "RGR-C1":
                rule.audits.append(_audit(ALICE))
    submit_in_vocab_node_audit(
        node, derivation, author_email=ALICE, catalogs=[IGR, RGR]
    )
    submit_descent_divergence(
        gt,
        _path(hops=[_hop("Defense")]),
        author_email=ALICE,
        recursive_catalog=RGR,
        ontology_children=_children,
    )

    from core.models.ground_truth.stage_blocks import ChunkGT

    chunk = ChunkGT(
        extracted_phrases={
            "car parts": ExtractedPhraseGT(
                search_round=1,
                llm_relationship=RelationshipGT(llm_result="serves carmakers"),
                in_vocab_grounding=gt,
            )
        },
        missed_phrases=[],
    )
    rollup = rule_agreement_rollup({"0:60": chunk})

    assert rollup["RGR-C1"].agree == 1  # the superseded node's audit survives