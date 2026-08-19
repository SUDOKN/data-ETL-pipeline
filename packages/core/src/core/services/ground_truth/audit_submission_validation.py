"""The audit-submission contract.

Submissions VALIDATE against the pinned catalog and MUTATE the in-memory
document; persisting is the caller's, and the document is written whole
(settled semantics #5). Every rejection here is a claim the fold (P2.4) never
has to re-examine.

The shape rules in one paragraph: a derivation either keeps the LLM's
entity/tag and mirrors the LLM copy's tree (where any changed outcome or
explanation must carry an audit — no silent edits), or replaces it and derives
FRESH from the catalog's silent skeleton (where every ``all``-section rule
must be evaluated and every ``ordered`` ladder must choose exactly one branch,
the wire's own cardinality). "No candidate" cannot be asserted against an
identified entity: rejection is expressed through rule outcomes, so ``passed``
stays a derived fact, never an opinion.

Authorship: one submission, one author — every audit entry inside must carry
the submitting author, and a derivation containing no audit entry anywhere
asserts nothing. Same-author resubmission pops the stack top (PH-7); missed
phrases instead replace by (author, phrase), being keyed assertions rather
than stacked opinions.
"""

from __future__ import annotations

import re
from typing import Callable, Iterator, Optional

from core.models.extraction_schemas.grounding import is_sentinel_grounding_label
from core.models.ground_truth.audits import AuditVerdict, TextFieldAudit
from core.models.ground_truth.rule_tree import AuditedRule, AuditedSection
from core.models.ground_truth.stage_blocks import (
    ChunkGT,
    GroundingDerivation,
    HumanDescentPath,
    HumanScreeningDerivation,
    InVocabGroundingGT,
    InVocabNodeGT,
    MissedPhraseEntry,
    ScreeningGT,
)
from core.models.rule_catalog import RuleCatalog
from core.services.brute_search_service import word_regex
from core.services.ground_truth.catalog_template_inflation import (
    UNREPORTED_GUARD_OUTCOME,
    inflate_applied_rules,
)


class AuditSubmissionError(ValueError):
    """A submission the contract rejects; nothing was mutated."""


# --- tree helpers -----------------------------------------------------------


def _walk_rules(sections: list[AuditedSection]) -> Iterator[AuditedRule]:
    def _descend(rules: list[AuditedRule]) -> Iterator[AuditedRule]:
        for rule in rules:
            yield rule
            yield from _descend(rule.sub_rules)

    for section in sections:
        yield from _descend(section.applied_rules)


def _tree_signature(sections: list[AuditedSection]):
    def _rule_sig(rule: AuditedRule):
        return (rule.rule_id, tuple(_rule_sig(r) for r in rule.sub_rules))

    return tuple(
        (s.section_id, s.combinator, tuple(_rule_sig(r) for r in s.applied_rules))
        for s in sections
    )


def _check_shape(
    sections: list[AuditedSection],
    template: list[AuditedSection],
    what: str,
) -> None:
    if _tree_signature(sections) != _tree_signature(template):
        raise AuditSubmissionError(
            f"{what}: the section/rule tree does not mirror its template — "
            f"derivations keep the catalog's exact shape, only outcomes, "
            f"explanations and audits are theirs to write"
        )


def _check_rules_against_catalog(
    sections: list[AuditedSection], catalog: RuleCatalog, what: str
) -> None:
    known = catalog.rules_by_id()
    for rule in _walk_rules(sections):
        node = known.get(rule.rule_id)
        if node is None:
            raise AuditSubmissionError(
                f"{what}: rule {rule.rule_id!r} does not exist at "
                f"catalog_version {catalog.catalog_version!r}"
            )
        if rule.kind != node.kind:
            raise AuditSubmissionError(
                f"{what}: rule {rule.rule_id!r} claims kind {rule.kind!r} but "
                f"the catalog says {node.kind!r}"
            )
        if rule.outcome is None:
            continue
        if node.kind == "guard" and rule.outcome == UNREPORTED_GUARD_OUTCOME:
            continue  # the silence marker the template synthesizer writes
        if rule.outcome not in catalog.valid_outcomes(rule.rule_id):
            raise AuditSubmissionError(
                f"{what}: rule {rule.rule_id!r} outcome {rule.outcome!r} is not "
                f"in the {node.kind!r} vocabulary "
                f"{sorted(catalog.valid_outcomes(rule.rule_id))}"
            )


def _check_fresh_complete(
    sections: list[AuditedSection], catalog: RuleCatalog, what: str
) -> None:
    """A fresh derivation applied the whole catalog: ``all`` sections fully
    evaluated, ``ordered`` ladders choosing exactly one branch, guards free."""
    for section in sections:
        if section.combinator == "all":
            for rule in section.applied_rules:
                if rule.kind != "note" and rule.outcome is None:
                    raise AuditSubmissionError(
                        f"{what}: fresh derivation left {rule.rule_id!r} "
                        f"unevaluated in all-section {section.section_id!r}"
                    )
        elif section.combinator == "ordered":
            chosen = [
                rule.rule_id
                for rule in section.applied_rules
                if rule.outcome == "chosen"
            ]
            if len(chosen) != 1:
                raise AuditSubmissionError(
                    f"{what}: ordered section {section.section_id!r} must choose "
                    f"exactly one branch, got {chosen or 'none'}"
                )


# --- authorship -------------------------------------------------------------


def _inner_audits(
    audits: list[TextFieldAudit], sections: list[AuditedSection]
) -> list[TextFieldAudit]:
    return [*audits, *(a for rule in _walk_rules(sections) for a in rule.audits)]


def _check_single_author(
    entries: list[TextFieldAudit], author_email: str, what: str
) -> None:
    if not entries:
        raise AuditSubmissionError(
            f"{what}: a derivation with no audit entry anywhere asserts nothing"
        )
    strangers = sorted(
        {e.author_email for e in entries if e.author_email != author_email}
    )
    if strangers:
        raise AuditSubmissionError(
            f"{what}: one submission, one author — {author_email} cannot submit "
            f"entries authored by {strangers}"
        )


# --- append machinery -------------------------------------------------------


def append_text_audit(audits: list[TextFieldAudit], entry: TextFieldAudit) -> None:
    """Append in submission order; a same-author resubmission pops their
    previous entry, stack-top only (PH-7 — the old service's convention)."""
    if audits and audits[-1].author_email == entry.author_email:
        audits.pop()
    audits.append(entry)


def _append_popping_same_author(
    items: list, item, item_author: object, author_of
) -> None:
    """PH-7 stack-top pop. ``item_author`` is whatever ``author_of`` yields —
    a plain email for audits/derivations, the divergence key for paths."""
    if items and author_of(items[-1]) == item_author:
        items.pop()
    items.append(item)


def _derivation_author(audits: list[TextFieldAudit], sections) -> str:
    return _inner_audits(audits, sections)[0].author_email


# --- screening --------------------------------------------------------------


def _check_replacement_coherence(
    audits: list[TextFieldAudit], replacement: str, what: str
) -> None:
    if not audits or audits[-1].type is not AuditVerdict.DISAGREE:
        raise AuditSubmissionError(
            f"{what}: replacing the LLM's text requires a latest audit of type "
            f"'disagree' carrying the replacement"
        )
    if audits[-1].corrected_text != replacement:
        raise AuditSubmissionError(
            f"{what}: the disagree audit's corrected_text "
            f"{audits[-1].corrected_text!r} must equal the replacement "
            f"{replacement!r}"
        )


def _check_copy_diffs_are_audited(
    sections: list[AuditedSection],
    llm_sections: list[AuditedSection],
    what: str,
) -> None:
    for mine, llm in zip(_walk_rules(sections), _walk_rules(llm_sections)):
        changed = (mine.outcome, mine.explanation) != (llm.outcome, llm.explanation)
        if changed and not mine.audits:
            raise AuditSubmissionError(
                f"{what}: rule {mine.rule_id!r} differs from the LLM copy with "
                f"no audit entry — no silent edits"
            )


def submit_screening_derivation(
    screening_gt: ScreeningGT,
    derivation: HumanScreeningDerivation,
    *,
    author_email: str,
    catalog: RuleCatalog,
) -> None:
    what = "screening derivation"
    _check_single_author(
        _inner_audits(derivation.audits, derivation.sections), author_email, what
    )
    _check_rules_against_catalog(derivation.sections, catalog, what)

    llm = screening_gt.llm_result
    if derivation.identified_entity != llm.identified_entity:
        replacement = derivation.identified_entity
        if replacement is None:
            raise AuditSubmissionError(
                f"{what}: 'no candidate' cannot be asserted against an "
                f"identified entity — express rejection through rule outcomes; "
                f"the fold decides passed"
            )
        _check_replacement_coherence(derivation.audits, replacement, what)
        skeleton = inflate_applied_rules(catalog, [], synthesize_all_on_empty=True)
        _check_shape(derivation.sections, skeleton, what)
        _check_fresh_complete(derivation.sections, catalog, what)
    else:
        if any(a.type is AuditVerdict.DISAGREE for a in derivation.audits):
            raise AuditSubmissionError(
                f"{what}: a disagree audit on an unchanged entity is incoherent "
                f"— disagreeing means replacing it"
            )
        _check_shape(derivation.sections, llm.sections, what)
        _check_copy_diffs_are_audited(derivation.sections, llm.sections, what)

    _append_popping_same_author(
        screening_gt.audits,
        derivation,
        author_email,
        lambda d: _derivation_author(d.audits, d.sections),
    )


# --- grounding --------------------------------------------------------------


def submit_grounding_derivation(
    audits_list: list[GroundingDerivation],
    derivation: GroundingDerivation,
    *,
    author_email: str,
    llm_tag: str,
    llm_sections: Optional[list[AuditedSection]],
    catalog: RuleCatalog,
) -> None:
    """One grounding link's derivation — ``llm_sections=None`` is a
    human-asserted tag, always derived fresh. The tag plays the role
    ``identified_entity`` plays in screening."""
    what = f"grounding derivation for tag {derivation.tag!r}"
    _check_single_author(
        _inner_audits(derivation.audits, derivation.sections), author_email, what
    )
    if is_sentinel_grounding_label(derivation.tag):
        raise AuditSubmissionError(
            f"{what}: reserved non-labels never reach ground truth"
        )
    _check_rules_against_catalog(derivation.sections, catalog, what)

    if llm_sections is None or derivation.tag != llm_tag:
        if llm_sections is not None:
            _check_replacement_coherence(derivation.audits, derivation.tag, what)
        skeleton = inflate_applied_rules(catalog, [], synthesize_all_on_empty=True)
        _check_shape(derivation.sections, skeleton, what)
        _check_fresh_complete(derivation.sections, catalog, what)
    else:
        if any(a.type is AuditVerdict.DISAGREE for a in derivation.audits):
            raise AuditSubmissionError(
                f"{what}: a disagree audit on an unchanged tag is incoherent — "
                f"disagreeing means replacing it"
            )
        _check_shape(derivation.sections, llm_sections, what)
        _check_copy_diffs_are_audited(derivation.sections, llm_sections, what)

    _append_popping_same_author(
        audits_list,
        derivation,
        author_email,
        lambda d: _derivation_author(d.audits, d.sections),
    )


# --- missed phrases ---------------------------------------------------------


def submit_missed_phrase(
    chunk: ChunkGT,
    entry: MissedPhraseEntry,
    *,
    chunk_text: str,
    screening_catalog: RuleCatalog,
    grounding_catalog: RuleCatalog,
) -> None:
    """A missed-phrase assertion for one chunk.

    Presence uses the old service's convention exactly: a case-insensitive
    word-boundary match (``word_regex``), not bare substring — "mill" must not
    ride in on "milling". The model already enforced the happy path (PH-10);
    this adds what needs the catalog and the chunk: fresh completeness and
    placement.
    """
    what = f"missed phrase {entry.phrase!r}"
    if entry.phrase in chunk.extracted_phrases:
        raise AuditSubmissionError(
            f"{what}: the run extracted this phrase in this chunk — audit it "
            f"there instead of asserting it missed"
        )
    if not re.search(word_regex(entry.phrase), chunk_text, re.IGNORECASE):
        raise AuditSubmissionError(
            f"{what}: not present in the chunk text as a whole word"
        )

    inner = _inner_audits(entry.screening.audits, entry.screening.sections)
    for grounding in entry.groundings:
        inner += _inner_audits(grounding.audits, grounding.sections)
    strangers = sorted(
        {a.author_email for a in inner if a.author_email != entry.author_email}
    )
    if strangers:
        raise AuditSubmissionError(
            f"{what}: entries authored by {strangers} inside an assertion by "
            f"{entry.author_email}"
        )

    screening_skeleton = inflate_applied_rules(
        screening_catalog, [], synthesize_all_on_empty=True
    )
    _check_shape(entry.screening.sections, screening_skeleton, what)
    _check_rules_against_catalog(entry.screening.sections, screening_catalog, what)
    _check_fresh_complete(entry.screening.sections, screening_catalog, what)

    grounding_skeleton = inflate_applied_rules(
        grounding_catalog, [], synthesize_all_on_empty=True
    )
    for grounding in entry.groundings:
        g_what = f"{what}, grounding {grounding.tag!r}"
        if is_sentinel_grounding_label(grounding.tag):
            raise AuditSubmissionError(
                f"{g_what}: reserved non-labels never reach ground truth"
            )
        _check_shape(grounding.sections, grounding_skeleton, g_what)
        _check_rules_against_catalog(grounding.sections, grounding_catalog, g_what)
        _check_fresh_complete(grounding.sections, grounding_catalog, g_what)

    # Keyed assertions, not stacked opinions: the same author re-asserting the
    # same phrase replaces their previous entry wherever it sits.
    chunk.missed_phrases[:] = [
        existing
        for existing in chunk.missed_phrases
        if not (
            existing.author_email == entry.author_email
            and existing.phrase == entry.phrase
        )
    ]
    chunk.missed_phrases.append(entry)


# --- in-vocab descent (P3 verdict 6) -----------------------------------------

# (concept_name) -> its children's names, or None when the name is not a
# concept in the pinned ontology. The app layer builds this from the ontology
# version the run pinned; core stays sync and pure.
OntologyChildren = Callable[[str], Optional[list[str]]]


def _catalog_for_section(
    section: AuditedSection, catalogs: list[RuleCatalog], what: str
) -> RuleCatalog:
    """The one catalog that knows every rule id in this section.

    Mixed descent nodes concatenate sections from both grounding catalogs
    (P2.1); rule ids are disjoint between them, so each section routes
    unambiguously.
    """
    rule_ids = [rule.rule_id for rule in _walk_rules([section])]
    for catalog in catalogs:
        known = catalog.rules_by_id()
        if all(rule_id in known for rule_id in rule_ids):
            return catalog
    raise AuditSubmissionError(
        f"{what}: section {section.section_id!r} carries rule ids no single "
        f"pinned catalog knows in full ({rule_ids}) — cannot route validation"
    )


def submit_in_vocab_node_audit(
    node: InVocabNodeGT,
    derivation: GroundingDerivation,
    *,
    author_email: str,
    catalogs: list[RuleCatalog],
) -> None:
    """Keep-path audit of one stored descent node (verdict 6c).

    Descent nodes accept rule-outcome audits only: the tag must be kept — a
    different link is a divergence path, a different initial concept is a
    reset on the initial map. Mixed IGR+RGR nodes validate per section, each
    routed to the catalog that knows its rule ids.
    """
    what = f"in-vocab node audit for {node.group_id!r}"
    _check_single_author(
        _inner_audits(derivation.audits, derivation.sections), author_email, what
    )
    if derivation.tag != node.group_id:
        raise AuditSubmissionError(
            f"{what}: descent nodes accept keep-path audits only — a "
            f"different link is a divergence path anchored at the parent you "
            f"still agree with, and a different initial concept is a reset "
            f"on the initial map"
        )
    if any(a.type is AuditVerdict.DISAGREE for a in derivation.audits):
        raise AuditSubmissionError(
            f"{what}: a disagree audit on a kept tag is incoherent — "
            f"disagreeing with the link itself is a divergence path"
        )
    _check_shape(derivation.sections, node.sections, what)
    for section in derivation.sections:
        catalog = _catalog_for_section(section, catalogs, what)
        _check_rules_against_catalog([section], catalog, what)
    _check_copy_diffs_are_audited(derivation.sections, node.sections, what)
    _append_popping_same_author(
        node.audits,
        derivation,
        author_email,
        lambda d: _derivation_author(d.audits, d.sections),
    )


def submit_descent_divergence(
    in_vocab_gt: InVocabGroundingGT,
    path: HumanDescentPath,
    *,
    author_email: str,
    recursive_catalog: RuleCatalog,
    ontology_children: OntologyChildren,
) -> None:
    """A divergence path (verdict 6b): anchored at the last agreed stored
    node, replacing one named child's subtree (or continuing past a stored
    stop when the anchor has no children), hops derived fresh under the
    recursive catalog and chained as REAL edges of the pinned ontology.
    """
    what = f"divergence path at {path.anchor_group_id!r}"
    if path.author_email != author_email:
        raise AuditSubmissionError(
            f"{what}: path authored by {path.author_email!r} in a submission "
            f"by {author_email!r}"
        )
    inner: list[TextFieldAudit] = []
    for hop in path.hops:
        inner += _inner_audits(hop.audits, hop.sections)
    strangers = sorted(
        {a.author_email for a in inner if a.author_email != author_email}
    )
    if strangers:
        raise AuditSubmissionError(
            f"{what}: entries authored by {strangers} inside a path by "
            f"{author_email}"
        )

    anchor_exists = any(
        node.parent_group_id == path.anchor_parent_group_id
        and node.group_id == path.anchor_group_id
        for node in in_vocab_gt.levels.get(path.anchor_level, [])
    )
    if not anchor_exists:
        raise AuditSubmissionError(
            f"{what}: no stored node at level {path.anchor_level} with parent "
            f"{path.anchor_parent_group_id!r} and group "
            f"{path.anchor_group_id!r} — the anchor is the last stored node "
            f"you still agree with"
        )
    child_level = path.anchor_level + 1
    stored_children = [
        node.group_id
        for node in in_vocab_gt.levels.get(child_level, [])
        if node.parent_group_id == path.anchor_group_id
    ]
    if path.replaces_group_id is None:
        if stored_children:
            raise AuditSubmissionError(
                f"{what}: the anchor has stored children {stored_children} — "
                f"name the one this path replaces; adding a sibling branch "
                f"beside kept ones is not recordable"
            )
    elif path.replaces_group_id not in stored_children:
        raise AuditSubmissionError(
            f"{what}: replaces_group_id {path.replaces_group_id!r} is not a "
            f"stored child of the anchor at level {child_level} — stored "
            f"children: {stored_children or 'none'}"
        )

    skeleton = inflate_applied_rules(
        recursive_catalog, [], synthesize_all_on_empty=True
    )
    previous = path.anchor_group_id
    for hop in path.hops:
        hop_what = f"{what}, hop {hop.tag!r}"
        if is_sentinel_grounding_label(hop.tag):
            raise AuditSubmissionError(
                f"{hop_what}: the sentinel is the stop signal, not a hop — "
                f"end the path with stopped=true instead"
            )
        children = ontology_children(previous)
        if children is None:
            raise AuditSubmissionError(
                f"{hop_what}: {previous!r} is not a concept in the pinned "
                f"ontology"
            )
        if hop.tag not in children:
            raise AuditSubmissionError(
                f"{hop_what}: not a child of {previous!r} in the pinned "
                f"ontology — its children: {sorted(children) or 'none'}"
            )
        _check_shape(hop.sections, skeleton, hop_what)
        _check_rules_against_catalog(hop.sections, recursive_catalog, hop_what)
        _check_fresh_complete(hop.sections, recursive_catalog, hop_what)
        previous = hop.tag

    def _divergence_key(p: HumanDescentPath):
        return (
            p.author_email,
            p.anchor_level,
            p.anchor_parent_group_id,
            p.anchor_group_id,
            p.replaces_group_id,
        )

    _append_popping_same_author(
        in_vocab_gt.human_paths, path, _divergence_key(path), _divergence_key
    )
