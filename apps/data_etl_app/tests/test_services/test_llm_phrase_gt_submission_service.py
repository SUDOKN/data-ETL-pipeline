"""The batch apply plane (P3.5b): every surface lands, every rejection names
its item, and nothing observable mutates on failure.

Documents come from the shared fixtures against the REAL deployed catalogs;
the ontology is a dict fake (core owns edge semantics — app tests own
dispatch and atomicity).
"""

import copy
from datetime import datetime, timezone
from typing import Optional

import pytest

from core.models.ground_truth.audits import AuditVerdict, TextFieldAudit
from core.models.ground_truth.stage_blocks import (
    GroundingDerivation,
    HumanDescentPath,
    HumanScreeningDerivation,
    MissedPhraseEntry,
)
from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum
from data_etl_app.services.ground_truth.llm_phrase_gt_submission_service import (
    BatchItemError,
    DescentDivergenceItem,
    InVocabNodeAuditItem,
    MissedPhraseItem,
    OovGroundingItem,
    RelationshipAuditItem,
    ScreeningDerivationItem,
    apply_submission_batch,
    children_from_concept_map,
)
from data_etl_app.services.ground_truth.llm_phrase_gt_template_service import (
    TemplateAssemblyError,
    build_llm_phrase_gt_template,
)
from data_etl_app.services.ground_truth.llm_phrase_gt_view_service import (
    TextWitnessError,
)

ALICE = "alice@example.com"


def _audit(author=ALICE, type=AuditVerdict.AGREE, corrected_text=None):
    return TextFieldAudit(
        type=type,
        corrected_text=corrected_text,
        author_email=author,
        at=datetime(2026, 8, 16, tzinfo=timezone.utc),
        source="api",
    )


@pytest.fixture
def keyword_doc(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    mfg = make_gt_manufacturer(equipments=make_keyword_gt_results())
    doc = build_llm_phrase_gt_template(
        mfg, KeywordTypeEnum.equipments, gt_scraped_text, catalog_lookup=gt_catalog_lookup
    )
    doc.chunks[f"0:{len(gt_scraped_text)}"] = doc.chunks.pop("0:1000")
    return doc


@pytest.fixture
def concept_doc(
    make_gt_manufacturer, make_concept_gt_results, gt_catalog_lookup, gt_scraped_text
):
    mfg = make_gt_manufacturer(industries=make_concept_gt_results())
    return build_llm_phrase_gt_template(
        mfg, ConceptTypeEnum.industries, gt_scraped_text, catalog_lookup=gt_catalog_lookup
    )


def _chunk_key(doc) -> str:
    return next(iter(doc.chunks))


def _rel_item(doc, author=ALICE) -> RelationshipAuditItem:
    return RelationshipAuditItem(
        surface="relationship_audit",
        chunk_key=_chunk_key(doc),
        phrase="cnc mill",
        audit=_audit(author, AuditVerdict.AGREE_BUT, "five-axis work too"),
    )


def test_relationship_audit_lands_on_the_copy_not_the_original(
    keyword_doc, gt_catalog_lookup
):
    updated = apply_submission_batch(
        keyword_doc,
        [_rel_item(keyword_doc)],
        author_email=ALICE,
        catalog_lookup=gt_catalog_lookup,
    )

    key = _chunk_key(keyword_doc)
    assert (
        updated.chunks[key].extracted_phrases["cnc mill"].llm_relationship.audits
        != []
    )
    assert (
        keyword_doc.chunks[key]
        .extracted_phrases["cnc mill"]
        .llm_relationship.audits
        == []
    )


def test_wrong_author_is_an_item_error(keyword_doc, gt_catalog_lookup):
    with pytest.raises(BatchItemError) as exc_info:
        apply_submission_batch(
            keyword_doc,
            [_rel_item(keyword_doc, author="mallory@example.com")],
            author_email=ALICE,
            catalog_lookup=gt_catalog_lookup,
        )
    assert exc_info.value.index == 0
    assert exc_info.value.surface == "relationship_audit"
    assert "mallory@example.com" in exc_info.value.reason


def test_screening_keep_derivation_lands(keyword_doc, gt_catalog_lookup):
    key = _chunk_key(keyword_doc)
    llm = keyword_doc.chunks[key].extracted_phrases["cnc mill"].llm_screening
    assert llm is not None
    item = ScreeningDerivationItem(
        surface="screening_derivation",
        chunk_key=key,
        phrase="cnc mill",
        derivation=HumanScreeningDerivation(
            identified_entity=llm.llm_result.identified_entity,
            audits=[_audit()],
            sections=copy.deepcopy(llm.llm_result.sections),
        ),
    )

    updated = apply_submission_batch(
        keyword_doc, [item], author_email=ALICE, catalog_lookup=gt_catalog_lookup
    )

    screening = updated.chunks[key].extracted_phrases["cnc mill"].llm_screening
    assert screening is not None and len(screening.audits) == 1


def test_screening_on_an_unscreened_phrase_is_rejected(
    keyword_doc, gt_catalog_lookup
):
    key = _chunk_key(keyword_doc)
    keyword_doc.chunks[key].extracted_phrases["cnc mill"].llm_screening = None
    item = ScreeningDerivationItem(
        surface="screening_derivation",
        chunk_key=key,
        phrase="cnc mill",
        derivation=HumanScreeningDerivation(
            identified_entity="cnc mill", audits=[_audit()], sections=[]
        ),
    )

    with pytest.raises(BatchItemError, match="did not reach"):
        apply_submission_batch(
            keyword_doc, [item], author_email=ALICE, catalog_lookup=gt_catalog_lookup
        )


def test_oov_keep_entry_audit_lands(keyword_doc, gt_catalog_lookup):
    key = _chunk_key(keyword_doc)
    block = keyword_doc.chunks[key].extracted_phrases["cnc mill"].oov_grounding
    assert block is not None
    llm_sections = block.tags["CNC Milling Machine"].llm_result
    assert llm_sections is not None
    item = OovGroundingItem(
        surface="oov_grounding_derivation",
        chunk_key=key,
        phrase="cnc mill",
        tag="CNC Milling Machine",
        derivation=GroundingDerivation(
            tag="CNC Milling Machine",
            audits=[_audit()],
            sections=copy.deepcopy(llm_sections),
        ),
    )

    updated = apply_submission_batch(
        keyword_doc, [item], author_email=ALICE, catalog_lookup=gt_catalog_lookup
    )

    updated_block = updated.chunks[key].extracted_phrases["cnc mill"].oov_grounding
    assert updated_block is not None
    assert len(updated_block.tags["CNC Milling Machine"].audits) == 1


def test_human_asserted_tag_creates_its_entry(
    keyword_doc, gt_catalog_lookup, gt_fresh_happy
):
    key = _chunk_key(keyword_doc)
    freehand = gt_catalog_lookup("phrase_freehand_grounding", "equipments")
    item = OovGroundingItem(
        surface="oov_grounding_derivation",
        chunk_key=key,
        phrase="cnc mill",
        tag="Vertical Machining Center",
        derivation=GroundingDerivation(
            tag="Vertical Machining Center",
            audits=[_audit()],
            sections=gt_fresh_happy(freehand),
        ),
    )

    updated = apply_submission_batch(
        keyword_doc, [item], author_email=ALICE, catalog_lookup=gt_catalog_lookup
    )

    updated_block = updated.chunks[key].extracted_phrases["cnc mill"].oov_grounding
    assert updated_block is not None
    entry = updated_block.tags["Vertical Machining Center"]
    assert entry.llm_result is None
    assert len(entry.audits) == 1


def test_new_tag_must_be_addressed_by_its_own_name(keyword_doc, gt_catalog_lookup, gt_fresh_happy):
    key = _chunk_key(keyword_doc)
    freehand = gt_catalog_lookup("phrase_freehand_grounding", "equipments")
    item = OovGroundingItem(
        surface="oov_grounding_derivation",
        chunk_key=key,
        phrase="cnc mill",
        tag="Some Other Address",
        derivation=GroundingDerivation(
            tag="Vertical Machining Center",
            audits=[_audit()],
            sections=gt_fresh_happy(freehand),
        ),
    )

    with pytest.raises(BatchItemError, match="derivation's own tag"):
        apply_submission_batch(
            keyword_doc, [item], author_email=ALICE, catalog_lookup=gt_catalog_lookup
        )


def test_grounding_block_is_created_on_demand(keyword_doc, gt_catalog_lookup, gt_fresh_happy):
    key = _chunk_key(keyword_doc)
    keyword_doc.chunks[key].extracted_phrases["cnc mill"].oov_grounding = None
    freehand = gt_catalog_lookup("phrase_freehand_grounding", "equipments")
    item = OovGroundingItem(
        surface="oov_grounding_derivation",
        chunk_key=key,
        phrase="cnc mill",
        tag="Vertical Machining Center",
        derivation=GroundingDerivation(
            tag="Vertical Machining Center",
            audits=[_audit()],
            sections=gt_fresh_happy(freehand),
        ),
    )

    updated = apply_submission_batch(
        keyword_doc, [item], author_email=ALICE, catalog_lookup=gt_catalog_lookup
    )

    block = updated.chunks[key].extracted_phrases["cnc mill"].oov_grounding
    assert block is not None and "Vertical Machining Center" in block.tags


def _descent_block(doc):
    block = doc.chunks["0:1000"].extracted_phrases[
        "aerospace parts"
    ].in_vocab_grounding
    assert block is not None
    return block


def test_in_vocab_keep_audit_lands(concept_doc, gt_catalog_lookup):
    node = _descent_block(concept_doc).levels[1][0]
    item = InVocabNodeAuditItem(
        surface="in_vocab_node_audit",
        chunk_key="0:1000",
        phrase="aerospace parts",
        level=1,
        parent_group_id=None,
        group_id="Aerospace",
        derivation=GroundingDerivation(
            tag="Aerospace",
            audits=[_audit()],
            sections=copy.deepcopy(node.sections),
        ),
    )

    updated = apply_submission_batch(
        concept_doc, [item], author_email=ALICE, catalog_lookup=gt_catalog_lookup
    )

    assert len(_descent_block(updated).levels[1][0].audits) == 1


def test_missing_descent_node_is_named(concept_doc, gt_catalog_lookup):
    item = InVocabNodeAuditItem(
        surface="in_vocab_node_audit",
        chunk_key="0:1000",
        phrase="aerospace parts",
        level=2,
        parent_group_id="Aerospace",
        group_id="Satellites",
        derivation=GroundingDerivation(
            tag="Satellites", audits=[_audit()], sections=[]
        ),
    )

    with pytest.raises(BatchItemError, match="no stored descent node"):
        apply_submission_batch(
            concept_doc, [item], author_email=ALICE, catalog_lookup=gt_catalog_lookup
        )


class _FakeConcept:
    def __init__(self, children: list[str]):
        self.children = children


def test_divergence_continuation_applies(concept_doc, gt_catalog_lookup, gt_fresh_happy):
    recursive = gt_catalog_lookup("phrase_recursive_grounding", "industries")
    ontology_children = children_from_concept_map(
        {
            "Aerospace": _FakeConcept(["Satellites"]),
            "Satellites": _FakeConcept([]),
        }
    )
    item = DescentDivergenceItem(
        surface="descent_divergence",
        chunk_key="0:1000",
        phrase="aerospace parts",
        path=HumanDescentPath(
            author_email=ALICE,
            at=datetime(2026, 8, 16, tzinfo=timezone.utc),
            source="api",
            anchor_level=1,
            anchor_parent_group_id=None,
            anchor_group_id="Aerospace",
            replaces_group_id=None,
            hops=[
                GroundingDerivation(
                    tag="Satellites", audits=[], sections=gt_fresh_happy(recursive)
                )
            ],
            stopped=True,
        ),
    )

    updated = apply_submission_batch(
        concept_doc,
        [item],
        author_email=ALICE,
        catalog_lookup=gt_catalog_lookup,
        ontology_children=ontology_children,
    )

    assert len(_descent_block(updated).human_paths) == 1
    assert _descent_block(concept_doc).human_paths == []


def test_divergence_needs_the_pinned_ontology(concept_doc, gt_catalog_lookup, gt_fresh_happy):
    recursive = gt_catalog_lookup("phrase_recursive_grounding", "industries")
    item = DescentDivergenceItem(
        surface="descent_divergence",
        chunk_key="0:1000",
        phrase="aerospace parts",
        path=HumanDescentPath(
            author_email=ALICE,
            at=datetime(2026, 8, 16, tzinfo=timezone.utc),
            source="api",
            anchor_level=1,
            anchor_parent_group_id=None,
            anchor_group_id="Aerospace",
            replaces_group_id=None,
            hops=[
                GroundingDerivation(
                    tag="Satellites", audits=[], sections=gt_fresh_happy(recursive)
                )
            ],
            stopped=True,
        ),
    )

    with pytest.raises(ValueError, match="ontology_children"):
        apply_submission_batch(
            concept_doc, [item], author_email=ALICE, catalog_lookup=gt_catalog_lookup
        )


def _missed_entry(gt_catalog_lookup, gt_fresh_happy, author=ALICE) -> MissedPhraseEntry:
    screening = gt_catalog_lookup("phrase_relationship_screening", "equipments")
    freehand = gt_catalog_lookup("phrase_freehand_grounding", "equipments")
    return MissedPhraseEntry(
        phrase="CNC mills",
        author_email=author,
        at=datetime(2026, 8, 16, tzinfo=timezone.utc),
        source="api",
        relationship_text="machines they run in-house",
        screening=HumanScreeningDerivation(
            identified_entity="CNC mills",
            audits=[_audit(author)],
            sections=gt_fresh_happy(screening),
        ),
        groundings=[
            GroundingDerivation(
                tag="CNC Milling Machine",
                audits=[_audit(author)],
                sections=gt_fresh_happy(freehand),
            )
        ],
    )


def test_missed_phrase_lands_with_witnessed_chunk_text(
    keyword_doc, gt_catalog_lookup, gt_fresh_happy, gt_scraped_text
):
    item = MissedPhraseItem(
        surface="missed_phrase",
        chunk_key=_chunk_key(keyword_doc),
        entry=_missed_entry(gt_catalog_lookup, gt_fresh_happy),
    )

    updated = apply_submission_batch(
        keyword_doc,
        [item],
        author_email=ALICE,
        catalog_lookup=gt_catalog_lookup,
        full_text=gt_scraped_text,
    )

    missed = updated.chunks[_chunk_key(keyword_doc)].missed_phrases
    assert [entry.phrase for entry in missed] == ["CNC mills"]


def test_missed_phrase_without_text_is_a_caller_error(
    keyword_doc, gt_catalog_lookup, gt_fresh_happy
):
    item = MissedPhraseItem(
        surface="missed_phrase",
        chunk_key=_chunk_key(keyword_doc),
        entry=_missed_entry(gt_catalog_lookup, gt_fresh_happy),
    )

    with pytest.raises(ValueError, match="full_text"):
        apply_submission_batch(
            keyword_doc, [item], author_email=ALICE, catalog_lookup=gt_catalog_lookup
        )


def test_tampered_text_fails_the_witness_before_any_slice(
    keyword_doc, gt_catalog_lookup, gt_fresh_happy, gt_scraped_text
):
    item = MissedPhraseItem(
        surface="missed_phrase",
        chunk_key=_chunk_key(keyword_doc),
        entry=_missed_entry(gt_catalog_lookup, gt_fresh_happy),
    )

    with pytest.raises(TextWitnessError):
        apply_submission_batch(
            keyword_doc,
            [item],
            author_email=ALICE,
            catalog_lookup=gt_catalog_lookup,
            full_text=gt_scraped_text + " tampered",
        )


def test_atomicity_a_failing_item_leaves_nothing_behind(
    keyword_doc, gt_catalog_lookup
):
    key = _chunk_key(keyword_doc)
    llm = keyword_doc.chunks[key].extracted_phrases["cnc mill"].llm_screening
    assert llm is not None
    silent_edit = copy.deepcopy(llm.llm_result.sections)
    for section in silent_edit:
        for rule in section.applied_rules:
            if rule.outcome == "satisfied":
                rule.outcome = "failed"  # no audit — the contract rejects it
    bad = ScreeningDerivationItem(
        surface="screening_derivation",
        chunk_key=key,
        phrase="cnc mill",
        derivation=HumanScreeningDerivation(
            identified_entity=llm.llm_result.identified_entity,
            audits=[_audit()],
            sections=silent_edit,
        ),
    )

    with pytest.raises(BatchItemError) as exc_info:
        apply_submission_batch(
            keyword_doc,
            [_rel_item(keyword_doc), bad],
            author_email=ALICE,
            catalog_lookup=gt_catalog_lookup,
        )

    assert exc_info.value.index == 1
    assert exc_info.value.surface == "screening_derivation"
    # The original document carries none of item 0's successful apply.
    assert (
        keyword_doc.chunks[key].extracted_phrases["cnc mill"].llm_relationship.audits
        == []
    )


def test_unknown_chunk_and_unextracted_phrase_are_item_errors(
    keyword_doc, gt_catalog_lookup
):
    with pytest.raises(BatchItemError, match="available"):
        apply_submission_batch(
            keyword_doc,
            [
                RelationshipAuditItem(
                    surface="relationship_audit",
                    chunk_key="9:99",
                    phrase="cnc mill",
                    audit=_audit(),
                )
            ],
            author_email=ALICE,
            catalog_lookup=gt_catalog_lookup,
        )

    with pytest.raises(BatchItemError, match="missed phrase"):
        apply_submission_batch(
            keyword_doc,
            [
                RelationshipAuditItem(
                    surface="relationship_audit",
                    chunk_key=_chunk_key(keyword_doc),
                    phrase="never extracted",
                    audit=_audit(),
                )
            ],
            author_email=ALICE,
            catalog_lookup=gt_catalog_lookup,
        )


def test_catalog_pin_drift_is_batch_level_not_item_level(
    keyword_doc, gt_catalog_lookup
):
    keyword_doc.metadata.llm_phrase_relationship_screening.catalog_version = (
        "stale.0"
    )

    with pytest.raises(TemplateAssemblyError, match="stale.0"):
        apply_submission_batch(
            keyword_doc,
            [_rel_item(keyword_doc)],
            author_email=ALICE,
            catalog_lookup=gt_catalog_lookup,
        )
