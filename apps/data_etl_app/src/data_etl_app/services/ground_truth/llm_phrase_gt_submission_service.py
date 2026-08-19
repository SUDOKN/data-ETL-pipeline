"""The batch submission plane: addressed items → one atomic document update.

One POST carries a sitting's judgments (P3-2): each item addresses a node of
the document (chunk, phrase, and for groundings the tag / descent
coordinates) and carries the exact core payload model. ``apply_submission_batch``
works on a DEEP COPY and applies items in order through the P2.3/P3.5a
validator-mutators — the caller persists the returned copy only when every
item passed, so atomicity holds by construction: nothing observable mutates
on failure.

Batch-level failures (catalog pin drift, text-witness mismatch, missing
caller-supplied dependencies) raise their own error types; a rejected ITEM
raises ``BatchItemError`` naming its index, surface, and reason — the tight
fix-and-resubmit loop the charter promised.
"""

from __future__ import annotations

from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from core.models.ground_truth.audits import TextFieldAudit
from core.models.ground_truth.stage_blocks import (
    ChunkGT,
    ExtractedPhraseGT,
    GroundingDerivation,
    HumanDescentPath,
    HumanScreeningDerivation,
    MissedPhraseEntry,
    OovGroundingGT,
    TagGroundingGT,
)
from core.services.ground_truth.audit_submission_validation import (
    AuditSubmissionError,
    OntologyChildren,
    append_text_audit,
    submit_descent_divergence,
    submit_grounding_derivation,
    submit_in_vocab_node_audit,
    submit_missed_phrase,
    submit_screening_derivation,
)
from core.services.ontology_service import get_ontology_service
from data_etl_app.db_models.llm_phrase_ground_truth import LLMPhraseGroundTruth
from data_etl_app.models.types_and_enums import ConceptTypeEnum
from data_etl_app.services.ground_truth.llm_phrase_gt_template_service import (
    CatalogLookup,
    PinnedCatalogs,
    resolve_pinned_catalogs,
)
from data_etl_app.services.ground_truth.llm_phrase_gt_view_service import (
    witnessed_chunk_text,
)
from data_etl_app.services.prompt_assembly_service import build_rule_catalog_lookup


# --- the addressed items -----------------------------------------------------


class RelationshipAuditItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    surface: Literal["relationship_audit"]
    chunk_key: str
    phrase: str
    audit: TextFieldAudit


class ScreeningDerivationItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    surface: Literal["screening_derivation"]
    chunk_key: str
    phrase: str
    derivation: HumanScreeningDerivation


class OovGroundingItem(BaseModel):
    """``tag`` addresses the entry in the phrase's tag map — the LLM's tag
    for keep/replace, or the (equal) new tag for a human-asserted entry."""

    model_config = ConfigDict(extra="forbid")
    surface: Literal["oov_grounding_derivation"]
    chunk_key: str
    phrase: str
    tag: str
    derivation: GroundingDerivation


class InVocabNodeAuditItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    surface: Literal["in_vocab_node_audit"]
    chunk_key: str
    phrase: str
    level: int
    parent_group_id: Optional[str]
    group_id: str
    derivation: GroundingDerivation


class DescentDivergenceItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    surface: Literal["descent_divergence"]
    chunk_key: str
    phrase: str
    path: HumanDescentPath


class MissedPhraseItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    surface: Literal["missed_phrase"]
    chunk_key: str
    entry: MissedPhraseEntry


SubmissionItem = Annotated[
    Union[
        RelationshipAuditItem,
        ScreeningDerivationItem,
        OovGroundingItem,
        InVocabNodeAuditItem,
        DescentDivergenceItem,
        MissedPhraseItem,
    ],
    Field(discriminator="surface"),
]


class BatchItemError(ValueError):
    """One item the contract rejected — nothing was persisted."""

    def __init__(self, index: int, surface: str, reason: str):
        self.index = index
        self.surface = surface
        self.reason = reason
        super().__init__(f"item {index} ({surface}): {reason}")


# --- ontology predicate (verdict 6d) -----------------------------------------


def children_from_concept_map(concept_map: dict) -> OntologyChildren:
    """The sync edge-lookup the core validator takes: name → children names,
    None when the name is not a concept."""

    def children(name: str) -> Optional[list[str]]:
        concept = concept_map.get(name)
        return None if concept is None else list(concept.children)

    return children


async def build_ontology_children(
    ontology_version_id: str, field_type: ConceptTypeEnum
) -> OntologyChildren:
    """Edge lookup over the ontology version the run PINNED — divergence hops
    are validated against the tree the run actually descended."""
    service = await get_ontology_service()
    ontology = await service.get_ontology(ontology_version_id)
    return children_from_concept_map(ontology.concept_map(field_type.base_uri))


# --- the batch ---------------------------------------------------------------


def _chunk_or_error(doc: LLMPhraseGroundTruth, chunk_key: str) -> ChunkGT:
    chunk = doc.chunks.get(chunk_key)
    if chunk is None:
        available = sorted(doc.chunks, key=lambda key: int(key.split(":")[0]))
        raise AuditSubmissionError(
            f"no chunk {chunk_key!r} in this document — available: {available}"
        )
    return chunk


def _phrase_or_error(chunk: ChunkGT, chunk_key: str, phrase: str) -> ExtractedPhraseGT:
    phrase_gt = chunk.extracted_phrases.get(phrase)
    if phrase_gt is None:
        raise AuditSubmissionError(
            f"phrase {phrase!r} was not extracted in chunk {chunk_key!r} — "
            f"a phrase the run missed is asserted as a missed phrase"
        )
    return phrase_gt


def _apply_one(
    doc: LLMPhraseGroundTruth,
    item: (
        RelationshipAuditItem
        | ScreeningDerivationItem
        | OovGroundingItem
        | InVocabNodeAuditItem
        | DescentDivergenceItem
        | MissedPhraseItem
    ),
    *,
    author_email: str,
    catalogs: PinnedCatalogs,
    ontology_children: Optional[OntologyChildren],
    full_text: Optional[str],
) -> None:
    chunk = _chunk_or_error(doc, item.chunk_key)

    if isinstance(item, MissedPhraseItem):
        if item.entry.author_email != author_email:
            raise AuditSubmissionError(
                f"missed phrase authored by {item.entry.author_email!r} in a "
                f"submission by {author_email!r}"
            )
        if full_text is None:
            raise ValueError(
                "a missed-phrase item needs the scraped text — the caller "
                "fetches the pinned S3 version and passes full_text"
            )
        chunk_text = witnessed_chunk_text(doc, item.chunk_key, full_text)
        submit_missed_phrase(
            chunk,
            item.entry,
            chunk_text=chunk_text,
            screening_catalog=catalogs.screening,
            grounding_catalog=catalogs.oov,
        )
        return

    phrase_gt = _phrase_or_error(chunk, item.chunk_key, item.phrase)

    if isinstance(item, RelationshipAuditItem):
        if item.audit.author_email != author_email:
            raise AuditSubmissionError(
                f"audit authored by {item.audit.author_email!r} in a "
                f"submission by {author_email!r}"
            )
        append_text_audit(phrase_gt.llm_relationship.audits, item.audit)
        return

    if isinstance(item, ScreeningDerivationItem):
        if phrase_gt.llm_screening is None:
            raise AuditSubmissionError(
                f"the run's screening stage did not reach {item.phrase!r} — "
                f"there is no LLM verdict to audit, and the document cannot "
                f"hold a human screening without one"
            )
        submit_screening_derivation(
            phrase_gt.llm_screening,
            item.derivation,
            author_email=author_email,
            catalog=catalogs.screening,
        )
        return

    if isinstance(item, OovGroundingItem):
        block = phrase_gt.oov_grounding
        entry = block.tags.get(item.tag) if block is not None else None
        if entry is not None:
            submit_grounding_derivation(
                entry.audits,
                item.derivation,
                author_email=author_email,
                llm_tag=item.tag,
                llm_sections=entry.llm_result,
                catalog=catalogs.oov,
            )
            return
        # A tag the map does not hold: a human-asserted entry, keyed by the
        # derivation's own tag.
        if item.tag != item.derivation.tag:
            raise AuditSubmissionError(
                f"no stored entry for tag {item.tag!r} — a new human-asserted "
                f"entry is addressed by the derivation's own tag "
                f"({item.derivation.tag!r})"
            )
        audits_list: list[GroundingDerivation] = []
        submit_grounding_derivation(
            audits_list,
            item.derivation,
            author_email=author_email,
            llm_tag=item.derivation.tag,
            llm_sections=None,
            catalog=catalogs.oov,
        )
        if block is None:
            block = OovGroundingGT(tags={})
            phrase_gt.oov_grounding = block
        block.tags[item.derivation.tag] = TagGroundingGT(
            llm_result=None, audits=audits_list
        )
        return

    # Both remaining surfaces live on the descent tree.
    if phrase_gt.in_vocab_grounding is None:
        raise AuditSubmissionError(
            f"{item.phrase!r} has no recursive-descent record in this run — "
            f"nothing to audit or diverge from"
        )

    if isinstance(item, InVocabNodeAuditItem):
        node = next(
            (
                candidate
                for candidate in phrase_gt.in_vocab_grounding.levels.get(
                    item.level, []
                )
                if candidate.parent_group_id == item.parent_group_id
                and candidate.group_id == item.group_id
            ),
            None,
        )
        if node is None:
            raise AuditSubmissionError(
                f"no stored descent node at level {item.level} with parent "
                f"{item.parent_group_id!r} and group {item.group_id!r}"
            )
        submit_in_vocab_node_audit(
            node,
            item.derivation,
            author_email=author_email,
            catalogs=[
                catalog
                for catalog in (catalogs.initial, catalogs.recursive)
                if catalog is not None
            ],
        )
        return

    if catalogs.recursive is None:
        raise AuditSubmissionError(
            "this field family has no recursive grounding — divergence paths "
            "do not apply"
        )
    if ontology_children is None:
        raise ValueError(
            "a divergence item needs the pinned ontology — the caller builds "
            "ontology_children from the document's ontology_version_id"
        )
    submit_descent_divergence(
        phrase_gt.in_vocab_grounding,
        item.path,
        author_email=author_email,
        recursive_catalog=catalogs.recursive,
        ontology_children=ontology_children,
    )


def apply_submission_batch(
    doc: LLMPhraseGroundTruth,
    items: list[SubmissionItem],
    *,
    author_email: str,
    catalog_lookup: Optional[CatalogLookup] = None,
    ontology_children: Optional[OntologyChildren] = None,
    full_text: Optional[str] = None,
) -> LLMPhraseGroundTruth:
    """All items applied in order on a deep copy, returned only if every one
    passed. The input document is never mutated."""
    updated = doc.model_copy(deep=True)
    lookup = (
        catalog_lookup if catalog_lookup is not None else build_rule_catalog_lookup()
    )
    catalogs = resolve_pinned_catalogs(updated.metadata, updated.field_type, lookup)
    for index, item in enumerate(items):
        try:
            _apply_one(
                updated,
                item,
                author_email=author_email,
                catalogs=catalogs,
                ontology_children=ontology_children,
                full_text=full_text,
            )
        except AuditSubmissionError as error:
            raise BatchItemError(index, item.surface, str(error)) from error
    return updated
