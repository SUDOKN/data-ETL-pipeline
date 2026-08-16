from __future__ import annotations

import hashlib
import json
from typing import Optional

from pydantic import BaseModel, ConfigDict, model_validator

from core.field_types import OntologyVersionIDType
from core.models.chunking_strat import ChunkingStrategy
from core.models.extraction_results.llm_phrase_extraction_results import (
    ExtractionNodeMetadata,
    LLMPhraseExtractionMetadata,
)
from infra.field_types import S3FileVersionIDType
from llm_providers.models.open_ai.gpt_model_params import GPTModelParams


class NodeIdentity(BaseModel):
    """One pipeline node's configuration, reduced to what changes answers.

    Everything is explicit per the I1/PH-6 verdict — the annotator reading two
    identities should see *what* differed, not just that something did. That
    includes ``model_params``, stored whole rather than hashed (user, 08-15:
    "I would like to know: what was the mismatch?"). Model equality stays
    order-insensitive for its dict-valued fields (``response_format``) because
    dict equality is; only index-key mechanics care about order, and that is
    the Document layer's problem (P1.5).

    The caps are here because a different batching cap regroups which phrases
    share a request, which changes answers (see
    ``ExtractionNodeMetadata.to_custom_id_segment``).

    Consciously excluded: ``created_at`` (a timestamp is when, not what) and
    ``prompt_name`` (addressing; ``prompt_version_id`` already pins the
    bytes). The tripwire test records both exclusions.
    """

    model_config = ConfigDict(extra="forbid")

    llm_model: str
    model_params: GPTModelParams
    prompt_version_id: S3FileVersionIDType
    catalog_version: Optional[str]
    max_rounds: Optional[int] = None
    max_phrases_per_request: Optional[int] = None
    max_pairs_per_request: Optional[int] = None

    @classmethod
    def from_node(cls, node: ExtractionNodeMetadata) -> "NodeIdentity":
        return cls(
            llm_model=node.llm_model.name,
            model_params=node.model_params,
            prompt_version_id=node.prompt_version_id,
            catalog_version=node.catalog_version,
            max_rounds=getattr(node, "max_rounds", None),
            max_phrases_per_request=getattr(node, "max_phrases_per_request", None),
            max_pairs_per_request=getattr(node, "max_pairs_per_request", None),
        )


# The grounding node names on KeywordExtractionMetadata / ConceptExtractionMetadata.
# Pulled by attribute so ``from_metadata`` serves all three metadata classes; the
# tripwire test pins the names to the metadata models.
_OPTIONAL_NODE_NAMES = (
    "llm_phrase_freehand_grounding",
    "llm_phrase_initial_grounding",
    "llm_phrase_recursive_grounding",
)


class ExplicitRunIdentity(BaseModel):
    """The run configuration a ground-truth document is keyed by (I1).

    Any version change here is a different ground truth. The four shared
    phrase-pipeline nodes are always present; the grounding nodes mirror the
    metadata subclass the run used — freehand for keyword fields, initial +
    recursive (always together) for concept fields.

    The whole identity IS the unique index (user, 08-15): any parameter two
    runs can differ on keys their ground truths apart — a run is quite
    literally identified by its identity. The index *mechanics* (dotted paths
    vs a validated-cache digest) are the Document layer's decision (P1.5);
    nothing here is exempt from keying.
    """

    model_config = ConfigDict(extra="forbid")

    chunk_strat: ChunkingStrategy
    ontology_version_id: OntologyVersionIDType
    llm_phrase_search: NodeIdentity
    llm_phrase_recursive_search: NodeIdentity
    llm_phrase_relationship: NodeIdentity
    llm_phrase_relationship_screening: NodeIdentity
    llm_phrase_freehand_grounding: Optional[NodeIdentity] = None
    llm_phrase_initial_grounding: Optional[NodeIdentity] = None
    llm_phrase_recursive_grounding: Optional[NodeIdentity] = None

    @model_validator(mode="after")
    def check_grounding_nodes_match_a_field_family(self) -> "ExplicitRunIdentity":
        concept_nodes = (
            self.llm_phrase_initial_grounding,
            self.llm_phrase_recursive_grounding,
        )
        if any(n is not None for n in concept_nodes) and not all(
            n is not None for n in concept_nodes
        ):
            raise ValueError(
                "concept runs ground initially and recursively — the two node "
                "identities come together or not at all"
            )
        if self.llm_phrase_freehand_grounding is not None and any(
            n is not None for n in concept_nodes
        ):
            raise ValueError(
                "a run grounds freehand (keyword fields) or through the "
                "ontology (concept fields), never both"
            )
        return self

    def canonical_digest(self) -> str:
        """The identity's fingerprint — what the unique index keys on (PH-11 b).

        Canonical JSON with recursively sorted keys, so the insertion order of
        dict-valued params (``response_format``) can never split two equal
        identities into two index keys. The digest is stored on the document
        as a validated cache (the ``passed`` pattern): the explicit fields
        answer "what was the mismatch", the digest enforces "a run is
        identified by its identity".
        """
        canonical = json.dumps(self.model_dump(mode="json"), sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @classmethod
    def from_metadata(
        cls, metadata: LLMPhraseExtractionMetadata
    ) -> "ExplicitRunIdentity":
        optional_nodes = {
            name: NodeIdentity.from_node(node)
            for name in _OPTIONAL_NODE_NAMES
            if (node := getattr(metadata, name, None)) is not None
        }
        return cls(
            chunk_strat=metadata.chunk_strat,
            ontology_version_id=metadata.ontology_version_id,
            llm_phrase_search=NodeIdentity.from_node(metadata.llm_phrase_search),
            llm_phrase_recursive_search=NodeIdentity.from_node(
                metadata.llm_phrase_recursive_search
            ),
            llm_phrase_relationship=NodeIdentity.from_node(
                metadata.llm_phrase_relationship
            ),
            llm_phrase_relationship_screening=NodeIdentity.from_node(
                metadata.llm_phrase_relationship_screening
            ),
            **optional_nodes,
        )
