from functools import cached_property
from typing import Dict, List

import rdflib
from pydantic import BaseModel, ConfigDict, PrivateAttr

from core.models.field_types import ConceptFieldType
from core.models.skos_concept import Concept, ConceptNode
from core.utils.rdf_to_graph_util import (
    build_concept_tree,
    get_graph,
    tree_list_to_flat,
)


class Ontology(BaseModel):
    """
    Ontology model representing a versioned RDF ontology with lazy-loaded concept subtrees.

    Each instance represents a specific version of the ontology (identified by s3_version_id).
    Concept hierarchies are built on-demand and cached per subtree base URI. *Which* subtrees
    exist is the consuming app's concern — this model only knows how to resolve one.
    """

    model_config = ConfigDict(frozen=True)

    s3_version_id: str
    rdf: str

    _concept_nodes_cache: Dict[str, List[ConceptNode]] = PrivateAttr(
        default_factory=dict
    )
    _concepts_flat_cache: Dict[str, set[Concept]] = PrivateAttr(default_factory=dict)
    _concept_map_cache: Dict[str, Dict[str, Concept]] = PrivateAttr(
        default_factory=dict
    )

    def concept_nodes(self, base_uri: str) -> List[ConceptNode]:
        """Root concept nodes of the subtree under `base_uri`."""
        if not base_uri:
            raise ValueError(
                f"Cannot build a concept tree from ontology:{self.s3_version_id} without a base URI."
            )
        if base_uri not in self._concept_nodes_cache:
            self._concept_nodes_cache[base_uri] = build_concept_tree(
                self.graph, rdflib.URIRef(base_uri)
            )["children"]
        return self._concept_nodes_cache[base_uri]

    def concepts_flat(self, base_uri: str) -> set[Concept]:
        """Flattened concepts of the subtree under `base_uri`."""
        if base_uri not in self._concepts_flat_cache:
            self._concepts_flat_cache[base_uri] = tree_list_to_flat(
                self.concept_nodes(base_uri)
            )
        return self._concepts_flat_cache[base_uri]

    def concept_map(
        self, base_uri: str, include_alt_labels: bool = False
    ) -> Dict[str, Concept]:
        """Map concept names (optionally also altLabels) to Concept objects."""
        cache_key = f"{base_uri}|{include_alt_labels}"
        if cache_key not in self._concept_map_cache:
            mapping: Dict[str, Concept] = {}
            for concept in self.concepts_flat(base_uri):
                mapping[concept.name] = concept
                if include_alt_labels:
                    for alt_label in concept.altLabels:
                        mapping[alt_label] = concept
            self._concept_map_cache[cache_key] = mapping
        return self._concept_map_cache[cache_key]

    def get_concepts_flat(self, concept_type: ConceptFieldType) -> set[Concept]:
        return self.concepts_flat(concept_type.base_uri)

    @property
    def version_id(self) -> str:
        """Alias for s3_version_id for cleaner consumer API."""
        return self.s3_version_id

    @cached_property
    def graph(self) -> rdflib.Graph:
        """Parse and cache the RDF graph."""
        return get_graph(self.rdf)
