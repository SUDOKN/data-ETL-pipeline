from functools import cached_property
from typing import Dict, List

import rdflib
from pydantic import BaseModel, ConfigDict

from data_etl_app.models.skos_concept import Concept, ConceptNode
from data_etl_app.utils.rdf_to_graph_util import (
    build_concept_tree,
    get_graph,
    tree_list_to_flat,
)
from data_etl_app.utils.ontology_uri_util import (
    certificate_base_uri,
    industry_base_uri,
    material_cap_base_uri,
    naics_base_uri,
    ownership_status_base_uri,
    process_cap_base_uri,
)


class Ontology(BaseModel):
    """
    Ontology model representing a versioned RDF ontology with lazy-loaded concept properties.

    Each instance represents a specific version of the ontology (identified by s3_version_id).
    Concept hierarchies are built on-demand and cached using @cached_property.
    """

    model_config = ConfigDict(frozen=True)

    s3_version_id: str
    rdf: str

    @property
    def version_id(self) -> str:
        """Alias for s3_version_id for cleaner consumer API."""
        return self.s3_version_id

    @cached_property
    def graph(self) -> rdflib.Graph:
        """Parse and cache the RDF graph."""
        return get_graph(self.rdf)

    # Process capabilities
    @cached_property
    def process_capability_concept_nodes(self) -> List[ConceptNode]:
        """Build and cache process capability concept tree."""
        base_uri = process_cap_base_uri()
        if not base_uri:
            raise ValueError("Process capability base URI is not set.")
        return build_concept_tree(self.graph, rdflib.URIRef(base_uri), set())[
            "children"
        ]

    @cached_property
    def process_caps(self) -> set[Concept]:
        """Flatten process capability concept tree."""
        return tree_list_to_flat(self.process_capability_concept_nodes)

    @cached_property
    def process_cap_map(self) -> Dict[str, Concept]:
        """Map process capability names to Concept objects."""
        return {cap.name: cap for cap in self.process_caps}

    # Material capabilities
    @cached_property
    def material_capability_concept_nodes(self) -> List[ConceptNode]:
        """Build and cache material capability concept tree."""
        base_uri = material_cap_base_uri()
        if not base_uri:
            raise ValueError("Material capability base URI is not set.")
        return build_concept_tree(self.graph, rdflib.URIRef(base_uri), set())[
            "children"
        ]

    @cached_property
    def material_caps(self) -> set[Concept]:
        """Flatten material capability concept tree."""
        return tree_list_to_flat(self.material_capability_concept_nodes)

    @cached_property
    def material_cap_map(self) -> Dict[str, Concept]:
        """Map material capability names to Concept objects."""
        return {cap.name: cap for cap in self.material_caps}

    # Industries
    @cached_property
    def industry_concept_nodes(self) -> List[ConceptNode]:
        """Build and cache industry concept tree."""
        base_uri = industry_base_uri()
        if not base_uri:
            raise ValueError("Industry base URI is not set.")
        return build_concept_tree(self.graph, rdflib.URIRef(base_uri), set())[
            "children"
        ]

    @cached_property
    def industries(self) -> set[Concept]:
        """Flatten industry concept tree."""
        return tree_list_to_flat(self.industry_concept_nodes)

    @cached_property
    def industry_map(self) -> Dict[str, Concept]:
        """Map industry names to Concept objects."""
        return {ind.name: ind for ind in self.industries}

    # Certificates
    @cached_property
    def certificate_concept_nodes(self) -> List[ConceptNode]:
        """Build and cache certificate concept tree."""
        base_uri = certificate_base_uri()
        if not base_uri:
            raise ValueError("Certificate base URI is not set.")
        return build_concept_tree(self.graph, rdflib.URIRef(base_uri), set())[
            "children"
        ]

    @cached_property
    def certificates(self) -> set[Concept]:
        """Flatten certificate concept tree."""
        return tree_list_to_flat(self.certificate_concept_nodes)

    @cached_property
    def certificate_map(self) -> Dict[str, Concept]:
        """Map certificate names to Concept objects."""
        return {cert.name: cert for cert in self.certificates}

    # Ownership statuses
    @cached_property
    def ownership_concept_nodes(self) -> List[ConceptNode]:
        """Build and cache ownership status concept tree."""
        base_uri = ownership_status_base_uri()
        if not base_uri:
            raise ValueError("Ownership status base URI is not set.")
        return build_concept_tree(self.graph, rdflib.URIRef(base_uri), set())[
            "children"
        ]

    @cached_property
    def ownership_statuses(self) -> set[Concept]:
        """Flatten ownership status concept tree."""
        return tree_list_to_flat(self.ownership_concept_nodes)

    @cached_property
    def ownership_status_map(self) -> Dict[str, Concept]:
        """Map ownership status names (including altLabels) to Concept objects."""
        result = {}
        for status in self.ownership_statuses:
            result[status.name] = status
            for alt_label in status.altLabels:
                result[alt_label] = status
        return result

    # NAICS codes
    @cached_property
    def naics_concept_nodes(self) -> List[ConceptNode]:
        """Build and cache NAICS code concept tree."""
        base_uri = naics_base_uri()
        if not base_uri:
            raise ValueError("NAICS base URI is not set.")
        return build_concept_tree(self.graph, rdflib.URIRef(base_uri), set())[
            "children"
        ]

    @cached_property
    def naics_codes(self) -> set[Concept]:
        """Flatten NAICS code concept tree."""
        return tree_list_to_flat(self.naics_concept_nodes)

    @cached_property
    def naics_code_map(self) -> Dict[str, Concept]:
        """Map NAICS code names to Concept objects."""
        return {code.name: code for code in self.naics_codes}
