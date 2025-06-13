import threading
import os
import rdflib
from typing import Dict, List

from data_etl_app.utils.s3_util import read_s3_file
from data_etl_app.utils.rdf_to_knowledge import get_graph, build_children
from data_etl_app.models.skos_concept import Concept, ConceptNode
from data_etl_app.utils.rdf_to_knowledge import (
    insert_ancestors,
    insert_dummy_antiLabels,
    transform_node,
    tree_list_to_flat,
)


BASE_URIS = {
    "process": os.getenv("SUDOKN_PROCESS_CAP_BASE_URI"),
    "material": os.getenv("SUDOKN_MATERIAL_CAP_BASE_URI"),
    "industry": os.getenv("SUDOKN_INDUSTRY_BASE_URI"),
    "certificate": os.getenv("SUDOKN_CERTIFICATE_BASE_URI"),
}
# TODO: Add a check to ensure that all BASE_URIS are set

"""
Make sure that the concepts are only ready, never modified.
"""


class OntologyService:
    _instance: "OntologyService | None" = None
    _lock = (
        threading.Lock()
    )  # not strictly necessary, but good practice for thread safety, read in notes
    graph: rdflib.Graph
    _cache: Dict[str, List[Concept]]

    """Singleton service to manage the ontology data and provide access to capabilities."""

    def __new__(cls) -> "OntologyService":
        # this gets called before __init__, when anyone calls OntologyService()
        # it ensures that only one instance of the service is created
        # every next time, it will return the same instance
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._init_data()
        return cls._instance

    def _init_data(self) -> None:
        rdf_content = read_s3_file("sudokn-ontology", "SUDOKN.rdf")
        self.graph = get_graph(rdf_content)
        # Clear all cached properties for concept nodes and processed lists
        for attr in [
            "_process_capability_concept_nodes",
            "_material_capability_concept_nodes",
            "_industry_concept_nodes",
            "_certificate_concept_nodes",
            "_process_capabilities",
            "_material_capabilities",
            "_industries",
            "_certificates",
        ]:
            if hasattr(self, attr):
                delattr(self, attr)

    def refresh(self) -> None:
        """Reload ontology data from S3 and clear all cached properties."""
        with self._lock:
            self._init_data()

    @property
    def process_capability_concept_nodes(self) -> List[ConceptNode]:
        if BASE_URIS["process"] is None:
            raise ValueError(
                "BASE_URIS['process'] is not set. Cannot build process capabilities."
            )
        if not hasattr(self, "_process_capability_concept_nodes"):
            self._process_capability_concept_nodes = build_children(
                self.graph, rdflib.URIRef(BASE_URIS["process"])
            )
        return self._process_capability_concept_nodes

    @property
    def process_capabilities(self) -> List[Concept]:
        if not hasattr(self, "_process_capabilities"):
            process_trees = self.process_capability_concept_nodes
            for tree in process_trees:
                insert_ancestors(tree, [])
            self._process_capabilities = tree_list_to_flat(process_trees)
        return self._process_capabilities

    @property
    def material_capability_concept_nodes(self) -> List[ConceptNode]:
        if BASE_URIS["material"] is None:
            raise ValueError(
                "BASE_URIS['material'] is not set. Cannot build material capabilities."
            )
        if not hasattr(self, "_material_capability_concept_nodes"):
            self._material_capability_concept_nodes = build_children(
                self.graph, rdflib.URIRef(BASE_URIS["material"])
            )
        return self._material_capability_concept_nodes

    @property
    def material_capabilities(self) -> List[Concept]:
        if not hasattr(self, "_material_capabilities"):
            material_trees = self.material_capability_concept_nodes
            for tree in material_trees:
                insert_ancestors(tree, [])
            self._material_capabilities = tree_list_to_flat(material_trees)
        return self._material_capabilities

    @property
    def industry_concept_nodes(self) -> List[ConceptNode]:
        if BASE_URIS["industry"] is None:
            raise ValueError(
                "BASE_URIS['industry'] is not set. Cannot build industry capabilities."
            )
        if not hasattr(self, "_industry_concept_nodes"):
            self._industry_concept_nodes = build_children(
                self.graph, rdflib.URIRef(BASE_URIS["industry"])
            )
        return self._industry_concept_nodes

    @property
    def industries(self) -> List[Concept]:
        if not hasattr(self, "_industries"):
            industry_trees = self.industry_concept_nodes
            for tree in industry_trees:
                transform_node(tree, insert_dummy_antiLabels)
            self._industries = tree_list_to_flat(industry_trees)
        return self._industries

    @property
    def certificate_concept_nodes(self) -> List[ConceptNode]:
        if BASE_URIS["certificate"] is None:
            raise ValueError(
                "BASE_URIS['certificate'] is not set. Cannot build certificate capabilities."
            )
        if not hasattr(self, "_certificate_concept_nodes"):
            self._certificate_concept_nodes = build_children(
                self.graph, rdflib.URIRef(BASE_URIS["certificate"])
            )
        return self._certificate_concept_nodes

    @property
    def certificates(self) -> List[Concept]:
        if not hasattr(self, "_certificates"):
            certificate_trees = self.certificate_concept_nodes
            for tree in certificate_trees:
                insert_ancestors(tree, [])
            self._certificates = tree_list_to_flat(certificate_trees)
        return self._certificates


# Auto-initialize the singleton instance when the module is imported
ontology_service = OntologyService()
