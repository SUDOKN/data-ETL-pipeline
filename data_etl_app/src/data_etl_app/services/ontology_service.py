import threading
import rdflib
import logging
from typing import Dict, List

from shared.models.types import OntologyVersionIDType

from data_etl_app.models.skos_concept import Concept, ConceptNode
from data_etl_app.utils.ontology_rdf_util import download_ontology_rdf
from data_etl_app.utils.ontology_uri_util import (
    process_cap_uri,
    material_cap_uri,
    industry_uri,
    certificate_uri,
)
from data_etl_app.utils.rdf_to_knowledge_util import (
    get_graph,
    build_children,
    insert_ancestors,
    insert_dummy_antiLabels,
    transform_node,
    tree_list_to_flat,
)

logger = logging.getLogger(__name__)

BASE_URIS = {
    "process": process_cap_uri(),
    "material": material_cap_uri(),
    "industry": industry_uri(),
    "certificate": certificate_uri(),
}


class OntologyService:
    """
    Singleton service to manage the ontology data and provide access to capabilities.

    THREAD SAFETY NOTES:
    - Currently using single worker (-w 1) in gunicorn, so thread contention is minimal
    - Thread safety is kept for future-proofing and async operation safety
    - If scaling to multiple workers, consider shared state solution (Redis/DB)
    - Lock protects singleton creation and refresh operations

    SCALING OPTIONS:
    1. Single worker (current): Simple, solves refresh problem, may limit throughput
    2. Multiple workers + shared state: Better throughput, requires inter-process communication
    3. Stateless design: Move ontology to external store, eliminate singleton
    """

    _instance: "OntologyService | None" = None
    _lock = threading.Lock()  # Thread safety for singleton creation and refresh
    _cache: Dict[str, List[Concept]]
    graph: rdflib.Graph
    ontology_version_id: OntologyVersionIDType

    def __new__(cls) -> "OntologyService":
        # this gets called before __init__, when anyone calls OntologyService()
        # it ensures that only one instance of the service is created
        # every next time, it will return the same instance
        if cls._instance is None:
            logger.info("OntologyService instance is None, acquiring lock for creation")
            with cls._lock:
                if cls._instance is None:
                    logger.info("Creating new OntologyService singleton instance")
                    cls._instance = super().__new__(cls)
                    cls._instance._init_data()
                else:
                    logger.info(
                        "Another thread created the instance while waiting for lock"
                    )
        else:
            logger.debug("Returning existing OntologyService singleton instance")
        return cls._instance

    def _init_data(self) -> None:
        rdf_content, version_id = download_ontology_rdf(None)
        self.graph = get_graph(rdf_content)
        self.ontology_version_id = version_id
        logger.info(
            f"OntologyService initialized with version ID: {self.ontology_version_id}"
        )
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
        logger.info("Refreshing ontology data - acquiring lock")
        with self._lock:
            logger.info("Lock acquired, starting ontology refresh")
            old_version = getattr(self, "ontology_version_id", "unknown")
            self._init_data()
            logger.info(
                f"Ontology refreshed: {old_version} -> {self.ontology_version_id}"
            )
        logger.info("Ontology refresh completed, lock released")

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
    def process_caps(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        if not hasattr(self, "_process_capabilities"):
            process_trees = self.process_capability_concept_nodes
            for tree in process_trees:
                insert_ancestors(tree, [])
            self._process_capabilities = tree_list_to_flat(process_trees)
        return self.ontology_version_id, self._process_capabilities

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
    def material_caps(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        if not hasattr(self, "_material_capabilities"):
            material_trees = self.material_capability_concept_nodes
            for tree in material_trees:
                insert_ancestors(tree, [])
            self._material_capabilities = tree_list_to_flat(material_trees)
        return self.ontology_version_id, self._material_capabilities

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
    def industries(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        if not hasattr(self, "_industries"):
            industry_trees = self.industry_concept_nodes
            for tree in industry_trees:
                transform_node(tree, insert_dummy_antiLabels)
            self._industries = tree_list_to_flat(industry_trees)
        return self.ontology_version_id, self._industries

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
    def certificates(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        if not hasattr(self, "_certificates"):
            certificate_trees = self.certificate_concept_nodes
            for tree in certificate_trees:
                insert_ancestors(tree, [])
            self._certificates = tree_list_to_flat(certificate_trees)
        return self.ontology_version_id, self._certificates

    def get_service_info(self) -> dict:
        """Return service information for debugging and health checks."""
        return {
            "instance_id": id(self),
            "ontology_version_id": getattr(
                self, "ontology_version_id", "not_initialized"
            ),
            "graph_loaded": hasattr(self, "graph") and self.graph is not None,
            "cached_properties": {
                "process_capabilities": hasattr(self, "_process_capabilities"),
                "material_capabilities": hasattr(self, "_material_capabilities"),
                "industries": hasattr(self, "_industries"),
                "certificates": hasattr(self, "_certificates"),
            },
        }


# Auto-initialize the singleton instance when the module is imported
ontology_service = OntologyService()
