import asyncio
import rdflib
import logging
from typing import Dict, List

from core.models.field_types import OntologyVersionIDType

from data_etl_app.models.skos_concept import Concept, ConceptNode
from data_etl_app.utils.ontology_rdf_s3_util import download_ontology_rdf
from data_etl_app.utils.ontology_uri_util import (
    process_cap_uri,
    material_cap_uri,
    industry_uri,
    certificate_uri,
)
from data_etl_app.utils.rdf_to_knowledge_util import (
    get_graph,
    build_children,
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
    - If scaling to multiple workers, consider core state solution (Redis/DB)
    - Lock protects singleton creation and refresh operations

    SCALING OPTIONS:
    1. Single worker (current): Simple, solves refresh problem, may limit throughput
    2. Multiple workers + core state: Better throughput, requires inter-process communication
    3. Stateless design: Move ontology to external store, eliminate singleton
    """

    _instance: "OntologyService | None" = None
    _lock = asyncio.Lock()
    _initialized = False

    def __init__(self):
        self._cache: Dict[str, List[Concept]] = {}
        self.graph: rdflib.Graph | None = None
        self.ontology_version_id: OntologyVersionIDType | None = None

    @classmethod
    async def get_instance(cls) -> "OntologyService":
        """Get the singleton instance with lazy initialization."""
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    logger.info("Creating new OntologyService singleton instance")
                    cls._instance = cls()

        # Initialize data if not already done
        if not cls._initialized:
            async with cls._lock:
                if not cls._initialized:
                    logger.info("Initializing OntologyService data")
                    await cls._instance._init_data()
                    cls._initialized = True

        return cls._instance

    async def _init_data(self) -> None:
        """Initialize ontology data by downloading from S3."""
        try:
            rdf_content, version_id = await download_ontology_rdf(None)
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
        except Exception as e:
            logger.error(f"Failed to initialize ontology service: {e}")
            raise

    async def refresh(self) -> None:
        """Reload ontology data from S3 and clear all cached properties."""
        logger.info("Refreshing ontology data - acquiring lock")
        async with self._lock:
            logger.info("Lock acquired, starting ontology refresh")
            old_version = getattr(self, "ontology_version_id", "unknown")
            await self._init_data()
            logger.info(
                f"Ontology refreshed: {old_version} -> {self.ontology_version_id}"
            )
        logger.info("Ontology refresh completed, lock released")

    def _ensure_initialized(self) -> None:
        """Ensure the service is properly initialized."""
        if not self._initialized:
            raise RuntimeError(
                "OntologyService not initialized. Call get_instance() first."
            )
        if self.graph is None or self.ontology_version_id is None:
            raise RuntimeError("OntologyService initialization incomplete.")

    @property
    def process_capability_concept_nodes(
        self,
    ) -> tuple[OntologyVersionIDType, List[ConceptNode]]:
        self._ensure_initialized()
        if BASE_URIS["process"] is None:
            raise ValueError(
                "BASE_URIS['process'] is not set. Cannot build process capabilities."
            )
        if not hasattr(self, "_process_capability_concept_nodes"):
            # Type assertion safe after _ensure_initialized check
            assert self.graph is not None
            self._process_capability_concept_nodes = build_children(
                self.graph, rdflib.URIRef(BASE_URIS["process"])
            )
        # Type assertion safe after _ensure_initialized check
        assert self.ontology_version_id is not None
        return self.ontology_version_id, self._process_capability_concept_nodes

    @property
    def process_caps(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_process_capabilities"):
            self._process_capabilities = tree_list_to_flat(
                self.process_capability_concept_nodes[1]
            )
        # Type assertion safe after _ensure_initialized check
        assert self.ontology_version_id is not None
        return self.ontology_version_id, self._process_capabilities

    @property
    def material_capability_concept_nodes(
        self,
    ) -> tuple[OntologyVersionIDType, List[ConceptNode]]:
        self._ensure_initialized()
        if BASE_URIS["material"] is None:
            raise ValueError(
                "BASE_URIS['material'] is not set. Cannot build material capabilities."
            )
        if not hasattr(self, "_material_capability_concept_nodes"):
            # Type assertion safe after _ensure_initialized check
            assert self.graph is not None
            self._material_capability_concept_nodes = build_children(
                self.graph, rdflib.URIRef(BASE_URIS["material"])
            )
        # Type assertion safe after _ensure_initialized check
        assert self.ontology_version_id is not None
        return self.ontology_version_id, self._material_capability_concept_nodes

    @property
    def material_caps(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_material_capabilities"):
            self._material_capabilities = tree_list_to_flat(
                self.material_capability_concept_nodes[1]
            )
        # Type assertion safe after _ensure_initialized check
        assert self.ontology_version_id is not None
        return self.ontology_version_id, self._material_capabilities

    @property
    def industry_concept_nodes(self) -> tuple[OntologyVersionIDType, List[ConceptNode]]:
        self._ensure_initialized()
        if BASE_URIS["industry"] is None:
            raise ValueError(
                "BASE_URIS['industry'] is not set. Cannot build industry capabilities."
            )
        if not hasattr(self, "_industry_concept_nodes"):
            # Type assertion safe after _ensure_initialized check
            assert self.graph is not None
            self._industry_concept_nodes = build_children(
                self.graph, rdflib.URIRef(BASE_URIS["industry"])
            )
        # Type assertion safe after _ensure_initialized check
        assert self.ontology_version_id is not None
        return self.ontology_version_id, self._industry_concept_nodes

    @property
    def industries(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_industries"):
            self._industries = tree_list_to_flat(self.industry_concept_nodes[1])
        # Type assertion safe after _ensure_initialized check
        assert self.ontology_version_id is not None
        return self.ontology_version_id, self._industries

    @property
    def certificate_concept_nodes(
        self,
    ) -> tuple[OntologyVersionIDType, List[ConceptNode]]:
        self._ensure_initialized()
        if BASE_URIS["certificate"] is None:
            raise ValueError(
                "BASE_URIS['certificate'] is not set. Cannot build certificate capabilities."
            )
        if not hasattr(self, "_certificate_concept_nodes"):
            # Type assertion safe after _ensure_initialized check
            assert self.graph is not None
            self._certificate_concept_nodes = build_children(
                self.graph, rdflib.URIRef(BASE_URIS["certificate"])
            )
        # Type assertion safe after _ensure_initialized check
        assert self.ontology_version_id is not None
        return self.ontology_version_id, self._certificate_concept_nodes

    @property
    def certificates(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_certificates"):
            self._certificates = tree_list_to_flat(self.certificate_concept_nodes[1])
        # Type assertion safe after _ensure_initialized check
        assert self.ontology_version_id is not None
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


# Factory function for getting the service instance
async def get_ontology_service() -> OntologyService:
    """Factory function to get the OntologyService instance."""
    return await OntologyService.get_instance()
