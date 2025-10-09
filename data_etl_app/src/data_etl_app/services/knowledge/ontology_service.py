import asyncio
import rdflib
import logging
from typing import Dict, List, Optional

from core.models.field_types import OntologyVersionIDType

from data_etl_app.models.skos_concept import Concept, ConceptNode
from data_etl_app.models.ontology import Ontology
from data_etl_app.utils.ontology_rdf_s3_util import download_ontology_rdf
from data_etl_app.utils.ontology_uri_util import (
    ownership_status_base_uri,
    process_cap_base_uri,
    material_cap_base_uri,
    industry_base_uri,
    certificate_base_uri,
    naics_base_uri,
)
from data_etl_app.utils.rdf_to_graph_util import (
    get_graph,
    build_concept_tree,
    tree_list_to_flat,
)

logger = logging.getLogger(__name__)

BASE_URIS = {
    "process": process_cap_base_uri(),
    "material": material_cap_base_uri(),
    "industry": industry_base_uri(),
    "certificate": certificate_base_uri(),
    "ownership_status": ownership_status_base_uri(),
    "naics": naics_base_uri(),
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
        self.graph: rdflib.Graph
        self.ontology: Ontology

    @classmethod
    async def get_instance(
        cls, ontology: Optional[Ontology] = None
    ) -> "OntologyService":
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
                    await cls._instance._init_data(ontology)
                    cls._initialized = True

        return cls._instance

    async def _init_data(self, ontology: Optional[Ontology] = None) -> None:
        """Initialize ontology data by downloading from S3."""
        try:
            self.ontology = ontology or await download_ontology_rdf(None)
            self.graph = get_graph(self.ontology.rdf)
            logger.info(
                f"OntologyService initialized with version ID: {self.ontology.s3_version_id}"
            )
            # Clear all cached properties for concept nodes and processed lists
            for attr in [
                "_process_capability_concept_nodes",
                "_material_capability_concept_nodes",
                "_industry_concept_nodes",
                "_certificate_concept_nodes",
                "_ownership_concept_nodes",
                "_naics_concept_nodes",
                "_process_capabilities",
                "_material_capabilities",
                "_industries",
                "_certificates",
                "_ownership_statuses",
                "_naics_codes",
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
            old_version = self.ontology.s3_version_id if self.ontology else None
            await self._init_data()
            assert self.ontology
            logger.info(
                f"Ontology refreshed: {old_version} -> {self.ontology.s3_version_id}"
            )
        logger.info("Ontology refresh completed, lock released")

    def _ensure_initialized(self) -> None:
        """Ensure the service is properly initialized."""
        if not self._initialized:
            raise RuntimeError(
                "OntologyService not initialized. Call get_instance() first."
            )
        if self.graph is None or self.ontology is None:
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
            self._process_capability_concept_nodes = build_concept_tree(
                self.graph, rdflib.URIRef(BASE_URIS["process"]), set()
            )["children"]
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._process_capability_concept_nodes

    @property
    def process_caps(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_process_capabilities"):
            self._process_capabilities = tree_list_to_flat(
                self.process_capability_concept_nodes[1]
            )
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._process_capabilities

    @property
    def process_cap_map(self) -> tuple[OntologyVersionIDType, Dict[str, Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_process_capabilities_map"):
            _, process_caps = self.process_caps
            self._process_capabilities_map = {cap.name: cap for cap in process_caps}
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._process_capabilities_map

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
            self._material_capability_concept_nodes = build_concept_tree(
                self.graph, rdflib.URIRef(BASE_URIS["material"]), set()
            )["children"]
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._material_capability_concept_nodes

    @property
    def material_caps(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_material_capabilities"):
            self._material_capabilities = tree_list_to_flat(
                self.material_capability_concept_nodes[1]
            )
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._material_capabilities

    @property
    def material_cap_map(self) -> tuple[OntologyVersionIDType, Dict[str, Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_material_capabilities_map"):
            _, material_caps = self.material_caps
            self._material_capabilities_map = {cap.name: cap for cap in material_caps}
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._material_capabilities_map

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
            self._industry_concept_nodes = build_concept_tree(
                self.graph, rdflib.URIRef(BASE_URIS["industry"]), set()
            )["children"]
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._industry_concept_nodes

    @property
    def industries(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_industries"):
            self._industries = tree_list_to_flat(self.industry_concept_nodes[1])
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._industries

    @property
    def industry_map(self) -> tuple[OntologyVersionIDType, Dict[str, Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_industries_map"):
            _, industries = self.industries
            self._industries_map = {ind.name: ind for ind in industries}
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._industries_map

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
            self._certificate_concept_nodes = build_concept_tree(
                self.graph, rdflib.URIRef(BASE_URIS["certificate"]), set()
            )["children"]
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._certificate_concept_nodes

    @property
    def certificates(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_certificates"):
            self._certificates = tree_list_to_flat(self.certificate_concept_nodes[1])
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._certificates

    @property
    def certificate_map(self) -> tuple[OntologyVersionIDType, Dict[str, Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_certificate_map"):
            _, certificates = self.certificates
            self._certificate_map = {cert.name: cert for cert in certificates}
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._certificate_map

    @property
    def ownership_concept_nodes(
        self,
    ) -> tuple[OntologyVersionIDType, List[ConceptNode]]:
        self._ensure_initialized()
        if BASE_URIS.get("ownership_status") is None:
            raise ValueError(
                "BASE_URIS['ownership_status'] is not set. Cannot build ownership status capabilities."
            )
        if not hasattr(self, "_ownership_concept_nodes"):
            # Type assertion safe after _ensure_initialized check
            assert self.graph is not None
            self._ownership_concept_nodes = build_concept_tree(
                self.graph, rdflib.URIRef(BASE_URIS["ownership_status"]), set()
            )["children"]
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._ownership_concept_nodes

    @property
    def ownership_statuses(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_ownership_statuses"):
            self._ownership_statuses = tree_list_to_flat(
                self.ownership_concept_nodes[1]
            )
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._ownership_statuses

    @property
    def ownership_status_map(self) -> tuple[OntologyVersionIDType, Dict[str, Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_ownership_status_map"):
            _, ownership_statuses = self.ownership_statuses
            self._ownership_status_map = {
                status.name: status for status in ownership_statuses
            }
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._ownership_status_map

    @property
    def naics_concept_nodes(
        self,
    ) -> tuple[OntologyVersionIDType, List[ConceptNode]]:
        self._ensure_initialized()
        if BASE_URIS.get("naics") is None:
            raise ValueError(
                "BASE_URIS['naics'] is not set. Cannot build NAICS capabilities."
            )
        if not hasattr(self, "_naics_concept_nodes"):
            # Type assertion safe after _ensure_initialized check
            assert self.graph is not None
            self._naics_concept_nodes = build_concept_tree(
                self.graph, rdflib.URIRef(BASE_URIS["naics"]), set()
            )["children"]
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._naics_concept_nodes

    @property
    def naics_codes(self) -> tuple[OntologyVersionIDType, List[Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_naics_codes"):
            self._naics_codes = tree_list_to_flat(self.naics_concept_nodes[1])
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._naics_codes

    @property
    def naics_code_map(self) -> tuple[OntologyVersionIDType, Dict[str, Concept]]:
        self._ensure_initialized()
        if not hasattr(self, "_naics_code_map"):
            _, naics_codes = self.naics_codes
            self._naics_code_map = {code.name: code for code in naics_codes}
        # Type assertion safe after _ensure_initialized check
        assert self.ontology is not None
        return self.ontology.s3_version_id, self._naics_code_map

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
                "ownership_statuses": hasattr(self, "_ownership_statuses"),
                "naics_codes": hasattr(self, "_naics_codes"),
            },
        }


# Factory function for getting the service instance
async def get_ontology_service(ontology: Optional[Ontology] = None) -> OntologyService:
    """Factory function to get the OntologyService instance."""
    return await OntologyService.get_instance(ontology)
