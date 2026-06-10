import asyncio
import logging
from typing import Dict, Optional

from data_etl_app.models.ontology import Ontology
from data_etl_app.utils.ontology_rdf_s3_util import download_ontology_rdf

logger = logging.getLogger(__name__)


class OntologyService:
    """
    Singleton service managing a cache of versioned Ontology instances.

    THREAD SAFETY NOTES:
    - Currently using single worker (-w 1) in gunicorn, so thread contention is minimal
    - Thread safety is kept for future-proofing and async operation safety
    - If scaling to multiple workers, consider shared state solution (Redis/DB)
    - Lock protects singleton creation, cache updates, and refresh operations

    SCALING OPTIONS:
    1. Single worker (current): Simple, solves refresh problem, may limit throughput
    2. Multiple workers + shared state: Better throughput, requires inter-process communication
    3. Stateless design: Move ontology to external store, eliminate singleton
    """

    _instance: "OntologyService | None" = None
    _lock = asyncio.Lock()

    def __init__(self):
        self._ontologies: Dict[str, Ontology] = {}
        self._latest_version_id: Optional[str] = None

    @classmethod
    async def get_instance(cls) -> "OntologyService":
        """Get the singleton instance with lazy initialization."""
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    logger.info("Creating new OntologyService singleton instance")
                    cls._instance = cls()
        return cls._instance

    async def get_ontology(self, version_id: str) -> Ontology:
        """
        Get or load a specific ontology version.

        Args:
            version_id: S3 version ID of the ontology to retrieve

        Returns:
            Ontology instance for the specified version

        Raises:
            ValueError: If the specified version does not exist in S3
        """
        if version_id in self._ontologies:
            logger.debug(f"Cache hit for ontology version {version_id}")
            return self._ontologies[version_id]

        logger.info(f"Loading ontology version {version_id} from S3")
        async with self._lock:
            # Double-check after acquiring lock
            if version_id in self._ontologies:
                return self._ontologies[version_id]

            ontology = await download_ontology_rdf(version_id)
            self._ontologies[version_id] = ontology
            logger.info(f"Cached ontology version {version_id}")
            return ontology

    async def get_latest_ontology(self) -> Ontology:
        """
        Get the latest ontology version from S3.

        Returns:
            Latest Ontology instance (downloaded from S3 if not cached)
        """
        async with self._lock:
            ontology = await download_ontology_rdf(None)  # None = latest
            version_id = ontology.s3_version_id

            # Update cache and latest pointer
            self._ontologies[version_id] = ontology
            old_latest = self._latest_version_id
            self._latest_version_id = version_id

            if old_latest != version_id:
                logger.info(f"Latest ontology version: {old_latest} -> {version_id}")
            else:
                logger.debug(f"Latest ontology version unchanged: {version_id}")

            return ontology

    async def refresh(self) -> Ontology:
        """
        Refresh the latest ontology version from S3.

        Returns:
            The refreshed latest Ontology instance
        """
        logger.info("Refreshing latest ontology from S3")
        ontology = await self.get_latest_ontology()
        logger.info(f"Ontology refreshed to version {ontology.version_id}")
        return ontology

    def get_service_info(self) -> dict:
        """Return service information for debugging and health checks."""
        return {
            "instance_id": id(self),
            "cached_versions": list(self._ontologies.keys()),
            "latest_version_id": self._latest_version_id,
            "cache_size": len(self._ontologies),
        }


# Factory function for getting the service instance
async def get_ontology_service() -> OntologyService:
    """Factory function to get the OntologyService instance."""
    return await OntologyService.get_instance()
