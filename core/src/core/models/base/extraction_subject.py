from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from pydantic import BaseModel, Field

from core.models.field_types import S3FileVersionIDType
from core.utils.time_util import get_current_time


class AbstractExtractionSubject(BaseModel, ABC):
    """Abstract interface a concrete extraction subject (e.g. Manufacturer) must satisfy.

    Core pipeline/service code depends on this instead of any concrete document type.
    """

    scraped_text_file_version_id: S3FileVersionIDType
    updated_at: datetime = Field(default_factory=lambda: get_current_time())

    @property
    @abstractmethod
    def subject_unique_id(self) -> str:
        """Stable unique identifier for this subject (maps to the concrete key field)."""
        ...

    @abstractmethod
    async def save(self) -> None: ...

    async def record_update(self, updated_at: datetime) -> None:
        self.updated_at = updated_at
        await self.save()


class AbstractDeferredExtractionSubject(BaseModel, ABC):
    """Abstract interface for the deferred-extraction counterpart (e.g. DeferredManufacturer)."""

    @property
    @abstractmethod
    def subject_unique_id(self) -> str:
        """Stable unique identifier for this subject (maps to the concrete key field)."""
        ...

    @abstractmethod
    async def save(self) -> None: ...
