import re
from datetime import datetime

from beanie import Document
from pydantic import Field, model_validator

from core.field_types import SubjectUniqueIDType
from core.models.extraction_results.concept_extraction_results import (
    ConceptExtractionMetadata,
)
from core.models.extraction_results.keyword_extraction_results import (
    KeywordExtractionMetadata,
)
from core.models.ground_truth.run_identity import ExplicitRunIdentity
from core.models.ground_truth.stage_blocks import ChunkGT
from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum
from infra.field_types import S3FileVersionIDType
from pure_utils.time_util import get_current_time

# The unique index, declared here so the seeder imports it rather than
# restating it. The digest stands in for the whole identity (PH-11 b): a
# compound index over every identity field would exceed Mongo's 32-field cap,
# and whole-subdocument keys compare field-order-sensitively.
LLM_PHRASE_GT_UNIQUE_INDEX_KEYS: list[tuple[str, int]] = [
    ("mfg_etld1", 1),
    ("field_type", 1),
    ("scraped_text_file_version_id", 1),
    ("identity_digest", 1),
]
LLM_PHRASE_GT_UNIQUE_INDEX_NAME = "llm_phrase_gt_unique_idx"

_CHUNK_KEY_PATTERN = re.compile(r"^\d+:\d+$")


class LLMPhraseGroundTruth(Document):
    """The run-scoped audit document over every phrase stage of one field.

    Self-contained on purpose: ``Manufacturer`` keeps one result slot per
    field, overwritten each run, so this document is the only surviving record
    of the run it audits. The witness fields pin the text the run read; the
    embedded metadata copy is the run's full configuration; ``run_identity``
    is its validated projection; ``identity_digest`` is the identity's
    validated fingerprint, and the unique index keys on it — a run is quite
    literally identified by its identity (PH-11).

    Written whole on submission (settled semantics #5); per-chunk trees live
    in ``chunks`` under the same ``"start:end"`` keys as
    ``chunked_extraction_stats``.
    """

    created_at: datetime = Field(default_factory=lambda: get_current_time())
    updated_at: datetime = Field(default_factory=lambda: get_current_time())

    mfg_etld1: SubjectUniqueIDType
    field_type: KeywordTypeEnum | ConceptTypeEnum
    scraped_text_file_version_id: S3FileVersionIDType
    # Text witness: proves which bytes the run read, even if the S3 version is
    # ever purged.
    scraped_text_sha256: str
    scraped_text_char_len: int

    run_identity: ExplicitRunIdentity
    identity_digest: str
    # The full configuration record the identity projects from — the union
    # discriminates on each family's required grounding nodes.
    metadata: KeywordExtractionMetadata | ConceptExtractionMetadata

    chunks: dict[str, ChunkGT]

    @model_validator(mode="after")
    def check_identity_is_a_projection_of_the_metadata(
        self,
    ) -> "LLMPhraseGroundTruth":
        derived = ExplicitRunIdentity.from_metadata(self.metadata)
        if self.run_identity != derived:
            raise ValueError(
                "run_identity must equal ExplicitRunIdentity.from_metadata("
                "metadata) — it is a projection of the embedded copy, never "
                "an independent claim"
            )
        return self

    @model_validator(mode="after")
    def check_digest_is_a_cache_of_the_identity(self) -> "LLMPhraseGroundTruth":
        expected = self.run_identity.canonical_digest()
        if self.identity_digest != expected:
            raise ValueError(
                "identity_digest is a validated cache of "
                "run_identity.canonical_digest(), never independent"
            )
        return self

    @model_validator(mode="after")
    def check_field_family_matches_metadata_family(self) -> "LLMPhraseGroundTruth":
        keyword_field = isinstance(self.field_type, KeywordTypeEnum)
        keyword_metadata = isinstance(self.metadata, KeywordExtractionMetadata)
        if keyword_field != keyword_metadata:
            raise ValueError(
                f"field_type {self.field_type.value!r} and metadata type "
                f"{type(self.metadata).__name__} belong to different field "
                f"families"
            )
        return self

    @model_validator(mode="after")
    def check_chunk_keys_match_stats_convention(self) -> "LLMPhraseGroundTruth":
        for key in self.chunks:
            if not _CHUNK_KEY_PATTERN.match(key):
                raise ValueError(
                    f"chunk key {key!r} is not a 'start:end' bounds key — the "
                    f"document mirrors chunked_extraction_stats keys exactly"
                )
        return self

    class Settings:
        name = "llm_phrase_ground_truths"


"""
Indexes in MongoDB for LLMPhraseGroundTruth (created by scripts/db_seed_indices.py,
which imports LLM_PHRASE_GT_UNIQUE_INDEX_KEYS above):

db.llm_phrase_ground_truths.createIndex(
  {
    mfg_etld1: 1,
    field_type: 1,
    scraped_text_file_version_id: 1,
    identity_digest: 1,
  },
  {
    name: "llm_phrase_gt_unique_idx",
    unique: true
  }
)
"""
