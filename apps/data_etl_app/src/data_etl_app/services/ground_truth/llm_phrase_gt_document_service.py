"""Get-or-create and lookup for ``LLMPhraseGroundTruth`` documents.

Get-or-create at template GET (P3-1): building the template from the
``Manufacturer``'s current slot and SAVING it snapshots the run before the
one-slot-per-field record is overwritten by a re-run. The unique index
(subject, field_type, text version, identity_digest) is what makes creation
idempotent — and since the digest is computable from the STORED metadata
alone, an existing document is found and returned without fetching the S3
text or inflating a single catalog. A concurrent GET that loses the insert
race re-fetches and returns the winner's document.

Beanie and S3 calls are injectable (the ``catalog_lookup`` taste) so the
decision logic tests offline; the defaults are the real thing. The default
finders are thin I/O one-liners — exercised by routes/integration, not unit
tests.
"""

from __future__ import annotations

from typing import Awaitable, Callable, Optional

from beanie import PydanticObjectId
from pymongo.errors import DuplicateKeyError

from core.models.ground_truth.run_identity import ExplicitRunIdentity
from data_etl_app.db_models.llm_phrase_ground_truth import LLMPhraseGroundTruth
from data_etl_app.db_models.manufacturer import Manufacturer
from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum
from data_etl_app.services.ground_truth.llm_phrase_gt_template_service import (
    CatalogLookup,
    TemplateAssemblyError,
    build_llm_phrase_gt_template,
)
from infra.utils.aws.s3.scraped_text_file_util import (
    download_scraped_text_from_s3_by_subject_unique_id,
)

# (subject_unique_id=..., version_id=...) -> (text, version_id)
TextFetcher = Callable[..., Awaitable[tuple[str, str]]]
# (mfg_etld1, field_type, scraped_text_file_version_id, identity_digest)
Finder = Callable[..., Awaitable[Optional[LLMPhraseGroundTruth]]]
Inserter = Callable[[LLMPhraseGroundTruth], Awaitable[object]]


async def get_llm_phrase_gt_by_id(
    document_id: PydanticObjectId,
) -> Optional[LLMPhraseGroundTruth]:
    """The document a route address names — routes resolve ids, this fetches."""
    return await LLMPhraseGroundTruth.get(document_id)


async def replace_llm_phrase_gt(doc: LLMPhraseGroundTruth) -> None:
    """Whole-document write (settled #5) — the batch route persists the
    applied copy through this seam."""
    await doc.replace()


async def find_llm_phrase_gt_by_identity(
    mfg_etld1: str,
    field_type: KeywordTypeEnum | ConceptTypeEnum,
    scraped_text_file_version_id: str,
    identity_digest: str,
) -> Optional[LLMPhraseGroundTruth]:
    """Exact unique-key lookup — the run's one document, or None."""
    return await LLMPhraseGroundTruth.find_one(
        LLMPhraseGroundTruth.mfg_etld1 == mfg_etld1,
        LLMPhraseGroundTruth.field_type == field_type,
        LLMPhraseGroundTruth.scraped_text_file_version_id
        == scraped_text_file_version_id,
        LLMPhraseGroundTruth.identity_digest == identity_digest,
    )


async def find_llm_phrase_gt(
    mfg_etld1: str,
    field_type: KeywordTypeEnum | ConceptTypeEnum,
    identity_digest: Optional[str] = None,
) -> Optional[LLMPhraseGroundTruth]:
    """By digest when given, else the latest-created for (subject, field)."""
    query = LLMPhraseGroundTruth.find(
        LLMPhraseGroundTruth.mfg_etld1 == mfg_etld1,
        LLMPhraseGroundTruth.field_type == field_type,
    )
    if identity_digest is not None:
        query = query.find(
            LLMPhraseGroundTruth.identity_digest == identity_digest
        )
    return await query.sort("-created_at").first_or_none()


async def does_an_llm_phrase_gt_exist_with_scraped_file_version(
    scraped_text_file_version_id: str,
) -> bool:
    """Whether any phrase-GT document pins this S3 text version — the
    scraped-file deletability guard's question (the witness proves bytes, but
    the phrase detail plane still needs to FETCH them)."""
    return (
        await LLMPhraseGroundTruth.find_one(
            LLMPhraseGroundTruth.scraped_text_file_version_id
            == scraped_text_file_version_id
        )
        is not None
    )


async def list_llm_phrase_gts(
    mfg_etld1: Optional[str] = None,
    field_type: Optional[KeywordTypeEnum | ConceptTypeEnum] = None,
) -> list[LLMPhraseGroundTruth]:
    """The work-list's rows: newest first, optionally filtered."""
    conditions = []
    if mfg_etld1 is not None:
        conditions.append(LLMPhraseGroundTruth.mfg_etld1 == mfg_etld1)
    if field_type is not None:
        conditions.append(LLMPhraseGroundTruth.field_type == field_type)
    return (
        await LLMPhraseGroundTruth.find(*conditions)
        .sort("-created_at")
        .to_list()
    )


async def _insert(doc: LLMPhraseGroundTruth) -> object:
    return await doc.insert()


async def get_or_create_llm_phrase_gt(
    manufacturer: Manufacturer,
    field_type: KeywordTypeEnum | ConceptTypeEnum,
    *,
    catalog_lookup: Optional[CatalogLookup] = None,
    fetch_text: TextFetcher = download_scraped_text_from_s3_by_subject_unique_id,
    find_existing: Finder = find_llm_phrase_gt_by_identity,
    insert: Inserter = _insert,
) -> tuple[LLMPhraseGroundTruth, bool]:
    """The saved audit document for the Manufacturer's current run.

    Returns ``(document, created)``: created from a fresh template on first
    read, found by its identity afterwards.
    """
    results = getattr(manufacturer, field_type.value)
    if results is None:
        raise TemplateAssemblyError(
            f"{manufacturer.etld1} has no stored results for "
            f"{field_type.value!r} — nothing to audit"
        )
    digest = ExplicitRunIdentity.from_metadata(results.metadata).canonical_digest()

    existing = await find_existing(
        manufacturer.etld1,
        field_type,
        manufacturer.scraped_text_file_version_id,
        digest,
    )
    if existing is not None:
        return existing, False

    scraped_text, _version = await fetch_text(
        subject_unique_id=manufacturer.etld1,
        version_id=manufacturer.scraped_text_file_version_id,
    )
    template = build_llm_phrase_gt_template(
        manufacturer, field_type, scraped_text, catalog_lookup=catalog_lookup
    )
    try:
        await insert(template)
    except DuplicateKeyError:
        # Lost a concurrent-create race; the winner's document is the record.
        winner = await find_existing(
            manufacturer.etld1,
            field_type,
            manufacturer.scraped_text_file_version_id,
            digest,
        )
        if winner is None:
            raise
        return winner, False
    return template, True
