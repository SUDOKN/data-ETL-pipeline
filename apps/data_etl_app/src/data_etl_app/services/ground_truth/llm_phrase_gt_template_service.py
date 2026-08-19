"""Assemble a full GT document template from a Manufacturer's stored results.

The template is everything the annotator's audit starts from: the embedded
metadata copy (deep — the document must be the surviving record, not a shared
reference into ``Manufacturer``, whose one result slot per field is overwritten
each run), the run identity projected from it, the text witness, and every
chunk's stage trees inflated from the pinned catalogs with empty audit slots.

Catalog-version pinning: the stored metadata says which ``catalog_version``
each stage ran with, and inflation only makes sense against that exact catalog.
The deployed catalog files are the only ones this service can load, so any
drift between the two hard-fails with both versions named — the remedy is to
re-run extraction on the current catalogs (or check out the matching ones),
never to inflate against a catalog the run did not use.

The scraped text is the CALLER's to supply: fetching the version pinned by
``manufacturer.scraped_text_file_version_id`` is route/notebook business, and
taking the text as an argument keeps this service pure. The witness fields are
computed from what is passed, so passing the wrong version produces a witness
that provably disagrees with the S3 object it claims to pin.
"""

from __future__ import annotations

import hashlib
from typing import Callable, NamedTuple, Optional

from core.models.extraction_results.concept_extraction_results import (
    ConceptExtractionStats,
)
from core.models.extraction_results.keyword_extraction_results import (
    KeywordExtractionStats,
)
from core.models.extraction_results.llm_phrase_extraction_results import (
    ExtractionNodeMetadata,
)
from core.models.ground_truth.run_identity import ExplicitRunIdentity
from core.models.rule_catalog import (
    STAGE_FREEHAND_GROUNDING,
    STAGE_INITIAL_GROUNDING,
    STAGE_RECURSIVE_GROUNDING,
    STAGE_RELATIONSHIP_SCREENING,
    RuleCatalog,
)
from core.services.ground_truth.catalog_template_inflation import inflate_chunk
from data_etl_app.db_models.llm_phrase_ground_truth import LLMPhraseGroundTruth
from data_etl_app.db_models.manufacturer import Manufacturer
from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum
from data_etl_app.services.prompt_assembly_service import build_rule_catalog_lookup

CatalogLookup = Callable[[str, str], Optional[RuleCatalog]]


class TemplateAssemblyError(ValueError):
    """The stored results cannot become a template with what is deployed."""


def _required_catalog(
    lookup: CatalogLookup, stage: str, field_type: str
) -> RuleCatalog:
    catalog = lookup(stage, field_type)
    if catalog is None:
        raise TemplateAssemblyError(
            f"no deployed catalog for ({stage!r}, {field_type!r})"
        )
    return catalog


def _check_catalog_pin(
    node: ExtractionNodeMetadata, catalog: RuleCatalog, node_name: str
) -> None:
    if node.catalog_version != catalog.catalog_version:
        raise TemplateAssemblyError(
            f"{node_name} ran with catalog_version {node.catalog_version!r} but "
            f"the deployed {catalog.prompt_name} is {catalog.catalog_version!r}. "
            f"Inflating against a catalog the run did not use would mis-slot its "
            f"rules — re-run extraction on the current catalogs, or check out "
            f"the matching ones."
        )


class PinnedCatalogs(NamedTuple):
    """The deployed catalogs a document's run pinned, one slot per stage.

    ``oov`` is the per-phrase tag-map catalog: freehand for keyword fields,
    initial grounding for concept fields (where it aliases ``initial``);
    ``initial``/``recursive`` are None for keyword fields.
    """

    screening: RuleCatalog
    oov: RuleCatalog
    initial: Optional[RuleCatalog]
    recursive: Optional[RuleCatalog]


def resolve_pinned_catalogs(
    metadata,
    field_type: KeywordTypeEnum | ConceptTypeEnum,
    lookup: CatalogLookup,
) -> PinnedCatalogs:
    """Deployed catalogs for every stage of this field, each hard pin-checked
    against the metadata's stored ``catalog_version`` (template assembly and
    batch submission share this — both are meaningless against catalogs the
    run did not use)."""
    screening_catalog = _required_catalog(
        lookup, STAGE_RELATIONSHIP_SCREENING, field_type.value
    )
    _check_catalog_pin(
        metadata.llm_phrase_relationship_screening,
        screening_catalog,
        "llm_phrase_relationship_screening",
    )
    if isinstance(field_type, KeywordTypeEnum):
        oov_catalog = _required_catalog(
            lookup, STAGE_FREEHAND_GROUNDING, field_type.value
        )
        _check_catalog_pin(
            metadata.llm_phrase_freehand_grounding,
            oov_catalog,
            "llm_phrase_freehand_grounding",
        )
        return PinnedCatalogs(screening_catalog, oov_catalog, None, None)
    initial_catalog = _required_catalog(
        lookup, STAGE_INITIAL_GROUNDING, field_type.value
    )
    recursive_catalog = _required_catalog(
        lookup, STAGE_RECURSIVE_GROUNDING, field_type.value
    )
    _check_catalog_pin(
        metadata.llm_phrase_initial_grounding,
        initial_catalog,
        "llm_phrase_initial_grounding",
    )
    _check_catalog_pin(
        metadata.llm_phrase_recursive_grounding,
        recursive_catalog,
        "llm_phrase_recursive_grounding",
    )
    return PinnedCatalogs(
        screening_catalog, initial_catalog, initial_catalog, recursive_catalog
    )


def build_llm_phrase_gt_template(
    manufacturer: Manufacturer,
    field_type: KeywordTypeEnum | ConceptTypeEnum,
    scraped_text: str,
    catalog_lookup: Optional[CatalogLookup] = None,
) -> LLMPhraseGroundTruth:
    """The unsaved audit document for one (manufacturer, field) run."""
    results = getattr(manufacturer, field_type.value)
    if results is None:
        raise TemplateAssemblyError(
            f"{manufacturer.etld1} has no stored results for "
            f"{field_type.value!r} — nothing to audit"
        )
    lookup = catalog_lookup if catalog_lookup is not None else build_rule_catalog_lookup()
    metadata = results.metadata.model_copy(deep=True)

    catalogs = resolve_pinned_catalogs(metadata, field_type, lookup)
    screening_catalog = catalogs.screening
    oov_catalog = catalogs.oov
    initial_catalog = catalogs.initial
    recursive_catalog = catalogs.recursive
    if isinstance(field_type, KeywordTypeEnum):
        # The two results containers name their chunk map differently.
        stats_map: dict[str, KeywordExtractionStats | ConceptExtractionStats] = (
            results.chunk_stats
        )
    else:
        stats_map = results.chunked_extraction_stats

    chunks = {
        chunk_key: inflate_chunk(
            stats,
            screening_catalog=screening_catalog,
            oov_catalog=oov_catalog,
            recursive_catalog=recursive_catalog,
            initial_catalog=initial_catalog,
        )
        for chunk_key, stats in stats_map.items()
    }

    identity = ExplicitRunIdentity.from_metadata(metadata)
    return LLMPhraseGroundTruth(
        mfg_etld1=manufacturer.etld1,
        field_type=field_type,
        scraped_text_file_version_id=manufacturer.scraped_text_file_version_id,
        scraped_text_sha256=hashlib.sha256(
            scraped_text.encode("utf-8")
        ).hexdigest(),
        scraped_text_char_len=len(scraped_text),
        run_identity=identity,
        identity_digest=identity.canonical_digest(),
        metadata=metadata,
        chunks=chunks,
    )
