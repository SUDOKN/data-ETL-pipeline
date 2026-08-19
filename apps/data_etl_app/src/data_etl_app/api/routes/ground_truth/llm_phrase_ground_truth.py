"""Routes for the LLM phrase ground-truth instrument (P3).

Template GET: get-or-create the run's audit document (P3-1 — saving at first
read snapshots the run before the ``Manufacturer``'s one-slot-per-field record
is overwritten) and serve the BROWSE view (P3-4a): every phrase with only the
relationship stage piggybacked. ``?full=true`` returns the whole saved
document. Role-gated to annotator/admin (X6 — attribution bookkeeping, not
security). No scrape-queue push here: this instrument audits an existing run.
"""

from typing import Optional

from beanie import PydanticObjectId
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from core.services.ground_truth.audit_submission_validation import OntologyChildren
from core.services.ground_truth.gt_fold import compute_document_truth
from data_etl_app.api.deps import require_registered_user, require_role
from data_etl_app.db_models.user import User, UserRole
from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum
from data_etl_app.services.ground_truth.llm_phrase_gt_document_service import (
    find_llm_phrase_gt,
    get_llm_phrase_gt_by_id,
    get_or_create_llm_phrase_gt,
    list_llm_phrase_gts,
    replace_llm_phrase_gt,
)
from data_etl_app.services.ground_truth.llm_phrase_gt_submission_service import (
    BatchItemError,
    OovGroundingItem,
    SubmissionItem,
    apply_submission_batch,
    build_ontology_children,
)
from data_etl_app.services.ground_truth.llm_phrase_gt_template_service import (
    TemplateAssemblyError,
)
from data_etl_app.services.ground_truth.llm_phrase_gt_view_service import (
    TextWitnessError,
    build_document_list_row,
    build_document_truth_view,
    build_phrase_detail_view,
    build_template_browse_view,
    count_unreviewed,
    find_next_unreviewed,
    witnessed_chunk_text,
)
from data_etl_app.services.manufacturer_service import find_manufacturer_by_url
from infra.utils.aws.s3.scraped_text_file_util import (
    download_scraped_text_from_s3_by_subject_unique_id,
)
from pure_utils.time_util import get_current_time
from pure_utils.url_util import (
    get_complete_url_with_compatible_protocol,
    get_etld1_from_host,
    get_normalized_url,
)

router = APIRouter()

FIELD_TYPE_VALUES = [e.value for e in KeywordTypeEnum] + [
    e.value for e in ConceptTypeEnum
]


def parse_field_type(value: str) -> KeywordTypeEnum | ConceptTypeEnum:
    try:
        return KeywordTypeEnum(value)
    except ValueError:
        try:
            return ConceptTypeEnum(value)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unknown field_type: {value!r}. "
                    f"One of {FIELD_TYPE_VALUES}."
                ),
            )


def normalized_mfg_url_or_400(mfg_url: str) -> str:
    try:
        _, normalized = get_normalized_url(
            get_complete_url_with_compatible_protocol(mfg_url)
        )
    except ValueError as error:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Invalid URL: '{mfg_url}' has no valid hostname. "
                f"Error: {error}"
            ),
        )
    return normalized


@router.get("/ground_truth/llm-phrase/template", response_class=JSONResponse)
async def fetch_llm_phrase_gt_template(
    user: User = Depends(require_role(UserRole.ANNOTATOR, UserRole.ADMIN)),
    mfg_url: str = Query(description="Manufacturer URL."),
    field_type: str = Query(description=f"One of {FIELD_TYPE_VALUES}."),
    full: bool = Query(
        default=False,
        description=(
            "Return the whole saved document instead of the browse view "
            "(debug/export escape hatch)."
        ),
    ),
):
    parsed_field = parse_field_type(field_type)
    normalized_url = normalized_mfg_url_or_400(mfg_url)

    manufacturer = await find_manufacturer_by_url(normalized_url)
    if not manufacturer:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No manufacturer found for URL: {mfg_url}. This instrument "
                f"audits an existing extraction run — run extraction first; "
                f"nothing has been queued for scraping."
            ),
        )
    if getattr(manufacturer, parsed_field.value) is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No stored {parsed_field.value!r} results for "
                f"{manufacturer.etld1} — nothing to audit."
            ),
        )

    try:
        doc, created = await get_or_create_llm_phrase_gt(manufacturer, parsed_field)
    except TemplateAssemblyError as error:
        # The stored run and the deployed catalogs disagree — a conflict of
        # state (the remedy is a re-run or matching catalogs), not a missing
        # resource.
        raise HTTPException(status_code=409, detail=str(error))

    if full:
        response = doc.model_dump(mode="json")
        response["id"] = str(doc.id) if doc.id is not None else None
        response["created"] = created
        return response
    return build_template_browse_view(doc, created).model_dump(mode="json")


@router.get("/ground_truth/llm-phrase/phrase", response_class=JSONResponse)
async def fetch_llm_phrase_gt_phrase(
    user: User = Depends(require_role(UserRole.ANNOTATOR, UserRole.ADMIN)),
    document_id: str = Query(
        description="GT document id, from the template GET response."
    ),
    chunk_key: Optional[str] = Query(
        default=None,
        description="'start:end' chunk key — together with `phrase`.",
    ),
    phrase: Optional[str] = Query(
        default=None,
        description="The extracted phrase to open — together with `chunk_key`.",
    ),
    next_unreviewed: bool = Query(
        default=False,
        description=(
            "Serve the first phrase with no audits anywhere in its slice "
            "(chunk order, then stored order) instead of an explicit address."
        ),
    ),
):
    explicit = chunk_key is not None or phrase is not None
    if next_unreviewed and explicit:
        raise HTTPException(
            status_code=400,
            detail=(
                "Address a phrase with chunk_key AND phrase, or pass "
                "next_unreviewed=true — not both."
            ),
        )
    if not next_unreviewed and (chunk_key is None or phrase is None):
        raise HTTPException(
            status_code=400,
            detail=(
                "Address a phrase with chunk_key AND phrase together, or "
                "pass next_unreviewed=true."
            ),
        )

    try:
        object_id = PydanticObjectId(document_id)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"{document_id!r} is not a valid document id.",
        )
    doc = await get_llm_phrase_gt_by_id(object_id)
    if doc is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No ground-truth document with id {document_id}. Fetch the "
                f"template first — it creates the document."
            ),
        )

    if next_unreviewed:
        address = find_next_unreviewed(doc)
        if address is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    "Every phrase in this document has been reviewed — "
                    "nothing unreviewed remains."
                ),
            )
        chunk_key, phrase = address
    else:
        assert chunk_key is not None and phrase is not None
        if chunk_key not in doc.chunks:
            available = sorted(
                doc.chunks, key=lambda key: int(key.split(":")[0])
            )
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No chunk {chunk_key!r} in this document. "
                    f"Available chunk keys: {available}."
                ),
            )
        if phrase not in doc.chunks[chunk_key].extracted_phrases:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Phrase {phrase!r} was not extracted in chunk "
                    f"{chunk_key!r} of this run. A phrase the run missed is "
                    f"asserted through a missed-phrase submission, not "
                    f"opened here."
                ),
            )

    full_text, _version = await download_scraped_text_from_s3_by_subject_unique_id(
        subject_unique_id=doc.mfg_etld1,
        version_id=doc.scraped_text_file_version_id,
    )
    try:
        chunk_text = witnessed_chunk_text(doc, chunk_key, full_text)
    except TextWitnessError as error:
        # The fetched bytes are not the ones the run read — serving a slice
        # of the wrong text would silently mislead the annotator.
        raise HTTPException(status_code=409, detail=str(error))

    return build_phrase_detail_view(doc, chunk_key, phrase, chunk_text).model_dump(
        mode="json"
    )


class LLMPhraseGTSubmissionRequest(BaseModel):
    """One sitting's save (P3-2): every item validated, one atomic write."""

    model_config = ConfigDict(extra="forbid")

    document_id: str
    items: list[SubmissionItem] = Field(min_length=1)
    return_full: bool = False


def _item_summary(
    index: int,
    item,  # SubmissionItem member
    classify: Optional[OntologyChildren],
) -> dict:
    summary = item.model_dump(
        mode="json", exclude={"audit", "derivation", "entry", "path"}
    )
    summary["index"] = index
    if item.surface == "missed_phrase":
        summary["phrase"] = item.entry.phrase
    if classify is not None and isinstance(item, OovGroundingItem):
        # Verdict 6d echo: a reset's replacement tag classifies as an
        # ontology concept or an OOV assertion — a typo'd concept shows up
        # here as out_of_vocab instead of silently persisting.
        summary["tag_classification"] = (
            "in_vocab"
            if classify(item.derivation.tag) is not None
            else "out_of_vocab"
        )
    return summary


@router.post("/ground_truth/llm-phrase/submissions", response_class=JSONResponse)
async def submit_llm_phrase_gt_batch(
    request: LLMPhraseGTSubmissionRequest,
    user: User = Depends(require_role(UserRole.ANNOTATOR, UserRole.ADMIN)),
):
    """Atomic batch submission: all items apply, or nothing persists.

    The batch's author is the gated user — every item's inner authorship is
    validated against it (one submission, one author).
    """
    try:
        object_id = PydanticObjectId(request.document_id)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"{request.document_id!r} is not a valid document id.",
        )
    doc = await get_llm_phrase_gt_by_id(object_id)
    if doc is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No ground-truth document with id {request.document_id}. "
                f"Fetch the template first — it creates the document."
            ),
        )

    full_text: Optional[str] = None
    if any(item.surface == "missed_phrase" for item in request.items):
        full_text, _version = await download_scraped_text_from_s3_by_subject_unique_id(
            subject_unique_id=doc.mfg_etld1,
            version_id=doc.scraped_text_file_version_id,
        )

    ontology_children: Optional[OntologyChildren] = None
    if isinstance(doc.field_type, ConceptTypeEnum) and any(
        item.surface in ("descent_divergence", "oov_grounding_derivation")
        for item in request.items
    ):
        ontology_children = await build_ontology_children(
            doc.metadata.ontology_version_id, doc.field_type
        )

    try:
        updated = apply_submission_batch(
            doc,
            request.items,
            author_email=user.email,
            ontology_children=ontology_children,
            full_text=full_text,
        )
    except BatchItemError as error:
        raise HTTPException(
            status_code=400,
            detail={
                "item_index": error.index,
                "surface": error.surface,
                "reason": error.reason,
            },
        )
    except (TemplateAssemblyError, TextWitnessError) as error:
        # Stored state and the deployed catalogs / fetched bytes disagree —
        # a conflict, not a bad request.
        raise HTTPException(status_code=409, detail=str(error))

    updated.updated_at = get_current_time()
    await replace_llm_phrase_gt(updated)

    response = {
        "document_id": str(updated.id) if updated.id is not None else None,
        "applied_items": [
            _item_summary(index, item, ontology_children)
            for index, item in enumerate(request.items)
        ],
        "unreviewed_remaining": count_unreviewed(updated),
        "updated_at": updated.updated_at.isoformat(),
        "truth": {
            chunk_key: chunk_truth.model_dump(mode="json")
            for chunk_key, chunk_truth in compute_document_truth(
                updated.chunks
            ).items()
        },
    }
    if request.return_full:
        response["document"] = updated.model_dump(mode="json")
    return response


def mfg_etld1_or_400(mfg_url: str) -> str:
    """URL → the GT documents' subject key. No Manufacturer load: the GT
    document is the surviving record and stays readable even after the
    Manufacturer's one-slot-per-field record moves on."""
    return get_etld1_from_host(normalized_mfg_url_or_400(mfg_url))


@router.get("/ground_truth/llm-phrase/truth", response_class=JSONResponse)
async def fetch_llm_phrase_gt_truth(
    user: User = Depends(require_registered_user),
    mfg_url: str = Query(description="Manufacturer URL."),
    field_type: str = Query(description=f"One of {FIELD_TYPE_VALUES}."),
    identity_digest: Optional[str] = Query(
        default=None,
        description=(
            "Pin one run's document; defaults to the latest-created for the "
            "(manufacturer, field)."
        ),
    ),
):
    """The per-document computed truth: the fold + per-rule agreement rollup.

    Open to any registered user (X6) — reading collected truth is not an
    annotation surface.
    """
    parsed_field = parse_field_type(field_type)
    etld1 = mfg_etld1_or_400(mfg_url)

    doc = await find_llm_phrase_gt(etld1, parsed_field, identity_digest)
    if doc is None:
        pinned = f", digest {identity_digest}" if identity_digest else ""
        raise HTTPException(
            status_code=404,
            detail=(
                f"No ground-truth document for ({etld1}, "
                f"{parsed_field.value}{pinned}). Fetch the template first — "
                f"it creates the document."
            ),
        )
    return build_document_truth_view(doc).model_dump(mode="json")


@router.get("/ground_truth/llm-phrase/list", response_class=JSONResponse)
async def list_llm_phrase_gt_documents(
    user: User = Depends(require_registered_user),
    mfg_url: Optional[str] = Query(
        default=None, description="Filter by manufacturer URL."
    ),
    field_type: Optional[str] = Query(
        default=None, description=f"Filter by field. One of {FIELD_TYPE_VALUES}."
    ),
):
    """The work-list: which documents exist, newest first, with per-document
    reviewed counts. Open to any registered user (X6)."""
    parsed_field = (
        parse_field_type(field_type) if field_type is not None else None
    )
    etld1 = mfg_etld1_or_400(mfg_url) if mfg_url is not None else None

    docs = await list_llm_phrase_gts(mfg_etld1=etld1, field_type=parsed_field)
    return {
        "total": len(docs),
        "documents": [
            build_document_list_row(doc).model_dump(mode="json") for doc in docs
        ],
    }
