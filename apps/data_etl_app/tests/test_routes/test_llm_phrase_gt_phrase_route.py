"""The phrase detail GET (P3.4), direct-call convention with fakes."""

import json
from datetime import datetime

import pytest
from beanie import PydanticObjectId
from fastapi import HTTPException

import data_etl_app.api.routes.ground_truth.llm_phrase_ground_truth as route_mod
from core.models.ground_truth.audits import AuditVerdict, TextFieldAudit
from data_etl_app.api.routes.ground_truth.llm_phrase_ground_truth import (
    fetch_llm_phrase_gt_phrase,
)
from data_etl_app.db_models.user import User, UserRole
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.services.ground_truth.llm_phrase_gt_template_service import (
    build_llm_phrase_gt_template,
)


def _annotator() -> User:
    return User(
        firstName="Ada",
        lastName="Annotator",
        email="ada@example.com",
        role=UserRole.ANNOTATOR,
        companyURL=None,
        salt="salt",
        hashedPassword="hashed",
    )


@pytest.fixture
def saved_doc(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    """A saved-shaped document: id assigned, chunk key within the text."""
    mfg = make_gt_manufacturer(equipments=make_keyword_gt_results())
    doc = build_llm_phrase_gt_template(
        mfg, KeywordTypeEnum.equipments, gt_scraped_text, catalog_lookup=gt_catalog_lookup
    )
    doc.chunks[f"0:{len(gt_scraped_text)}"] = doc.chunks.pop("0:1000")
    doc.id = PydanticObjectId()
    return doc


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch, saved_doc, gt_scraped_text):
    async def fake_get_by_id(object_id):
        return saved_doc if object_id == saved_doc.id else None

    async def fake_download(subject_unique_id: str, version_id: str):
        assert subject_unique_id == saved_doc.mfg_etld1
        assert version_id == saved_doc.scraped_text_file_version_id
        return gt_scraped_text, version_id

    monkeypatch.setattr(route_mod, "get_llm_phrase_gt_by_id", fake_get_by_id)
    monkeypatch.setattr(
        route_mod,
        "download_scraped_text_from_s3_by_subject_unique_id",
        fake_download,
    )
    return saved_doc


@pytest.mark.asyncio
async def test_explicit_fetch_serves_trail_effective_and_chunk_text(
    patched, gt_scraped_text
):
    doc = patched
    chunk_key = f"0:{len(gt_scraped_text)}"

    response = await fetch_llm_phrase_gt_phrase(
        user=_annotator(),
        document_id=str(doc.id),
        chunk_key=chunk_key,
        phrase="cnc mill",
        next_unreviewed=False,
    )

    assert response["phrase"] == "cnc mill"
    assert response["chunk_text"] == gt_scraped_text
    assert response["reviewed"] is False
    assert response["unreviewed_remaining"] == 1
    assert response["trail"]["llm_screening"]["llm_result"]["passed"] is True
    assert response["effective"]["relationship"]["text"] == "a machine they run"
    json.dumps(response)


@pytest.mark.asyncio
async def test_next_unreviewed_serves_the_cursor_phrase(patched):
    doc = patched

    response = await fetch_llm_phrase_gt_phrase(
        user=_annotator(),
        document_id=str(doc.id),
        chunk_key=None,
        phrase=None,
        next_unreviewed=True,
    )

    assert response["phrase"] == "cnc mill"


@pytest.mark.asyncio
async def test_next_unreviewed_404s_when_everything_is_reviewed(patched):
    doc = patched
    for chunk in doc.chunks.values():
        for phrase_gt in chunk.extracted_phrases.values():
            phrase_gt.llm_relationship.audits.append(
                TextFieldAudit(
                    type=AuditVerdict.AGREE,
                    corrected_text=None,
                    author_email="ada@example.com",
                    at=datetime(2026, 8, 16),
                    source="api_survey",
                )
            )

    with pytest.raises(HTTPException) as exc_info:
        await fetch_llm_phrase_gt_phrase(
            user=_annotator(),
            document_id=str(doc.id),
            chunk_key=None,
            phrase=None,
            next_unreviewed=True,
        )
    assert exc_info.value.status_code == 404
    assert "nothing unreviewed remains" in exc_info.value.detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chunk_key,phrase,next_unreviewed",
    [
        ("0:60", "cnc mill", True),  # both addressing modes
        ("0:60", None, False),  # half an explicit address
        (None, None, False),  # no address at all
    ],
)
async def test_ambiguous_addressing_is_400(patched, chunk_key, phrase, next_unreviewed):
    doc = patched

    with pytest.raises(HTTPException) as exc_info:
        await fetch_llm_phrase_gt_phrase(
            user=_annotator(),
            document_id=str(doc.id),
            chunk_key=chunk_key,
            phrase=phrase,
            next_unreviewed=next_unreviewed,
        )
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_invalid_and_unknown_document_ids(patched):
    with pytest.raises(HTTPException) as exc_info:
        await fetch_llm_phrase_gt_phrase(
            user=_annotator(),
            document_id="not-an-id",
            chunk_key=None,
            phrase=None,
            next_unreviewed=True,
        )
    assert exc_info.value.status_code == 400

    with pytest.raises(HTTPException) as exc_info:
        await fetch_llm_phrase_gt_phrase(
            user=_annotator(),
            document_id=str(PydanticObjectId()),
            chunk_key=None,
            phrase=None,
            next_unreviewed=True,
        )
    assert exc_info.value.status_code == 404
    assert "Fetch the template first" in exc_info.value.detail


@pytest.mark.asyncio
async def test_unknown_chunk_and_phrase_404s(patched, gt_scraped_text):
    doc = patched

    with pytest.raises(HTTPException) as exc_info:
        await fetch_llm_phrase_gt_phrase(
            user=_annotator(),
            document_id=str(doc.id),
            chunk_key="9:99",
            phrase="cnc mill",
            next_unreviewed=False,
        )
    assert exc_info.value.status_code == 404
    assert f"0:{len(gt_scraped_text)}" in exc_info.value.detail

    with pytest.raises(HTTPException) as exc_info:
        await fetch_llm_phrase_gt_phrase(
            user=_annotator(),
            document_id=str(doc.id),
            chunk_key=f"0:{len(gt_scraped_text)}",
            phrase="unheard-of phrase",
            next_unreviewed=False,
        )
    assert exc_info.value.status_code == 404
    assert "missed-phrase submission" in exc_info.value.detail


@pytest.mark.asyncio
async def test_wrong_s3_bytes_are_a_409_not_a_wrong_excerpt(
    patched, monkeypatch, gt_scraped_text
):
    doc = patched

    async def tampered_download(subject_unique_id: str, version_id: str):
        return gt_scraped_text + " tampered", version_id

    monkeypatch.setattr(
        route_mod,
        "download_scraped_text_from_s3_by_subject_unique_id",
        tampered_download,
    )

    with pytest.raises(HTTPException) as exc_info:
        await fetch_llm_phrase_gt_phrase(
            user=_annotator(),
            document_id=str(doc.id),
            chunk_key=f"0:{len(gt_scraped_text)}",
            phrase="cnc mill",
            next_unreviewed=False,
        )
    assert exc_info.value.status_code == 409
    assert "witnessed" in exc_info.value.detail
