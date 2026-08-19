"""The template GET route (P3.3), direct-call convention with fakes.

The role gate has its own tests (test_deps); here the handler is called with
a user already resolved, and the manufacturer lookup / get-or-create /
URL-normalization collaborators are monkeypatched module attributes.
"""

import json
from datetime import datetime

import pytest
from fastapi import HTTPException

import data_etl_app.api.routes.ground_truth.llm_phrase_ground_truth as route_mod
from data_etl_app.api.routes.ground_truth.llm_phrase_ground_truth import (
    fetch_llm_phrase_gt_template,
    parse_field_type,
)
from data_etl_app.db_models.user import User, UserRole
from data_etl_app.models.types_and_enums import ConceptTypeEnum, KeywordTypeEnum
from data_etl_app.services.ground_truth.llm_phrase_gt_template_service import (
    TemplateAssemblyError,
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
def keyword_doc_and_mfg(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    mfg = make_gt_manufacturer(equipments=make_keyword_gt_results())
    doc = build_llm_phrase_gt_template(
        mfg, KeywordTypeEnum.equipments, gt_scraped_text, catalog_lookup=gt_catalog_lookup
    )
    return doc, mfg


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch, keyword_doc_and_mfg):
    """Happy-path collaborators; individual tests override what they need."""
    doc, mfg = keyword_doc_and_mfg

    monkeypatch.setattr(
        route_mod,
        "get_complete_url_with_compatible_protocol",
        lambda url: url,
    )
    monkeypatch.setattr(
        route_mod, "get_normalized_url", lambda url: ("etld1", url)
    )

    async def fake_find(url: str):
        return mfg if url == "steelcraft.com" else None

    async def fake_get_or_create(manufacturer, field_type):
        assert manufacturer is mfg
        return doc, False

    monkeypatch.setattr(route_mod, "find_manufacturer_by_url", fake_find)
    monkeypatch.setattr(
        route_mod, "get_or_create_llm_phrase_gt", fake_get_or_create
    )
    return doc, mfg


def test_field_type_parses_across_both_enums():
    assert parse_field_type("equipments") is KeywordTypeEnum.equipments
    assert parse_field_type("industries") is ConceptTypeEnum.industries
    with pytest.raises(HTTPException) as exc_info:
        parse_field_type("colors")
    assert exc_info.value.status_code == 400
    assert "equipments" in exc_info.value.detail
    assert "industries" in exc_info.value.detail


@pytest.mark.asyncio
async def test_browse_response_is_the_default(patched):
    response = await fetch_llm_phrase_gt_template(
        user=_annotator(),
        mfg_url="steelcraft.com",
        field_type="equipments",
        full=False,
    )

    assert response["created"] is False
    assert response["field_type"] == "equipments"
    (row,) = response["chunks"]["0:1000"]["extracted_phrases"]
    assert row["phrase"] == "cnc mill"
    assert row["llm_relationship_text"] == "a machine they run"
    assert row["reviewed"] is False
    assert "metadata" not in response  # browse, not the full document
    json.dumps(response)  # JSON-serializable as returned


@pytest.mark.asyncio
async def test_full_true_returns_the_whole_saved_document(patched):
    doc, _mfg = patched

    response = await fetch_llm_phrase_gt_template(
        user=_annotator(),
        mfg_url="steelcraft.com",
        field_type="equipments",
        full=True,
    )

    assert response["created"] is False
    assert response["identity_digest"] == doc.identity_digest
    assert "metadata" in response
    assert "llm_screening" in response["chunks"]["0:1000"]["extracted_phrases"][
        "cnc mill"
    ]
    json.dumps(response)  # ObjectId/datetime/sets all serialized


@pytest.mark.asyncio
async def test_unknown_manufacturer_404s_without_queueing(patched):
    with pytest.raises(HTTPException) as exc_info:
        await fetch_llm_phrase_gt_template(
            user=_annotator(),
            mfg_url="unknown.com",
            field_type="equipments",
            full=False,
        )
    assert exc_info.value.status_code == 404
    assert "audits an existing extraction run" in exc_info.value.detail


@pytest.mark.asyncio
async def test_missing_field_results_404(patched, monkeypatch):
    _doc, mfg = patched
    mfg.equipments = None

    with pytest.raises(HTTPException) as exc_info:
        await fetch_llm_phrase_gt_template(
            user=_annotator(),
            mfg_url="steelcraft.com",
            field_type="equipments",
            full=False,
        )
    assert exc_info.value.status_code == 404
    assert "nothing to audit" in exc_info.value.detail


@pytest.mark.asyncio
async def test_template_assembly_conflict_is_409(patched, monkeypatch):
    async def drifted(manufacturer, field_type):
        raise TemplateAssemblyError("ran with catalog_version 'old.1' ...")

    monkeypatch.setattr(route_mod, "get_or_create_llm_phrase_gt", drifted)

    with pytest.raises(HTTPException) as exc_info:
        await fetch_llm_phrase_gt_template(
            user=_annotator(),
            mfg_url="steelcraft.com",
            field_type="equipments",
            full=False,
        )
    assert exc_info.value.status_code == 409
    assert "old.1" in exc_info.value.detail


@pytest.mark.asyncio
async def test_invalid_url_is_400(patched, monkeypatch):
    def raises(url: str):
        raise ValueError("no hostname")

    monkeypatch.setattr(route_mod, "get_normalized_url", raises)

    with pytest.raises(HTTPException) as exc_info:
        await fetch_llm_phrase_gt_template(
            user=_annotator(),
            mfg_url="???",
            field_type="equipments",
            full=False,
        )
    assert exc_info.value.status_code == 400
    assert "no valid hostname" in exc_info.value.detail
