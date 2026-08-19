"""The truth + work-list GETs (P3.7), direct-call convention with fakes."""

import json

import pytest
from beanie import PydanticObjectId
from fastapi import HTTPException

import data_etl_app.api.routes.ground_truth.llm_phrase_ground_truth as route_mod
from data_etl_app.api.routes.ground_truth.llm_phrase_ground_truth import (
    fetch_llm_phrase_gt_truth,
    list_llm_phrase_gt_documents,
)
from data_etl_app.db_models.user import User, UserRole
from data_etl_app.models.types_and_enums import KeywordTypeEnum
from data_etl_app.services.ground_truth.llm_phrase_gt_template_service import (
    build_llm_phrase_gt_template,
)


def _reader() -> User:
    """Truth/list are open to ANY registered user (X6) — a default-role user."""
    return User(
        firstName="Rae",
        lastName="Reader",
        email="rae@example.com",
        role=UserRole.DEFAULT,
        companyURL=None,
        salt="salt",
        hashedPassword="hashed",
    )


@pytest.fixture
def saved_doc(
    make_gt_manufacturer, make_keyword_gt_results, gt_catalog_lookup, gt_scraped_text
):
    mfg = make_gt_manufacturer(equipments=make_keyword_gt_results())
    doc = build_llm_phrase_gt_template(
        mfg, KeywordTypeEnum.equipments, gt_scraped_text, catalog_lookup=gt_catalog_lookup
    )
    doc.id = PydanticObjectId()
    return doc


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch, saved_doc):
    calls: dict = {"find": [], "list": []}

    async def fake_find(mfg_etld1, field_type, identity_digest=None):
        calls["find"].append((mfg_etld1, field_type, identity_digest))
        return saved_doc if mfg_etld1 == "steelcraft.com" else None

    async def fake_list(mfg_etld1=None, field_type=None):
        calls["list"].append((mfg_etld1, field_type))
        return [saved_doc]

    monkeypatch.setattr(route_mod, "find_llm_phrase_gt", fake_find)
    monkeypatch.setattr(route_mod, "list_llm_phrase_gts", fake_list)
    monkeypatch.setattr(
        route_mod, "get_complete_url_with_compatible_protocol", lambda url: url
    )
    monkeypatch.setattr(
        route_mod, "get_normalized_url", lambda url: ("https", url)
    )
    monkeypatch.setattr(
        route_mod, "get_etld1_from_host", lambda host: host
    )
    return calls


@pytest.mark.asyncio
async def test_truth_serves_fold_rollup_and_identity(patched, saved_doc):
    response = await fetch_llm_phrase_gt_truth(
        user=_reader(),
        mfg_url="steelcraft.com",
        field_type="equipments",
        identity_digest=None,
    )

    assert response["identity_digest"] == saved_doc.identity_digest
    assert response["total_phrases"] == 1
    assert response["unreviewed_phrases"] == 1
    assert "cnc mill" in response["truth"]["0:1000"]["phrases"]
    assert response["rule_rollup"] == {}
    json.dumps(response)


@pytest.mark.asyncio
async def test_truth_passes_the_digest_pin_through(patched):
    await fetch_llm_phrase_gt_truth(
        user=_reader(),
        mfg_url="steelcraft.com",
        field_type="equipments",
        identity_digest="abc123",
    )

    assert patched["find"] == [
        ("steelcraft.com", KeywordTypeEnum.equipments, "abc123")
    ]


@pytest.mark.asyncio
async def test_truth_404_points_at_the_template(patched):
    with pytest.raises(HTTPException) as exc_info:
        await fetch_llm_phrase_gt_truth(
            user=_reader(),
            mfg_url="unknown.com",
            field_type="equipments",
            identity_digest=None,
        )
    assert exc_info.value.status_code == 404
    assert "Fetch the template first" in exc_info.value.detail


@pytest.mark.asyncio
async def test_truth_unknown_field_type_is_400(patched):
    with pytest.raises(HTTPException) as exc_info:
        await fetch_llm_phrase_gt_truth(
            user=_reader(),
            mfg_url="steelcraft.com",
            field_type="colors",
            identity_digest=None,
        )
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_list_serves_rows_and_passes_filters(patched, saved_doc):
    response = await list_llm_phrase_gt_documents(
        user=_reader(), mfg_url="steelcraft.com", field_type="equipments"
    )

    assert patched["list"] == [("steelcraft.com", KeywordTypeEnum.equipments)]
    assert response["total"] == 1
    (row,) = response["documents"]
    assert row["document_id"] == str(saved_doc.id)
    assert row["unreviewed_phrases"] == 1
    json.dumps(response)


@pytest.mark.asyncio
async def test_list_without_filters_passes_none(patched):
    response = await list_llm_phrase_gt_documents(
        user=_reader(), mfg_url=None, field_type=None
    )

    assert patched["list"] == [(None, None)]
    assert response["total"] == 1
