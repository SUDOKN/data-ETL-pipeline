"""The shared route dependencies: registered-user lookup and role gating (X6).

Direct-call convention (the repo's route-test style): the dependency functions
are invoked as plain async functions with ``find_by_email`` monkeypatched — no
TestClient, no MongoDB.
"""

import pytest
from fastapi import HTTPException

import data_etl_app.api.deps as deps
from data_etl_app.api.deps import require_registered_user, require_role
from data_etl_app.db_models.user import User, UserRole


def _user(role: UserRole) -> User:
    return User(
        firstName="Ada",
        lastName="Annotator",
        email="ada@example.com",
        role=role,
        companyURL=None,
        salt="salt",
        hashedPassword="hashed",
    )


def _patch_found(monkeypatch: pytest.MonkeyPatch, user: User | None) -> None:
    async def fake_find_by_email(email: str) -> User | None:
        return user

    monkeypatch.setattr(deps, "find_by_email", fake_find_by_email)


def test_annotator_role_wire_value_is_pinned():
    assert UserRole.ANNOTATOR.value == "annotator"


@pytest.mark.asyncio
async def test_require_registered_user_returns_the_user(
    monkeypatch: pytest.MonkeyPatch,
):
    user = _user(UserRole.DEFAULT)
    _patch_found(monkeypatch, user)

    assert await require_registered_user(author_email=user.email) is user


@pytest.mark.asyncio
async def test_require_registered_user_404s_on_unknown_email(
    monkeypatch: pytest.MonkeyPatch,
):
    _patch_found(monkeypatch, None)

    with pytest.raises(HTTPException) as exc_info:
        await require_registered_user(author_email="nobody@example.com")

    assert exc_info.value.status_code == 404
    assert "nobody@example.com" in exc_info.value.detail


@pytest.mark.asyncio
@pytest.mark.parametrize("role", [UserRole.ANNOTATOR, UserRole.ADMIN])
async def test_require_role_passes_each_allowed_role(role: UserRole):
    gate = require_role(UserRole.ANNOTATOR, UserRole.ADMIN)
    user = _user(role)

    assert await gate(user=user) is user


@pytest.mark.asyncio
async def test_require_role_403s_on_a_disallowed_role():
    gate = require_role(UserRole.ANNOTATOR, UserRole.ADMIN)
    user = _user(UserRole.DEFAULT)

    with pytest.raises(HTTPException) as exc_info:
        await gate(user=user)

    assert exc_info.value.status_code == 403
    # The detail names both the user's role and what would have been enough.
    assert "'default'" in exc_info.value.detail
    assert "admin" in exc_info.value.detail
    assert "annotator" in exc_info.value.detail


@pytest.mark.asyncio
async def test_require_role_403_is_not_a_registration_problem(
    monkeypatch: pytest.MonkeyPatch,
):
    """A registered user with the wrong role is refused with 403, not 404 —
    the two failures must stay distinguishable to the caller."""
    user = _user(UserRole.EMPLOYEE)
    _patch_found(monkeypatch, user)

    resolved = await require_registered_user(author_email=user.email)
    gate = require_role(UserRole.ANNOTATOR)

    with pytest.raises(HTTPException) as exc_info:
        await gate(user=resolved)

    assert exc_info.value.status_code == 403
