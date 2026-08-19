"""FastAPI dependencies shared across routes.

This API plane has no password/token authentication — ``author_email`` is
self-asserted — so these gates are attribution bookkeeping, not security
(recorded at the P3 charter, fork X6): every write is pinned to a registered
user, and annotation surfaces are only touched by users enrolled for them.
"""

from typing import Awaitable, Callable

from fastapi import Depends, HTTPException, Query

from data_etl_app.db_models.user import User, UserRole
from data_etl_app.services.user_service import find_by_email


async def require_registered_user(
    author_email: str = Query(
        description=(
            "Registered user email, e.g. `?author_email=your_email@example.com` "
            "(in Postman, use the `Params` tab). "
            "Please register on `sudokn.com` first."
        ),
    ),
) -> User:
    user = await find_by_email(author_email)
    if not user:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Sorry, no registered user found with email: {author_email}. "
                f"Please register on `sudokn.com` first."
            ),
        )
    return user


def require_role(*roles: UserRole) -> Callable[..., Awaitable[User]]:
    """Dependency factory: the registered user must hold one of ``roles``."""
    allowed = set(roles)

    async def _require_role(
        user: User = Depends(require_registered_user),
    ) -> User:
        if user.role not in allowed:
            raise HTTPException(
                status_code=403,
                detail=(
                    f"User {user.email} has role {user.role.value!r}; this "
                    f"endpoint requires one of "
                    f"{sorted(role.value for role in allowed)}."
                ),
            )
        return user

    return _require_role
