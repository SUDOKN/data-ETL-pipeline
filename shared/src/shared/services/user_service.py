from typing import Optional
from shared.models.db.user import User, UserRole


async def find_by_email(
    email: str,
) -> Optional[User]:
    """
    Find a user by their email address.

    Args:
        email (str): The email address of the user to find.

    Returns:
        Optional[User]: The user object if found, otherwise None.
    """
    return await User.find_one({"email": email})


async def is_user_MEP(
    email: str,
) -> bool:
    """
    Check if a user with the given email has the MEP role.

    Args:
        email (str): The email address of the user to check.

    Returns:
        bool: True if the user has the MEP role, otherwise False.
    """
    user = await find_by_email(email)
    return user is not None and user.role == UserRole.MEP
