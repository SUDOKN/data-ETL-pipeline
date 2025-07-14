from typing import Optional
from shared.models.db.user import User


async def findByEmail(
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
