from typing import Optional
from shared.models.db.user import User, UserRole
from shared.models.new_user import NewUser

from shared.utils.password_util import hash_password, verify_password


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


async def addNewUser(
    data: NewUser,
) -> User:
    """
    Add a new user to the database.

    Args:
        data (NewUser): The new user data to be added.

    Returns:
        User: The created user object.
    """
    salt, hashed_password = hash_password(data.password, None)
    user = User(
        firstName=data.firstName,
        lastName=data.lastName,
        email=data.email,
        companyURL=data.companyURL,
        salt=salt,
        hashedPassword=hashed_password,
    )
    await user.create()
    return user


async def getRegisteredUser(
    email: str,
    password: str,
) -> User:
    """
    Get a registered user by email and password.

    Args:
        email (str): The email address of the user.
        password (str): The password of the user.

    Returns:
        User: The user object if found and password is valid.

    Raises:
        ValueError: If the user is not found or password is invalid.
    """
    user = await findByEmail(email)
    if not user:
        raise ValueError("User not found")

    if not verify_password(password, user.hashedPassword, user.salt):
        raise ValueError("Invalid password")

    return user


async def markUserAsEmployee(user_email: str, companyURL: str) -> None:
    """
    Mark a user as an employee.

    Args:
        companyURL (str): The company URL of the user to mark as an employee.
        Assumes the company URL is validated beforehand.

    Returns:
        None
    """
    user = await User.find_one({"email": user_email})
    if not user:
        raise ValueError("User not found")

    user.companyURL = companyURL
    user.role = UserRole.EMPLOYEE
    await user.save()


async def markUserAsMEP(user_email: str) -> None:
    """
    Mark a user as an authorized representative of MEP centers in USA.

    Args:
        user_email (str): The email address of the user to mark as MEP.

    Returns:
        None
    """
    user = await User.find_one({"email": user_email})
    if not user:
        raise ValueError("User not found")

    user.role = UserRole.MEP
    await user.save()


async def saveResetPasswordToken(
    email: str,
    token: str,
) -> None:
    """
    Save a reset password token for a user.

    Args:
        email (str): The email address of the user.
        token (str): The reset password token to save.

    Returns:
        None
    """
    user = await findByEmail(email)
    if not user:
        raise ValueError("User not found")

    user.resetPwdToken = token
    await user.save()


async def getResetPasswordToken(
    email: str,
) -> str:
    """
    Get the reset password token for a user.

    Args:
        email (str): The email address of the user.

    Returns:
        Optional[str]: The reset password token if it exists, otherwise None.
    """
    user = await findByEmail(email)
    if not user:
        raise ValueError("User not found")
    elif not user.resetPwdToken:
        raise ValueError("Reset password token not set")

    return user.resetPwdToken


async def updatePassword(
    email: str,
    new_password: str,
) -> None:
    """
    Update the password for a user.

    Args:
        email (str): The email address of the user.
        new_password (str): The new password to set.

    Returns:
        None
    """
    user = await findByEmail(email)
    if not user:
        raise ValueError("User not found")

    salt, hashed_password = hash_password(new_password, None)
    user.salt = salt
    user.hashedPassword = hashed_password
    user.resetPwdToken = None  # Clear reset token after password update
    await user.save()
