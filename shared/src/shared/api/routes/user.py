from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
import logging

from shared.models.new_user import NewUser
from shared.services import user_service
from shared.utils.password_util import verify_password

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/users/new", response_class=JSONResponse)
async def create_user(new_user: NewUser):
    try:
        existing_user = await user_service.findByEmail(new_user.email)
        if existing_user:
            raise HTTPException(
                status_code=400, detail="User with this email already exists"
            )

        registered_new_user = await user_service.addNewUser(new_user)
        return registered_new_user
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# get_user /users/:email
@router.get("/users/login", response_class=JSONResponse)
async def login_user(
    email: str = Query(
        description=(f"Email of the user to log in."),
    ),
    password: str = Query(
        description=(f"Password of the user to log in."),
    ),
):
    """Get a registered user by email and password.
    Args:
        email (str): The email address of the user.
        password (str): The password of the user.
    Returns:
        User: The user object if found and password is valid.
    Raises:
        HTTPException: If user is not found or password is invalid.
    """
    try:
        user = await user_service.findByEmail(email)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        if not verify_password(password, user.hashedPassword, user.salt):
            raise HTTPException(status_code=401, detail="Invalid password")

        return user
    except Exception as e:
        logger.error(f"Error fetching user: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
