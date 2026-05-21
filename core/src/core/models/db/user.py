from datetime import datetime
from typing import Optional
from beanie import Document
from enum import Enum
from pydantic import Field

from core.utils.time_util import get_current_time


class UserRole(str, Enum):
    DEFAULT = "default"
    EMPLOYEE = "employee"
    MEP = "mep"
    ADMIN = "admin"


class User(Document):
    firstName: str
    lastName: str
    email: str
    role: UserRole = UserRole.DEFAULT
    companyURL: Optional[
        str
    ]  # in case of UserRole[employee], companyURL has to be a valid manufacturer URL. This is verified when user attempts to edit a manufacturer profile.
    salt: str
    hashedPassword: str
    resetPwdToken: str | None = None
    createdAt: datetime = Field(default_factory=lambda: get_current_time())
    updatedAt: datetime = Field(default_factory=lambda: get_current_time())

    class Settings:
        name = "users"


"""
Indexes for Users

db.users.createIndex(
    { email: 1 },
    { name: "unique_user_email", unique: true }
)
"""
