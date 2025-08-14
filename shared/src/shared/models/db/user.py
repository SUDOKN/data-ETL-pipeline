from beanie import Document
from enum import Enum
from typing import Optional


class UserRole(str, Enum):
    DEFAULT = "default"
    EMPLOYEE = "employee"
    MEP = "mep"


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

    class Settings:
        name = "users"
