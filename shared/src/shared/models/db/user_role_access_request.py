from beanie import Document
from enum import Enum
from typing import Optional

from shared.models.db.user import UserRole


class UserRoleAccessRequest(Document):
    user_id: str
    requested_role: UserRole
    status: str
