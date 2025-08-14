from typing import Optional
from pydantic import BaseModel


class NewUser(BaseModel):
    firstName: str
    lastName: str
    email: str
    password: str
    companyURL: Optional[str]
