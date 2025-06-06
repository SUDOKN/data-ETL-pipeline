from pydantic import BaseModel
from typing import Optional

class StatusResponse(BaseModel):
    task_id: str
    progress: float
    message: Optional[str] = None
