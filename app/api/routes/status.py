from fastapi import APIRouter, HTTPException
from schemas.status import StatusResponse
from services.task_manager import get_task_status

router = APIRouter()

@router.get("/status/{task_id}", response_model=StatusResponse)
async def status(task_id: str):
    status = get_task_status(task_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return status
