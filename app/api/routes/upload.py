from fastapi import APIRouter, UploadFile, File, BackgroundTasks, HTTPException
from schemas.upload import UploadResponse
from services.task_manager import create_task
from utils.file_utils import save_upload_file

router = APIRouter()

@router.post("/upload", response_model=UploadResponse)
async def upload_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename must not be empty.")
    file_path = save_upload_file(file)
    task_id = create_task(file_path, background_tasks)
    return UploadResponse(task_id=task_id, filename=file.filename, status="processing")
