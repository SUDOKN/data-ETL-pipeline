import uuid
from fastapi import BackgroundTasks
from services.processor import process_file
from services.redis_client import set_task_status

def create_task(file_path: str, background_tasks: BackgroundTasks) -> str:
    task_id = str(uuid.uuid4())
    set_task_status(task_id, {"progress": 0.0, "message": "Queued"})
    background_tasks.add_task(process_file, file_path, task_id)
    return task_id

def get_task_status(task_id: str):
    from services.redis_client import get_task_status as redis_get_task_status
    return redis_get_task_status(task_id)
