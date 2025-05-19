import redis
import json
from typing import Any, Dict, Optional

_redis = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

def set_task_status(task_id: str, status: dict[str, Any]) -> None:
    _redis.set(task_id, json.dumps(status))

def get_task_status(task_id: str) -> Optional[Dict[str, Any]]:
    data = _redis.get(task_id)
    if data:
        return json.loads(data)
    return None

def update_task_progress(task_id: str, progress: float, message: str) -> None:
    status: Dict[str, Any] = get_task_status(task_id) or {}
    status.update({"progress": progress, "message": message})
    set_task_status(task_id, status)
