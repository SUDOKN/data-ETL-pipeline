import asyncio
import json
from services.redis_client import redis_client
from typing import Optional

def update_redis_progress(filename: str, progress: float, step: int, result: Optional[str] = None) -> None:
    data: dict[str, float | int | str] = {"progress": progress, "step": step}
    if result is not None:
        data["result"] = result
    redis_client.set(filename, json.dumps(data))

async def process_file_and_update_redis(file_path: str, filename: str) -> None:
    total_steps = 5
    with open(file_path, 'r') as f:
        lines = f.readlines()
    for step in range(1, total_steps + 1):
        await asyncio.sleep(1)
        update_redis_progress(filename, step / total_steps, step)
    num_lines = len(lines)
    update_redis_progress(filename, 1.0, total_steps, f"Task complete! File has {num_lines} lines.")
