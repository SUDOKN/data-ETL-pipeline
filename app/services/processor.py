import asyncio
from services.redis_client import update_task_progress

def process_file(file_path: str, task_id: str):
    async def _process():
        total_steps = 5
        for step in range(1, total_steps + 1):
            await asyncio.sleep(1)
            update_task_progress(task_id, step / total_steps, f"Step {step} done")
        update_task_progress(task_id, 1.0, "Processing complete")
    asyncio.create_task(_process())
