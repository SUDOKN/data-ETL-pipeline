import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor
from typing import Optional
import multiprocessing

logger = logging.getLogger(__name__)


class ProcessPoolManager:
    """
    Singleton manager for ProcessPoolExecutor to handle CPU-intensive tasks.
    Uses multiple processes to bypass Python's GIL.
    """

    _instance: Optional["ProcessPoolManager"] = None
    _executor: Optional[ProcessPoolExecutor] = None
    _max_workers: int = 4

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def initialize(cls, max_workers: Optional[int] = None):
        """
        Initialize the process pool.

        Args:
            max_workers: Number of worker processes.
                        Defaults to min(cpu_count, 4) to avoid overwhelming the system.
        """
        if cls._executor is not None:
            logger.warning("ProcessPoolManager already initialized")
            return

        if max_workers is None:
            # Use min of (CPU count, 4) to avoid creating too many processes
            cpu_count = multiprocessing.cpu_count()
            max_workers = min(cpu_count, 4)

        cls._max_workers = max_workers
        cls._executor = ProcessPoolExecutor(max_workers=max_workers)
        logger.info(f"ProcessPoolManager initialized with {max_workers} workers")

    @classmethod
    def get_executor(cls) -> ProcessPoolExecutor:
        """Get the executor, initializing if necessary."""
        if cls._executor is None:
            cls.initialize()
        assert cls._executor is not None
        return cls._executor

    @classmethod
    async def run_in_process(cls, func, *args, **kwargs):
        """
        Run a CPU-bound function in a worker process asynchronously.

        Args:
            func: The function to run (must be picklable)
            *args, **kwargs: Arguments to pass to the function

        Returns:
            The result from the function
        """
        loop = asyncio.get_event_loop()
        executor = cls.get_executor()

        # Submit to process pool and await result
        return await loop.run_in_executor(executor, func, *args, **kwargs)

    @classmethod
    def shutdown(cls, wait: bool = True):
        """
        Shutdown the process pool.

        Args:
            wait: If True, wait for all pending tasks to complete
        """
        if cls._executor is not None:
            logger.info("Shutting down ProcessPoolManager")
            cls._executor.shutdown(wait=wait)
            cls._executor = None
