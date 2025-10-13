"""
Test script for parallel chunking implementation.
Run this to verify the ProcessPoolManager and async chunking work correctly.
"""

import asyncio
import logging
import time

# Setup logging to see performance metrics
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

from data_etl_app.utils.process_pool_manager import ProcessPoolManager
from data_etl_app.utils.chunk_util import get_chunks_respecting_line_boundaries

logger = logging.getLogger(__name__)


async def test_small_text():
    """Test chunking with small text (should use sync method)"""
    logger.info("=" * 60)
    logger.info("TEST 1: Small text (<100KB)")
    logger.info("=" * 60)

    small_text = "Hello world.\n" * 1000  # ~13KB

    start = time.perf_counter()
    chunks = await get_chunks_respecting_line_boundaries(
        small_text,
        soft_limit_tokens=100,
        overlap_ratio=0.1,
    )
    elapsed = time.perf_counter() - start

    logger.info(f"Result: {len(chunks)} chunks in {elapsed*1000:.2f}ms")
    logger.info(f"Expected: Should use SYNC method (text is small)")
    logger.info("")


async def test_large_text():
    """Test chunking with large text (should use multiprocess)"""
    logger.info("=" * 60)
    logger.info("TEST 2: Large text (>100KB)")
    logger.info("=" * 60)

    # Generate ~200KB of text
    large_text = "This is a test line with some content.\n" * 5000

    start = time.perf_counter()
    chunks = await get_chunks_respecting_line_boundaries(
        large_text,
        soft_limit_tokens=500,
        overlap_ratio=0.15,
    )
    elapsed = time.perf_counter() - start

    logger.info(f"Result: {len(chunks)} chunks in {elapsed*1000:.2f}ms")
    logger.info(f"Expected: Should use MULTIPROCESS method (text is large)")
    logger.info("")


async def test_parallel_chunking():
    """Test multiple concurrent chunking operations"""
    logger.info("=" * 60)
    logger.info("TEST 3: Parallel chunking (4 concurrent operations)")
    logger.info("=" * 60)

    # Generate 4 different large texts
    texts = [
        "Certificate text content here.\n" * 4000,  # ~100KB
        "Industry description text.\n" * 5000,  # ~125KB
        "Process capability info.\n" * 3000,  # ~75KB (should use sync)
        "Material specification.\n" * 6000,  # ~150KB
    ]

    start = time.perf_counter()

    # Run all chunking operations in parallel
    results = await asyncio.gather(
        get_chunks_respecting_line_boundaries(texts[0], 1000, 0.0),
        get_chunks_respecting_line_boundaries(texts[1], 800, 0.15),
        get_chunks_respecting_line_boundaries(texts[2], 600, 0.15),
        get_chunks_respecting_line_boundaries(texts[3], 1000, 0.1),
    )

    elapsed = time.perf_counter() - start

    logger.info(f"Result: Processed 4 texts in {elapsed*1000:.2f}ms")
    logger.info(f"  - Text 1: {len(results[0])} chunks")
    logger.info(f"  - Text 2: {len(results[1])} chunks")
    logger.info(f"  - Text 3: {len(results[2])} chunks")
    logger.info(f"  - Text 4: {len(results[3])} chunks")
    logger.info(f"Expected: Should complete faster than sequential processing")
    logger.info("")


async def test_disabled_multiprocessing():
    """Test with multiprocessing explicitly disabled"""
    logger.info("=" * 60)
    logger.info("TEST 4: Large text with multiprocessing disabled")
    logger.info("=" * 60)

    large_text = "This is test content.\n" * 5000  # ~100KB

    start = time.perf_counter()
    chunks = await get_chunks_respecting_line_boundaries(
        large_text,
        soft_limit_tokens=500,
        overlap_ratio=0.1,
        use_multiprocessing=False,  # Force sync mode
    )
    elapsed = time.perf_counter() - start

    logger.info(f"Result: {len(chunks)} chunks in {elapsed*1000:.2f}ms")
    logger.info(f"Expected: Should use SYNC method (multiprocessing disabled)")
    logger.info("")


async def main():
    """Run all tests"""
    logger.info("\n" + "=" * 60)
    logger.info("PARALLEL CHUNKING TEST SUITE")
    logger.info("=" * 60 + "\n")

    # Initialize process pool
    ProcessPoolManager.initialize(max_workers=4)
    logger.info("✓ ProcessPoolManager initialized with 4 workers\n")

    try:
        # Run all tests
        await test_small_text()
        await test_large_text()
        await test_parallel_chunking()
        await test_disabled_multiprocessing()

        logger.info("=" * 60)
        logger.info("ALL TESTS COMPLETED SUCCESSFULLY ✓")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Test failed with error: {e}", exc_info=True)
        raise
    finally:
        # Cleanup
        ProcessPoolManager.shutdown(wait=True)
        logger.info("\n✓ ProcessPoolManager shut down cleanly")


if __name__ == "__main__":
    asyncio.run(main())
