from dataclasses import dataclass
import multiprocessing
import logging
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor

from open_ai_key_app.utils.token_util import num_tokens_from_string

logger = logging.getLogger(__name__)


def split_bytes_on_line_boundaries(
    data: bytes,
    max_chunk_size: int,
    newline_search_window: int = 10000,
) -> list[bytes]:
    """
    Split binary data into chunks at line boundaries (newline characters).

    This is particularly useful for JSONL files where each line is a complete JSON object.
    Ensures that no JSON objects are split across chunks.

    Args:
        data: The binary data to split
        max_chunk_size: Maximum size of each chunk in bytes
        newline_search_window: How far back from the chunk boundary to search for a newline (default: 10KB)

    Returns:
        list[bytes]: List of binary chunks, each ending on a line boundary (except possibly the last)

    Example:
        >>> data = b'{"a":1}\n{"b":2}\n{"c":3}\n'
        >>> chunks = split_bytes_on_line_boundaries(data, max_chunk_size=10)
        >>> # Each chunk will end at a newline, preserving JSON object integrity
    """
    if not data:
        return []

    if len(data) <= max_chunk_size:
        return [data]

    chunks = []
    offset = 0
    data_size = len(data)

    while offset < data_size:
        # Calculate the target chunk size
        chunk_size = min(max_chunk_size, data_size - offset)

        # If this isn't the last chunk, find the last newline within the chunk
        # to avoid splitting lines (e.g., JSON objects) across chunks
        if offset + chunk_size < data_size:
            # Look for the last newline in this chunk
            chunk_end = offset + chunk_size
            # Search backwards from chunk_end to find last newline
            search_start = max(offset, chunk_end - newline_search_window)
            last_newline = data.rfind(b"\n", search_start, chunk_end)

            if last_newline != -1 and last_newline > offset:
                # Found a newline, split there (include the newline in this chunk)
                chunk_size = last_newline - offset + 1
            # If no newline found, keep the original chunk_size
            # (rare for line-based formats like JSONL, but handles edge cases)

        chunk = data[offset : offset + chunk_size]
        chunks.append(chunk)
        offset += chunk_size

    return chunks


# Module-level thread pool for chunking operations
_chunk_thread_pool: ThreadPoolExecutor | None = None


def get_chunk_thread_pool() -> ThreadPoolExecutor:
    """Get or create thread pool for chunking operations

    Uses all CPU cores since:
    1. Chunking is CPU-bound (tokenization + string operations)
    2. Overall concurrency is controlled by semaphore in calling code
    3. This prevents chunking from becoming a bottleneck
    """
    global _chunk_thread_pool
    if _chunk_thread_pool is None:

        # Use all CPU cores for maximum throughput on CPU-bound chunking
        # max_workers = multiprocessing.cpu_count()
        max_workers = multiprocessing.cpu_count() + 2  # 10 cores → 12 workers
        _chunk_thread_pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="chunking"
        )
        logger.info(f"Initialized chunking thread pool with {max_workers} workers")
    return _chunk_thread_pool


def shutdown_chunk_thread_pool(wait: bool = True):
    """Shutdown the chunking thread pool"""
    global _chunk_thread_pool
    if _chunk_thread_pool is not None:
        logger.info("Shutting down chunking thread pool")
        _chunk_thread_pool.shutdown(wait=wait)
        _chunk_thread_pool = None


@dataclass
class ChunkingStrat:
    overlap: float  # must be between [0, 1)
    max_tokens: int = 10000

    def __post_init__(self):
        if self.overlap < 0 or self.overlap >= 1:
            raise ValueError("Overlap must be between >=0 and <1")
        if self.max_tokens > 25000:
            raise ValueError("Max Tokens must be less than 25000")


def get_roughly_even_chunks(
    text: str,
    max_tokens_allowed_per_chunk: int = 120000,
    overlap_ratio: float = 0.25,
    max_chunks: int | None = None,
) -> dict[str, str]:
    """
    Attempts to create roughly even-sized chunks with the given target size.
    Actual chunks may vary due to line boundaries and overlap.

    Args:
        text: The input text to be chunked
        max_tokens_allowed_per_chunk: Maximum tokens per chunk
        overlap_ratio: Fraction of tokens to overlap
        max_chunks: Maximum number of chunks to generate. If None, generates all chunks.
    """
    num_divisions = 1
    total_tokens = num_tokens_from_string(text)

    # Find how many divisions we need to get close to our target
    while total_tokens // num_divisions > max_tokens_allowed_per_chunk:
        num_divisions += 1

    # Calculate target size for each division
    approximate_chunk_tokens = total_tokens // num_divisions
    return get_chunks_respecting_line_boundaries_sync(
        text, approximate_chunk_tokens, overlap_ratio, max_chunks
    )


def get_chunks_respecting_line_boundaries_sync(
    text: str,
    soft_limit_tokens: int = 5000,
    overlap_ratio: float = 0.25,
    max_chunks: int | None = None,
) -> dict[str, str]:
    """
    SYNC version for use in ProcessPoolExecutor.

    Splits text into chunks that try to stay around soft_limit_tokens, but may
    exceed it to respect line boundaries. Each chunk overlaps with the previous
    by overlap_ratio of tokens.

    Args:
        text (str): The input text to be chunked.
        soft_limit_tokens (int): Target token count per chunk (may be exceeded for line boundaries).
        overlap_ratio (float): Fraction of tokens to overlap from the previous chunk.
        max_chunks (int | None): Maximum number of chunks to generate. If None, generates all chunks.

    Returns:
        dict[str, str]: A mapping from "start:end" character offsets to chunk text.
    """
    chunks_with_bounds: dict[str, str] = {}

    # Split into lines, preserving newline characters so we can track exact offsets.
    lines_with_ends = text.splitlines(keepends=True)

    # Build a list of (line_text, token_count, start_offset, end_offset) tuples.
    line_info: list[tuple[str, int, int, int]] = []
    char_offset = 0
    for raw_line in lines_with_ends:
        line_tokens = num_tokens_from_string(raw_line)
        start = char_offset
        length = len(raw_line)
        end = char_offset + length
        line_info.append((raw_line, line_tokens, start, end))
        char_offset = end

    # State for the current chunk:
    current_chunk: list[tuple[str, int, int, int]] = []
    current_chunk_tokens = 0
    current_chunk_start: int | None = None

    for line_text, line_tokens, line_start, line_end in line_info:
        # If adding this line would exceed token limit, finalize current chunk first.
        if current_chunk_tokens + line_tokens > soft_limit_tokens and current_chunk:
            # Compute how many tokens to carry as overlap
            target_overlap = int(current_chunk_tokens * overlap_ratio)
            overlap_lines: list[tuple[str, int, int, int]] = []
            overlap_tokens = 0

            if target_overlap > 0:
                # Grab full lines from the end until we reach target_overlap
                for l_text, l_tok, l_start, l_end in reversed(current_chunk):
                    overlap_lines.insert(0, (l_text, l_tok, l_start, l_end))
                    overlap_tokens += l_tok
                    if overlap_tokens >= target_overlap:
                        break

            # Finalize the chunk we're closing:
            #   - start = current_chunk_start
            #   - end = end offset of the last line in current_chunk
            last_line_end = current_chunk[-1][3]
            chunk_text = "".join(l[0] for l in current_chunk)
            key = f"{current_chunk_start}:{last_line_end}"
            chunks_with_bounds[key] = chunk_text

            # Early stop if we've reached max_chunks
            if max_chunks is not None and len(chunks_with_bounds) >= max_chunks:
                return chunks_with_bounds

            # Build the next chunk, starting from the overlap (if any), plus this line
            if overlap_lines:
                new_start = overlap_lines[0][2]
            else:
                new_start = line_start

            current_chunk = overlap_lines + [
                (line_text, line_tokens, line_start, line_end)
            ]
            current_chunk_tokens = overlap_tokens + line_tokens
            current_chunk_start = new_start

        else:
            # Just append this line into the current chunk
            if not current_chunk:
                current_chunk_start = line_start
            current_chunk.append((line_text, line_tokens, line_start, line_end))
            current_chunk_tokens += line_tokens

    # Finalize the last chunk (if any)
    if current_chunk:
        last_line_end = current_chunk[-1][3]
        chunk_text = "".join(l[0] for l in current_chunk)
        key = f"{current_chunk_start}:{last_line_end}"
        chunks_with_bounds[key] = chunk_text

    return chunks_with_bounds


async def get_chunks_respecting_line_boundaries(
    text: str,
    soft_limit_tokens: int = 5000,
    overlap_ratio: float = 0.25,
    use_multiprocessing: bool = True,
    size_threshold_kb: int = 100,  # Only use thread pool for texts >100KB
    max_chunks: int | None = None,
) -> dict[str, str]:
    """
    ASYNC version that runs chunking in thread pool to avoid blocking event loop.

    Args:
        text: The input text to be chunked
        soft_limit_tokens: Target token count per chunk
        overlap_ratio: Fraction of tokens to overlap
        use_multiprocessing: Whether to use thread pool for large texts
        size_threshold_kb: Minimum text size (in KB) to use thread pool
        max_chunks: Maximum number of chunks to generate. If None, generates all chunks.

    Returns:
        dict[str, str]: A mapping from "start:end" character offsets to chunk text
    """
    start_time = time.perf_counter()
    text_size_kb = len(text) / 1024

    # Decide whether to use thread pool
    # Only worth it for large texts when requesting many chunks (overhead ~50-80ms)
    should_use_threading = (
        use_multiprocessing
        and text_size_kb >= size_threshold_kb
        and (max_chunks is None or max_chunks > 3)  # Only worth it for 4+ chunks
    )

    # For small texts or when requesting very few chunks, run synchronously
    if not should_use_threading:
        reasons = []
        if not use_multiprocessing:
            reasons.append("disabled")
        elif text_size_kb < size_threshold_kb:
            reasons.append(
                f"text too small ({text_size_kb:.1f}KB < {size_threshold_kb}KB)"
            )
        if max_chunks is not None and max_chunks <= 3:
            reasons.append(f"requesting only {max_chunks} chunk(s)")

        logger.debug(
            f"Chunking {text_size_kb:.1f}KB text synchronously "
            f"({', '.join(reasons) if reasons else 'default'})"
        )
        result = get_chunks_respecting_line_boundaries_sync(
            text, soft_limit_tokens, overlap_ratio, max_chunks
        )
    else:
        # Run in thread pool to avoid blocking event loop
        logger.debug(
            f"Chunking {text_size_kb:.1f}KB text in thread pool "
            f"(text size: {text_size_kb:.1f}KB, chunks: {max_chunks or 'all'})"
        )

        loop = asyncio.get_event_loop()
        thread_pool = get_chunk_thread_pool()

        result = await loop.run_in_executor(
            thread_pool,
            get_chunks_respecting_line_boundaries_sync,
            text,
            soft_limit_tokens,
            overlap_ratio,
            max_chunks,
        )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    method = "thread_pool" if should_use_threading else "sync"

    logger.info(
        f"Chunking [{method}]: {text_size_kb:.1f}KB → {len(result)} chunks "
        f"in {elapsed_ms:.1f}ms{f' (max_chunks={max_chunks})' if max_chunks else ''}"
    )

    return result
