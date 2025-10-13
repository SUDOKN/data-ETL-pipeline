from dataclasses import dataclass
import logging
import time

from open_ai_key_app.utils.token_util import num_tokens_from_string
from data_etl_app.utils.process_pool_manager import ProcessPoolManager

logger = logging.getLogger(__name__)


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
    size_threshold_kb: int = 100,  # Only use multiprocessing for texts >100KB
    max_chunks: int | None = None,
) -> dict[str, str]:
    """
    ASYNC version that automatically uses multiprocessing for large texts.

    Args:
        text: The input text to be chunked
        soft_limit_tokens: Target token count per chunk
        overlap_ratio: Fraction of tokens to overlap
        use_multiprocessing: Whether to use ProcessPoolExecutor for large texts
        size_threshold_kb: Minimum text size (in KB) to use multiprocessing
        max_chunks: Maximum number of chunks to generate. If None, generates all chunks.

    Returns:
        dict[str, str]: A mapping from "start:end" character offsets to chunk text
    """
    start_time = time.perf_counter()
    text_size_kb = len(text) / 1024

    # Decide whether to use multiprocessing
    # Only worth it for large texts when requesting many chunks (overhead ~50-80ms)
    should_use_multiprocessing = (
        use_multiprocessing
        and text_size_kb >= size_threshold_kb
        and (max_chunks is None or max_chunks > 3)  # Only worth it for 4+ chunks
    )

    # For small texts or when requesting very few chunks, run synchronously
    if not should_use_multiprocessing:
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
        # For large texts with many chunks, use multiprocessing
        logger.debug(
            f"Chunking {text_size_kb:.1f}KB text in worker process "
            f"(text size: {text_size_kb:.1f}KB, chunks: {max_chunks or 'all'})"
        )

        result = await ProcessPoolManager.run_in_process(
            get_chunks_respecting_line_boundaries_sync,
            text,
            soft_limit_tokens,
            overlap_ratio,
            max_chunks,
        )

    elapsed_ms = (time.perf_counter() - start_time) * 1000
    method = "multiprocess" if should_use_multiprocessing else "sync"

    logger.info(
        f"Chunking [{method}]: {text_size_kb:.1f}KB → {len(result)} chunks "
        f"in {elapsed_ms:.1f}ms{f' (max_chunks={max_chunks})' if max_chunks else ''}"
    )

    return result
