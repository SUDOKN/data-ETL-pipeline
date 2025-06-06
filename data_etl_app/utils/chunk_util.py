import bisect
from dataclasses import dataclass

from utils.multi_key_gpt import num_tokens_from_string


@dataclass
class ChunkingStrat:
    overlap: float  # must be between [0, 1)
    max_tokens: int = 10000

    def __post_init__(self):
        if self.overlap < 0 or self.overlap >= 1:
            raise ValueError("Overlap must be between >=0 and <1")
        if self.max_tokens > 25000:
            raise ValueError("Max Tokens must be less than 20000")


def get_chunks(text: str, MAX_CHUNK_TOKENS: int = 5000) -> list[str]:
    # Split the text into lines
    lines = text.split("\n")
    # Precompute token counts for each line
    token_counts = [num_tokens_from_string(line) for line in lines]

    # Create a cumulative sum of token counts
    cumulative = [0]
    for count in token_counts:
        cumulative.append(cumulative[-1] + count)

    chunks: list[str] = []
    start_idx = 0
    n = len(lines)

    while start_idx < n:
        # Find the furthest index where cumulative token count difference is within the limit
        target = cumulative[start_idx] + MAX_CHUNK_TOKENS
        # bisect_right returns the first index where cumulative > target, so subtract one to stay within limit
        end_idx = bisect.bisect_right(cumulative, target) - 1
        # Make sure that at least one line is added in case of an extremely long single line
        if end_idx <= start_idx:
            end_idx = start_idx + 1
        chunks.append("\n".join(lines[start_idx:end_idx]))
        start_idx = end_idx

    return chunks


def get_chunks_with_overlap(
    text: str, MAX_CHUNK_TOKENS: int = 5000, overlap_ratio: float = 0.25
) -> list[str]:
    """
    Splits the input text into chunks where each chunk's token count is
    less than or equal to MAX_CHUNK_TOKENS. Each new chunk will start with
    an overlap equal to 'overlap_ratio' (default 25%) of the previous chunk's tokens.

    Args:
        text (str): The input text to be chunked.
        MAX_CHUNK_TOKENS (int): Maximum number of tokens allowed per chunk, including overlap.
        overlap_ratio (float): The fraction of tokens to overlap from the previous chunk.

    Returns:
        list[str]: A list of text chunks with overlapping content.
    """
    chunks: list[str] = []
    current_chunk: list[str] = []
    current_chunk_tokens: int = 0

    # Split text into lines
    lines: list[str] = text.split("\n")

    for line in lines:
        line_tokens = num_tokens_from_string(line)
        if current_chunk_tokens + line_tokens > MAX_CHUNK_TOKENS:
            # Determine target number of tokens for the overlap
            target_overlap_tokens: int = int(current_chunk_tokens * overlap_ratio)
            overlap_lines: list[str] = []
            overlap_tokens: int = 0

            # Only collect overlap if target_overlap_tokens > 0 i.e. overlap_ratio > 0
            # otherwise, the last line will always be included in the new chunk
            if target_overlap_tokens > 0:
                # Work backwards over the current chunk to collect enough overlap tokens.
                # We use entire lines even if it slightly exceeds the target.
                for l in reversed(current_chunk):
                    tokens: int = num_tokens_from_string(l)
                    overlap_lines.insert(0, l)  # insert at beginning to maintain order
                    overlap_tokens += tokens
                    if overlap_tokens >= target_overlap_tokens:
                        break

            # Finalize current chunk and start new chunk with the overlap lines
            chunks.append("\n".join(current_chunk))
            current_chunk = overlap_lines + [line]
            current_chunk_tokens = overlap_tokens + line_tokens
        else:
            # Add line to the current chunk
            current_chunk.append(line)
            current_chunk_tokens += line_tokens

    # Append the final chunk if it exists
    if current_chunk:
        chunks.append("\n".join(current_chunk))

    return chunks


def get_chunks_with_boundaries(
    text: str, MAX_CHUNK_TOKENS: int = 5000, overlap_ratio: float = 0.25
) -> dict[str, str]:
    """
    Splits the input text into chunks where each chunk's token count is
    ≤ MAX_CHUNK_TOKENS. Each new chunk will start with an overlap equal
    to 'overlap_ratio' of the previous chunk's tokens. Returns a dict
    mapping "start:end" character‐offset pairs (in the original text) to
    the chunk text.

    Args:
        text (str): The input text to be chunked.
        MAX_CHUNK_TOKENS (int): Maximum number of tokens allowed per chunk.
        overlap_ratio (float): Fraction of tokens to overlap from the previous chunk.

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
        if current_chunk_tokens + line_tokens > MAX_CHUNK_TOKENS and current_chunk:
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
