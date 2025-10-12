from dataclasses import dataclass

from open_ai_key_app.utils.token_util import num_tokens_from_string


@dataclass
class ChunkingStrat:
    overlap: float  # must be between [0, 1)
    max_tokens: int = 10000

    def __post_init__(self):
        if self.overlap < 0 or self.overlap >= 1:
            raise ValueError("Overlap must be between >=0 and <1")
        if self.max_tokens > 25000:
            raise ValueError("Max Tokens must be less than 20000")


def get_roughly_even_chunks(
    text: str, max_tokens_allowed_per_chunk: int = 120000, overlap_ratio: float = 0.25
) -> dict[str, str]:
    """
    Attempts to create roughly even-sized chunks with the given target size.
    Actual chunks may vary due to line boundaries and overlap.
    """
    num_divisions = 1
    total_tokens = num_tokens_from_string(text)

    # Find how many divisions we need to get close to our target
    while total_tokens // num_divisions > max_tokens_allowed_per_chunk:
        num_divisions += 1

    # Calculate target size for each division
    approximate_chunk_tokens = total_tokens // num_divisions
    return get_chunks_respecting_line_boundaries(
        text, approximate_chunk_tokens, overlap_ratio
    )


def get_chunks_respecting_line_boundaries(
    text: str, soft_limit_tokens: int = 5000, overlap_ratio: float = 0.25
) -> dict[str, str]:
    """
    Splits text into chunks that try to stay around soft_limit_tokens, but may
    exceed it to respect line boundaries. Each chunk overlaps with the previous
    by overlap_ratio of tokens.

    Args:
        text (str): The input text to be chunked.
        soft_limit_tokens (int): Target token count per chunk (may be exceeded for line boundaries).
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
