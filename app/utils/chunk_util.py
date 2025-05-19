import bisect
from dataclasses import dataclass

from multi_key_gpt import num_tokens_from_string


@dataclass
class ChunkingStrat:
    overlap: float  # must be between [0, 1)
    max_tokens: int = 10000

    def __post_init__(self):
        if self.overlap < 0 or self.overlap >= 1:
            raise ValueError("Overlap must be between >=0 and < 1")
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
