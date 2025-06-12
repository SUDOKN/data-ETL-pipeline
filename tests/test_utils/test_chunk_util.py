import pytest

from data_etl_app.utils.chunk_util import get_chunks_with_boundaries


def test_empty_text_returns_empty_dict():
    result = get_chunks_with_boundaries("")
    assert isinstance(result, dict)
    assert result == {}


def test_chunks_with_overlap_and_correct_boundaries(monkeypatch):
    # Monkeypatch token-count function so each line counts as 1 token
    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.num_tokens_from_string", lambda line: 1
    )
    # Prepare text with 5 lines
    text = """\
L1
L2
L3
L4
L5"""  # note: only first 4 lines end with newline when splitlines(keepends=True)
    # Use max_tokens=3 and 50% overlap => overlap of 1 line
    chunks = get_chunks_with_boundaries(text, MAX_CHUNK_TOKENS=3, overlap_ratio=0.5)

    # Expect two chunks: first with lines L1, L2, L3; second with L3, L4, L5
    # Calculate expected keys and values manually
    # First chunk: start=0, last_line_end at end of "L3\n" => offset 9
    # expected_first_text = "L1\nL2\nL3\n"
    # expected_second_text = "L3\nL4\nL5"

    # Build expected result dict
    expected = {
        "0:9": text[0:9],
        "6:14": text[6:14],
    }
    assert chunks == expected


def test_full_text_as_single_chunk_when_under_limit(monkeypatch):
    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.num_tokens_from_string",
        lambda line: len(line),  # small count to ensure under limit
    )
    text = "Hello world!"  # single line
    result = get_chunks_with_boundaries(text, MAX_CHUNK_TOKENS=100, overlap_ratio=0.5)
    # Entire text should be one chunk with key '0:12'
    assert list(result.values()) == [text]
    key = list(result.keys())[0]
    # key format should be '0:<length>' and length matches len(text)
    start, end = key.split(":")
    assert start == "0"
    assert int(end) == len(text)


def test_chunks_with_zero_overlap(monkeypatch):
    # Monkeypatch token-count to 1 token per line
    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.num_tokens_from_string", lambda line: 1
    )
    # Prepare text with 5 lines
    text = "L1\nL2\nL3\nL4\nL5"
    # Use max_tokens=3 and zero overlap
    chunks = get_chunks_with_boundaries(text, MAX_CHUNK_TOKENS=3, overlap_ratio=0)
    # Expect two chunks: first L1-L3, second L4-L5 without overlap
    expected = {
        "0:9": text[0:9],
        "9:14": text[9:14],
    }
    assert chunks == expected
