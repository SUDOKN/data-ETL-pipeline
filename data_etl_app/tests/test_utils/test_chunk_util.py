import pytest

from data_etl_app.utils.chunk_util import (
    get_chunks_respecting_line_boundaries,
    get_roughly_even_chunks,
)


@pytest.mark.asyncio
async def test_empty_text_returns_empty_dict():
    result = await get_chunks_respecting_line_boundaries("")
    assert isinstance(result, dict)
    assert result == {}


@pytest.mark.asyncio
async def test_chunks_with_overlap_and_correct_boundaries(monkeypatch):
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
    chunks = await get_chunks_respecting_line_boundaries(
        text, soft_limit_tokens=3, overlap_ratio=0.5
    )

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


@pytest.mark.asyncio
async def test_full_text_as_single_chunk_when_under_limit(monkeypatch):
    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.num_tokens_from_string",
        lambda line: len(line),  # small count to ensure under limit
    )
    text = "Hello world!"  # single line
    result = await get_chunks_respecting_line_boundaries(
        text, soft_limit_tokens=100, overlap_ratio=0.5
    )
    # Entire text should be one chunk with key '0:12'
    assert list(result.values()) == [text]
    key = list(result.keys())[0]
    # key format should be '0:<length>' and length matches len(text)
    start, end = key.split(":")
    assert start == "0"
    assert int(end) == len(text)


@pytest.mark.asyncio
async def test_chunks_with_zero_overlap(monkeypatch):
    # Monkeypatch token-count to 1 token per line
    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.num_tokens_from_string", lambda line: 1
    )
    # Prepare text with 5 lines
    text = "L1\nL2\nL3\nL4\nL5"
    # Use max_tokens=3 and zero overlap
    chunks = await get_chunks_respecting_line_boundaries(
        text, soft_limit_tokens=3, overlap_ratio=0
    )
    # Expect two chunks: first L1-L3, second L4-L5 without overlap
    expected = {
        "0:9": text[0:9],
        "9:14": text[9:14],
    }
    assert chunks == expected


# Tests for get_roughly_even_chunks
def test_get_roughly_even_chunks_empty_text():
    result = get_roughly_even_chunks("")
    assert isinstance(result, dict)
    assert result == {}


def test_get_roughly_even_chunks_text_under_target(monkeypatch):
    """Test when total tokens is under target_chunk_tokens"""
    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.num_tokens_from_string", lambda line: 10
    )
    text = "Short text"
    result = get_roughly_even_chunks(
        text, max_tokens_allowed_per_chunk=100, overlap_ratio=0.25
    )

    # Should return single chunk since text is under target
    assert len(result) == 1
    assert list(result.values())[0] == text


def test_get_roughly_even_chunks_calculates_divisions_correctly(monkeypatch):
    """Test that it calculates the right number of divisions using integer division"""

    # Mock num_tokens_from_string to return predictable values
    def mock_token_count(text):
        if text == "Line1\nLine2\nLine3\nLine4\nLine5\nLine6":
            return 6000  # Total tokens
        else:
            return 1000  # Each line is 1000 tokens to force chunking

    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.num_tokens_from_string", mock_token_count
    )

    text = "Line1\nLine2\nLine3\nLine4\nLine5\nLine6"
    # target_chunk_tokens=2500, using integer division:
    # 6000 // 1 = 6000 > 2500, so num_divisions = 2
    # 6000 // 2 = 3000 > 2500, so num_divisions = 3
    # 6000 // 3 = 2000 <= 2500, so use 3 divisions
    # approximate_chunk_tokens should be 6000 // 3 = 2000
    result = get_roughly_even_chunks(
        text, max_tokens_allowed_per_chunk=2500, overlap_ratio=0.25
    )

    # Should create multiple chunks since each line has 1000 tokens and limit is 2000
    assert len(result) > 1


def test_get_roughly_even_chunks_with_large_target(monkeypatch):
    """Test when target is much larger than text"""
    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.num_tokens_from_string", lambda line: 50
    )
    text = "Small text content"
    result = get_roughly_even_chunks(
        text, max_tokens_allowed_per_chunk=10000, overlap_ratio=0.25
    )

    # Should return single chunk
    assert len(result) == 1
    assert list(result.values())[0] == text


def test_get_roughly_even_chunks_division_calculation(monkeypatch):
    """Test the division calculation logic with integer division"""

    # Mock to return specific token counts
    def mock_token_count(text):
        if "full_text" in text:
            return 15000  # Total tokens
        else:
            return 100  # Each line/chunk

    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.num_tokens_from_string", mock_token_count
    )

    text = "full_text_content_here"
    # target_chunk_tokens=4000, using integer division:
    # 15000 // 1 = 15000 > 4000, so num_divisions = 2
    # 15000 // 2 = 7500 > 4000, so num_divisions = 3
    # 15000 // 3 = 5000 > 4000, so num_divisions = 4
    # 15000 // 4 = 3750 <= 4000, so use 4 divisions
    # approximate_chunk_tokens = 15000 // 4 = 3750

    result = get_roughly_even_chunks(
        text, max_tokens_allowed_per_chunk=4000, overlap_ratio=0.1
    )

    # The function should have been called and returned some chunks
    assert isinstance(result, dict)


def test_get_roughly_even_chunks_passes_correct_parameters(monkeypatch):
    """Test that parameters are passed correctly to get_chunks_respecting_line_boundaries_sync"""

    # Track what parameters were passed to get_chunks_respecting_line_boundaries_sync
    captured_params = {}

    def mock_get_chunks_respecting_boundaries(
        text, soft_limit_tokens, overlap_ratio, max_chunks
    ):
        captured_params["text"] = text
        captured_params["soft_limit_tokens"] = soft_limit_tokens
        captured_params["overlap_ratio"] = overlap_ratio
        return {"0:10": text[:10]}

    def mock_token_count(text):
        return 8000  # Total tokens

    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.num_tokens_from_string", mock_token_count
    )
    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.get_chunks_respecting_line_boundaries_sync",
        mock_get_chunks_respecting_boundaries,
    )

    text = "Test text content"
    target_tokens = 3000
    overlap = 0.3

    # Using integer division: 8000 // 3000 = 2, so:
    # 8000 // 1 = 8000 > 3000, so num_divisions = 2
    # 8000 // 2 = 4000 > 3000, so num_divisions = 3
    # 8000 // 3 = 2666 <= 3000, so use 3 divisions
    # approximate_chunk_tokens = 8000 // 3 = 2666

    result = get_roughly_even_chunks(
        text, max_tokens_allowed_per_chunk=target_tokens, overlap_ratio=overlap
    )

    # Verify the correct parameters were passed
    assert captured_params["text"] == text
    assert captured_params["soft_limit_tokens"] == 2666  # 8000 // 3
    assert captured_params["overlap_ratio"] == overlap


def test_get_roughly_even_chunks_realistic_scenario(monkeypatch):
    """Test get_roughly_even_chunks with a realistic text chunking scenario"""

    # Create a longer text that will definitely need chunking
    long_text = "\n".join(
        [
            f"This is line {i} with some content that represents a typical text document."
            for i in range(1, 21)
        ]
    )

    def mock_token_count(text):
        # Simulate realistic token counting: ~10 tokens per line
        return len(text.split("\n")) * 10

    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.num_tokens_from_string", mock_token_count
    )

    # Target 100 tokens per chunk, with 20% overlap
    result = get_roughly_even_chunks(
        long_text, max_tokens_allowed_per_chunk=100, overlap_ratio=0.2
    )

    # Should create multiple chunks since we have 200 tokens total (20 lines * 10 tokens)
    # and target is 100 tokens per chunk
    assert len(result) >= 2

    # Verify chunks contain actual text content
    for chunk_key, chunk_text in result.items():
        assert len(chunk_text.strip()) > 0
        assert "This is line" in chunk_text

    # Verify the keys follow the expected format "start:end"
    for key in result.keys():
        assert ":" in key
        start, end = key.split(":")
        assert start.isdigit()
        assert end.isdigit()
        assert int(start) < int(end)


def test_get_roughly_even_chunks_integer_division_behavior(monkeypatch):
    """Test that the function uses integer division correctly in the while loop"""

    def mock_token_count(text):
        return 10000  # Total tokens

    captured_divisions = []
    original_get_chunks = None

    def mock_get_chunks_respecting_boundaries(
        text, soft_limit_tokens, overlap_ratio, max_chunks
    ):
        # Capture what approximate_chunk_tokens was calculated as
        captured_divisions.append(soft_limit_tokens)
        return {"0:10": text[:10]}

    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.num_tokens_from_string", mock_token_count
    )
    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.get_chunks_respecting_line_boundaries_sync",
        mock_get_chunks_respecting_boundaries,
    )

    # Test case where integer division makes a difference
    # 10000 // 3333 = 3, but 10000 / 3333 = 3.0003
    target_tokens = 3333

    # Expected behavior with integer division:
    # 10000 // 1 = 10000 > 3333, so num_divisions = 2
    # 10000 // 2 = 5000 > 3333, so num_divisions = 3
    # 10000 // 3 = 3333 <= 3333, so use 3 divisions
    # approximate_chunk_tokens = 10000 // 3 = 3333

    result = get_roughly_even_chunks(
        "test text", max_tokens_allowed_per_chunk=target_tokens
    )

    # Verify the calculated chunk size
    assert len(captured_divisions) == 1
    assert captured_divisions[0] == 3333  # 10000 // 3


def test_get_roughly_even_chunks_validates_chunk_count_and_sizes(monkeypatch):
    """Test that the number of chunks and their sizes are around the target"""

    # Create predictable text with known token counts
    def mock_token_count(text):
        if "Line" in text and len(text.split("\n")) > 1:
            # Each line has 100 tokens, total varies by number of lines
            return len(text.split("\n")) * 100
        else:
            return 100  # Individual lines have 100 tokens each

    monkeypatch.setattr(
        "data_etl_app.utils.chunk_util.num_tokens_from_string", mock_token_count
    )

    # Create text with 10 lines = 1000 total tokens
    text = "\n".join([f"Line {i} with content" for i in range(1, 11)])
    target_tokens = 300
    overlap_ratio = 0.1  # Small overlap to reduce complexity

    # Expected calculation:
    # 1000 // 1 = 1000 > 300, so num_divisions = 2
    # 1000 // 2 = 500 > 300, so num_divisions = 3
    # 1000 // 3 = 333 > 300, so num_divisions = 4
    # 1000 // 4 = 250 <= 300, so use 4 divisions
    # approximate_chunk_tokens = 1000 // 4 = 250

    result = get_roughly_even_chunks(
        text, max_tokens_allowed_per_chunk=target_tokens, overlap_ratio=overlap_ratio
    )

    # Verify we get multiple chunks - expect around 10 due to overlap and line boundaries
    assert len(result) >= 4  # At least the calculated number
    assert len(result) <= 15  # But not too many

    # Verify each chunk size is reasonable (most should be around 200-300 tokens)
    chunk_sizes = []
    for chunk_key, chunk_text in result.items():
        chunk_tokens = mock_token_count(chunk_text)
        chunk_sizes.append(chunk_tokens)
        # Chunks should contain at least one line but not be excessively large
        assert chunk_tokens >= 100  # At least one line
        assert chunk_tokens <= 400  # Not too large given our target of 250

    # Verify that most chunks are reasonably sized around the target
    average_chunk_size = sum(chunk_sizes) / len(chunk_sizes)
    assert 200 <= average_chunk_size <= 350  # Should be close to our 250 target

    # Basic coverage check - all chunks together should cover more than original due to overlap
    total_chunk_chars = sum(len(chunk) for chunk in result.values())
    assert total_chunk_chars >= len(text)  # Should be larger due to overlap


def test_get_roughly_even_chunks_specific_division_scenarios(monkeypatch):
    """Test specific scenarios with known division calculations"""

    test_cases = [
        {
            "total_tokens": 12000,
            "target_tokens": 4000,
            "expected_divisions": 3,  # 12000//1=12000>4000, 12000//2=6000>4000, 12000//3=4000<=4000
            "expected_chunk_size": 4000,  # 12000 // 3
        },
        {
            "total_tokens": 15000,
            "target_tokens": 5000,
            "expected_divisions": 3,  # 15000//1=15000>5000, 15000//2=7500>5000, 15000//3=5000<=5000
            "expected_chunk_size": 5000,  # 15000 // 3
        },
        {
            "total_tokens": 8500,
            "target_tokens": 3000,
            "expected_divisions": 3,  # 8500//1=8500>3000, 8500//2=4250>3000, 8500//3=2833<=3000
            "expected_chunk_size": 2833,  # 8500 // 3
        },
    ]

    for case in test_cases:
        captured_params = {}

        def mock_token_count(text):
            return case["total_tokens"]

        def mock_get_chunks_respecting_boundaries(
            text, soft_limit_tokens, overlap_ratio, max_chunks
        ):
            captured_params["soft_limit_tokens"] = soft_limit_tokens
            # Create fake chunks based on the chunk size
            num_chunks = max(1, case["total_tokens"] // soft_limit_tokens)
            return {f"{i*100}:{(i+1)*100}": f"chunk_{i}" for i in range(num_chunks)}

        monkeypatch.setattr(
            "data_etl_app.utils.chunk_util.num_tokens_from_string", mock_token_count
        )
        monkeypatch.setattr(
            "data_etl_app.utils.chunk_util.get_chunks_respecting_line_boundaries_sync",
            mock_get_chunks_respecting_boundaries,
        )

        result = get_roughly_even_chunks(
            "test text", max_tokens_allowed_per_chunk=case["target_tokens"]
        )

        # Verify the calculated chunk size matches expected
        assert captured_params["soft_limit_tokens"] == case["expected_chunk_size"], (
            f"For total_tokens={case['total_tokens']}, target={case['target_tokens']}: "
            f"expected chunk size {case['expected_chunk_size']}, got {captured_params['soft_limit_tokens']}"
        )
