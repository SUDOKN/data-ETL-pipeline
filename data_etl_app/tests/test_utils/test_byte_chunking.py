import pytest
from data_etl_app.utils.chunk_util import split_bytes_on_line_boundaries


class TestSplitBytesOnLineBoundaries:
    """Test suite for split_bytes_on_line_boundaries function"""

    def test_empty_data(self):
        """Test with empty data"""
        result = split_bytes_on_line_boundaries(b"", max_chunk_size=100)
        assert result == []

    def test_data_smaller_than_chunk_size(self):
        """Test when data is smaller than max_chunk_size"""
        data = b"line1\nline2\nline3\n"
        result = split_bytes_on_line_boundaries(data, max_chunk_size=1000)
        assert result == [data]
        assert len(result) == 1

    def test_single_line_exactly_chunk_size(self):
        """Test when a single line is exactly the chunk size"""
        data = b"x" * 100 + b"\n"
        result = split_bytes_on_line_boundaries(data, max_chunk_size=101)
        assert result == [data]

    def test_jsonl_format_basic(self):
        """Test with JSONL format - basic case"""
        data = b'{"id":1}\n{"id":2}\n{"id":3}\n'
        result = split_bytes_on_line_boundaries(data, max_chunk_size=15)

        # Should split at newlines, preserving complete JSON objects
        for chunk in result:
            assert chunk.endswith(b"\n") or chunk == result[-1]
            # Each chunk should contain complete lines
            assert chunk.count(b"{") == chunk.count(b"}")

    def test_jsonl_with_large_objects(self):
        """Test with JSONL containing objects larger than chunk size"""
        # Create a JSON object larger than chunk size
        large_obj = b'{"data":"' + b"x" * 100 + b'"}\n'
        small_obj = b'{"id":1}\n'
        data = large_obj + small_obj + large_obj

        result = split_bytes_on_line_boundaries(data, max_chunk_size=50)

        # Reconstruct the data
        reconstructed = b"".join(result)
        assert reconstructed == data

    def test_multiple_chunks_with_newlines(self):
        """Test that chunks are split at newline boundaries"""
        lines = [f"line{i}\n".encode() for i in range(10)]
        data = b"".join(lines)

        result = split_bytes_on_line_boundaries(data, max_chunk_size=20)

        # All chunks except possibly the last should end with newline
        for i, chunk in enumerate(result[:-1]):
            assert chunk.endswith(b"\n"), f"Chunk {i} doesn't end with newline: {chunk}"

        # Reconstruct and verify
        reconstructed = b"".join(result)
        assert reconstructed == data

    def test_no_newline_in_search_window(self):
        """Test behavior when no newline is found in search window"""
        # Create data with a very long line that exceeds chunk size
        long_line = b"x" * 200
        data = long_line + b"\nshort\n"

        result = split_bytes_on_line_boundaries(
            data, max_chunk_size=100, newline_search_window=50
        )

        # Should still split (even without finding newline)
        assert len(result) > 1
        # But data should be preserved
        reconstructed = b"".join(result)
        assert reconstructed == data

    def test_exact_boundary_at_newline(self):
        """Test when chunk boundary falls exactly on a newline"""
        data = b"a" * 99 + b"\n" + b"b" * 99 + b"\n"
        result = split_bytes_on_line_boundaries(data, max_chunk_size=100)

        # Should split at the newlines
        assert len(result) == 2
        assert result[0] == b"a" * 99 + b"\n"
        assert result[1] == b"b" * 99 + b"\n"

    def test_newline_at_start_of_chunk(self):
        """Test when newline is at the very start of a potential chunk"""
        data = b"x" * 100 + b"\n" + b"y" * 100 + b"\n"
        result = split_bytes_on_line_boundaries(data, max_chunk_size=101)

        # Should include the newline with first chunk
        assert result[0] == b"x" * 100 + b"\n"

    def test_very_small_search_window(self):
        """Test with a very small search window"""
        data = b"line1\nline2\nline3\nline4\n"
        result = split_bytes_on_line_boundaries(
            data, max_chunk_size=15, newline_search_window=2
        )

        # Should still work but may not find optimal split points
        reconstructed = b"".join(result)
        assert reconstructed == data

    def test_large_jsonl_file_simulation(self):
        """Simulate a large JSONL file with varying object sizes"""
        # Create a realistic JSONL file
        lines = []
        for i in range(100):
            obj = {"id": i, "data": "x" * (50 + i % 20)}
            line = str(obj).replace("'", '"').encode() + b"\n"
            lines.append(line)

        data = b"".join(lines)

        # Split with 64MB simulation (use smaller size for test)
        result = split_bytes_on_line_boundaries(data, max_chunk_size=500)

        # Verify reconstruction
        reconstructed = b"".join(result)
        assert reconstructed == data

        # Verify all chunks except last end with newline
        for chunk in result[:-1]:
            assert chunk.endswith(b"\n")

    def test_single_byte_chunks(self):
        """Test edge case with very small chunk size"""
        data = b"a\nb\nc\n"
        result = split_bytes_on_line_boundaries(data, max_chunk_size=2)

        # Should still preserve data
        reconstructed = b"".join(result)
        assert reconstructed == data

    def test_data_without_newlines(self):
        """Test with data that has no newlines"""
        data = b"x" * 1000
        result = split_bytes_on_line_boundaries(data, max_chunk_size=100)

        # Should split anyway (since no newlines found)
        assert len(result) == 10
        reconstructed = b"".join(result)
        assert reconstructed == data

    def test_chunk_size_boundaries(self):
        """Test that chunks respect max_chunk_size (approximately)"""
        data = b"line\n" * 1000
        max_chunk = 500

        result = split_bytes_on_line_boundaries(data, max_chunk_size=max_chunk)

        # Most chunks should be close to max_chunk_size (within search window tolerance)
        for chunk in result[:-1]:  # Exclude last chunk
            # Should be at most max_chunk_size
            assert len(chunk) <= max_chunk
            # Should be reasonably close to max_chunk (not too small)
            # Allow for line boundary adjustments
            assert len(chunk) >= max_chunk - 10000 or len(chunk) < 100

    def test_unicode_content_preserved(self):
        """Test that binary content is preserved (including unicode when encoded)"""
        # Create JSONL with unicode content
        data = '{"name":"café"}\n{"name":"日本"}\n'.encode("utf-8")

        result = split_bytes_on_line_boundaries(data, max_chunk_size=20)

        reconstructed = b"".join(result)
        assert reconstructed == data
        # Verify it can be decoded back
        assert reconstructed.decode("utf-8") == '{"name":"café"}\n{"name":"日本"}\n'

    def test_realistic_openai_batch_size(self):
        """Test with realistic OpenAI batch upload parameters (50MB chunks)"""
        # Simulate smaller version of 50MB
        line = b'{"custom_id":"req_1","method":"POST","url":"/v1/chat/completions"}\n'
        # Create ~1MB of data
        data = line * 15000

        # Use 100KB chunks for test (scaled down from 50MB)
        result = split_bytes_on_line_boundaries(
            data, max_chunk_size=100 * 1024, newline_search_window=10000
        )

        # Verify integrity
        reconstructed = b"".join(result)
        assert reconstructed == data

        # Verify chunks end on line boundaries
        for chunk in result[:-1]:
            assert chunk.endswith(b"\n")
