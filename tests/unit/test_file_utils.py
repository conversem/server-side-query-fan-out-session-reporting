"""
Unit tests for file_utils.open_file_auto_decompress function.

Tests cover:
- Plain text files
- Gzip files with .gz extension
- Gzip files detected by magic bytes (no .gz extension)
- FileNotFoundError handling
- BadGzipFile for corrupt gzip
"""

import gzip
import tempfile
from pathlib import Path

import pytest

from llm_bot_pipeline.ingestion.file_utils import open_file_auto_decompress


class TestOpenFileAutoDecompress:
    """Tests for open_file_auto_decompress function."""

    def test_plain_text_file(self, tmp_path: Path) -> None:
        """Test reading a plain text file."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello, World!\nLine 2")

        with open_file_auto_decompress(test_file) as f:
            content = f.read()

        assert content == "Hello, World!\nLine 2"

    def test_gzip_file_with_extension(self, tmp_path: Path) -> None:
        """Test reading a gzip file with .gz extension."""
        test_file = tmp_path / "test.txt.gz"
        with gzip.open(test_file, "wt", encoding="utf-8") as f:
            f.write("Compressed content\nLine 2")

        with open_file_auto_decompress(test_file) as f:
            content = f.read()

        assert content == "Compressed content\nLine 2"

    def test_gzip_file_magic_bytes_no_extension(self, tmp_path: Path) -> None:
        """Test reading a gzip file detected by magic bytes (no .gz extension)."""
        # Create a gzip file without .gz extension
        test_file = tmp_path / "test.log"
        with gzip.open(test_file, "wt", encoding="utf-8") as f:
            f.write("Magic bytes detection")

        with open_file_auto_decompress(test_file) as f:
            content = f.read()

        assert content == "Magic bytes detection"

    def test_file_not_found(self, tmp_path: Path) -> None:
        """Test FileNotFoundError for non-existent file."""
        non_existent = tmp_path / "does_not_exist.txt"

        with pytest.raises(FileNotFoundError) as exc_info:
            open_file_auto_decompress(non_existent)

        assert "File not found" in str(exc_info.value)

    def test_bad_gzip_file(self, tmp_path: Path) -> None:
        """Test BadGzipFile for corrupt gzip file with .gz extension."""
        test_file = tmp_path / "corrupt.txt.gz"
        # Write non-gzip content to a .gz file
        test_file.write_bytes(b"This is not gzip content")

        with pytest.raises(gzip.BadGzipFile):
            with open_file_auto_decompress(test_file) as f:
                f.read()

    def test_json_file(self, tmp_path: Path) -> None:
        """Test reading a JSON file."""
        test_file = tmp_path / "data.json"
        test_file.write_text('{"key": "value"}')

        with open_file_auto_decompress(test_file) as f:
            content = f.read()

        assert content == '{"key": "value"}'

    def test_ndjson_gzip_file(self, tmp_path: Path) -> None:
        """Test reading a gzipped NDJSON file."""
        test_file = tmp_path / "logs.ndjson.gz"
        ndjson_content = '{"line": 1}\n{"line": 2}\n{"line": 3}'
        with gzip.open(test_file, "wt", encoding="utf-8") as f:
            f.write(ndjson_content)

        with open_file_auto_decompress(test_file) as f:
            content = f.read()

        assert content == ndjson_content

    def test_path_as_string(self, tmp_path: Path) -> None:
        """Test that function accepts string paths."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("String path test")

        with open_file_auto_decompress(str(test_file)) as f:
            content = f.read()

        assert content == "String path test"

    def test_custom_encoding(self, tmp_path: Path) -> None:
        """Test reading file with custom encoding."""
        test_file = tmp_path / "latin1.txt"
        # Write with latin-1 encoding
        test_file.write_bytes("Café résumé".encode("latin-1"))

        with open_file_auto_decompress(test_file, encoding="latin-1") as f:
            content = f.read()

        assert content == "Café résumé"

    def test_empty_file(self, tmp_path: Path) -> None:
        """Test reading an empty file."""
        test_file = tmp_path / "empty.txt"
        test_file.write_text("")

        with open_file_auto_decompress(test_file) as f:
            content = f.read()

        assert content == ""

    def test_empty_gzip_file(self, tmp_path: Path) -> None:
        """Test reading an empty gzip file."""
        test_file = tmp_path / "empty.txt.gz"
        with gzip.open(test_file, "wt", encoding="utf-8") as f:
            f.write("")

        with open_file_auto_decompress(test_file) as f:
            content = f.read()

        assert content == ""

    def test_large_gzip_file(self, tmp_path: Path) -> None:
        """Test reading a larger gzip file."""
        test_file = tmp_path / "large.log.gz"
        # Create 10000 lines
        lines = [f'{{"line": {i}, "data": "test data"}}' for i in range(10000)]
        content = "\n".join(lines)

        with gzip.open(test_file, "wt", encoding="utf-8") as f:
            f.write(content)

        with open_file_auto_decompress(test_file) as f:
            read_content = f.read()

        assert read_content == content
        assert read_content.count("\n") == 9999  # 10000 lines = 9999 newlines
