import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from main import main


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def test_cli_invalid_directory():
    with pytest.raises(SystemExit) as exc_info:
        with patch("sys.argv", ["duplicate-finder", "/nonexistent/dir"]):
            main()
    assert exc_info.value.code == 1


def test_cli_invalid_size_format():
    with pytest.raises(SystemExit) as exc_info:
        with patch("sys.argv", ["duplicate-finder", ".", "-m", "invalid"]):
            main()
    assert exc_info.value.code == 1


def test_cli_with_options(temp_dir, capsys):
    # Create test files
    (temp_dir / "file1.txt").write_text("content")
    (temp_dir / "file2.txt").write_text("content")
    (temp_dir / "unique.txt").write_text("unique")

    # Create excluded directory with duplicate
    excluded_dir = temp_dir / "excluded"
    excluded_dir.mkdir()
    (excluded_dir / "excluded.txt").write_text("content")

    # Test with various CLI options
    with patch(
        "sys.argv",
        [
            "duplicate-finder",
            str(temp_dir),
            "-e",
            str(excluded_dir),
            "-x",
            ".log",
            "-m",
            "1B",
            "-v",
        ],
    ):
        main()

    captured = capsys.readouterr()
    assert "Duplicate files:" in captured.out
    assert str(temp_dir / "file1.txt") in captured.out
    assert str(temp_dir / "file2.txt") in captured.out
    assert str(excluded_dir / "excluded.txt") not in captured.out


def test_cli_export_results(temp_dir):
    # Create duplicate files
    (temp_dir / "dup1.txt").write_text("duplicate")
    (temp_dir / "dup2.txt").write_text("duplicate")

    output_file = temp_dir / "duplicates.txt"

    # Test export functionality
    with patch("sys.argv", ["duplicate-finder", str(temp_dir), "-o", str(output_file)]):
        main()

    assert output_file.exists()
    content = output_file.read_text()
    assert "Duplicate set" in content
    assert "dup1.txt" in content
    assert "dup2.txt" in content
