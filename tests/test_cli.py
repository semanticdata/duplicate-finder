import csv
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from main import main


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files.

    Returns:
        Path: A pathlib.Path object pointing to the temporary directory.
        The directory is automatically cleaned up after each test.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def test_cli_invalid_directory():
    """Test the CLI behavior when given a non-existent directory.

    Verifies that the program exits with status code 1 when
    provided with a directory path that doesn't exist.
    """
    with pytest.raises(SystemExit) as exc_info:
        with patch("sys.argv", ["duplicate-finder", "/nonexistent/dir"]):
            main()
    assert exc_info.value.code == 1


def test_cli_invalid_size_format():
    """Test the CLI behavior when given an invalid minimum size format.

    Verifies that the program exits with status code 1 when
    the -m/--min-size argument is not in the correct format.
    """
    with pytest.raises(SystemExit) as exc_info:
        with patch("sys.argv", ["duplicate-finder", ".", "-m", "invalid"]):
            main()
    assert exc_info.value.code == 1


def test_cli_with_options(temp_dir, capsys):
    """Test the CLI with various command-line options.

    Creates a test environment with duplicate and unique files,
    then verifies that the program correctly:
    - Identifies duplicate files
    - Respects directory exclusions
    - Handles file extensions exclusions
    - Processes minimum size requirements
    - Handles verbose output

    Args:
        temp_dir: Pytest fixture providing a temporary directory
        capsys: Pytest fixture for capturing stdout/stderr
    """
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

    # Verify output contains expected duplicate files
    captured = capsys.readouterr()
    # Check for the new rich-formatted output
    assert "Duplicate set" in captured.out
    assert str(temp_dir / "file1.txt") in captured.out
    assert str(temp_dir / "file2.txt") in captured.out
    assert str(excluded_dir / "excluded.txt") not in captured.out  # Verify excluded dir is respected


def test_cli_export_results(temp_dir):
    """Test the export functionality of duplicate finder.

    Verifies that the program can correctly export duplicate file
    information to a text file, including:
    - Creating the output file
    - Writing duplicate set information
    - Including all duplicate file paths

    Args:
        temp_dir: Pytest fixture providing a temporary directory
    """
    # Create duplicate files
    (temp_dir / "dup1.txt").write_text("duplicate")
    (temp_dir / "dup2.txt").write_text("duplicate")

    output_file = temp_dir / "duplicates.txt"

    # Test export functionality
    with patch("sys.argv", ["duplicate-finder", str(temp_dir), "-o", str(output_file)]):
        main()

    # Verify export file contents
    assert output_file.exists()
    content = output_file.read_text()
    assert "Duplicate set" in content
    assert "dup1.txt" in content
    assert "dup2.txt" in content


def test_cli_dry_run(temp_dir, capsys):
    """Test the dry run functionality.

    Verifies that the program correctly shows what would be scanned
    without actually processing any files.

    Args:
        temp_dir: Pytest fixture providing a temporary directory
        capsys: Pytest fixture for capturing stdout/stderr
    """
    with patch(
        "sys.argv",
        [
            "duplicate-finder",
            str(temp_dir),
            "--dry-run",
            "-e",
            "excluded",
            "-x",
            ".log",
            "-m",
            "1MB",
        ],
    ):
        main()

    captured = capsys.readouterr()
    assert "DRY RUN" in captured.out
    assert str(temp_dir) in captured.out
    assert "excluded" in captured.out
    assert ".log" in captured.out
    assert "1MB" in captured.out


def test_cli_export_formats(temp_dir):
    """Test exporting results in different formats.

    Verifies that the program can correctly export duplicate file
    information in txt, json, and csv formats.

    Args:
        temp_dir: Pytest fixture providing a temporary directory
    """
    # Create duplicate files
    (temp_dir / "dup1.txt").write_text("duplicate")
    (temp_dir / "dup2.txt").write_text("duplicate")

    # Test JSON export
    json_file = temp_dir / "duplicates.json"
    with patch(
        "sys.argv",
        ["duplicate-finder", str(temp_dir), "-o", str(json_file), "--format", "json"],
    ):
        main()

    assert json_file.exists()
    with open(json_file) as f:
        json_data = json.load(f)
    assert "duplicate_sets" in json_data
    assert len(json_data["duplicate_sets"]) > 0

    # Test CSV export
    csv_file = temp_dir / "duplicates.csv"
    with patch(
        "sys.argv",
        ["duplicate-finder", str(temp_dir), "-o", str(csv_file), "--format", "csv"],
    ):
        main()

    assert csv_file.exists()
    with open(csv_file) as f:
        csv_reader = csv.reader(f)
        header = next(csv_reader)
        assert header == ["Set", "Size", "File"]
        rows = list(csv_reader)
        assert len(rows) > 0
