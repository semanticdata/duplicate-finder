import tempfile
from pathlib import Path

import pytest

from main import FileProcessor


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files.

    Returns:
        Path: A pathlib.Path object pointing to the temporary directory.
        The directory is automatically cleaned up after each test.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def create_test_file(path: Path, content: str) -> None:
    """Create a test file with specified content.

    Args:
        path: The path where the file should be created.
        content: The content to write to the file.
    """
    path.write_text(content)


def test_calculate_file_hash(temp_dir):
    """Test the file hash calculation functionality.

    Verifies that:
    1. The same file produces consistent hash values
    2. Modifying file content changes the hash value

    Args:
        temp_dir: Pytest fixture providing a temporary directory
    """
    # Create a test file with known content
    test_file = temp_dir / "test.txt"
    test_content = "Hello, World!"
    create_test_file(test_file, test_content)

    # Calculate hash and verify it's consistent
    processor = FileProcessor()
    hash1 = processor.calculate_file_hash(str(test_file))
    hash2 = processor.calculate_file_hash(str(test_file))
    assert hash1 == hash2

    # Modify content and verify hash changes
    create_test_file(test_file, test_content + "!")
    hash3 = processor.calculate_file_hash(str(test_file))
    assert hash1 != hash3


def test_process_file(temp_dir):
    """Test the file processing functionality with size filtering.

    Verifies that:
    1. Files smaller than minimum size are filtered out (return None)
    2. Files larger than minimum size are processed correctly
    3. The returned tuple contains correct types (str, str, int)

    Args:
        temp_dir: Pytest fixture providing a temporary directory
    """
    processor = FileProcessor()
    # Test with file smaller than min_size
    small_file = temp_dir / "small.txt"
    create_test_file(small_file, "small")
    result = processor.process_file((str(small_file), 100))
    assert result is None

    # Test with file larger than min_size
    large_file = temp_dir / "large.txt"
    create_test_file(large_file, "large" * 100)
    result = processor.process_file((str(large_file), 10))
    assert result is not None
    assert len(result) == 3
    assert isinstance(result[0], str)  # hash
    assert isinstance(result[1], str)  # path
    assert isinstance(result[2], int)  # size


def test_find_duplicates(temp_dir):
    """Test the duplicate file finding functionality.

    Verifies that the function correctly:
    1. Identifies sets of duplicate files
    2. Excludes files in specified directories
    3. Respects file extension exclusions
    4. Handles minimum size filtering
    5. Calculates correct file and duplicate size statistics

    Args:
        temp_dir: Pytest fixture providing a temporary directory
    """
    processor = FileProcessor()
    # Create unique files
    create_test_file(temp_dir / "unique1.txt", "content1")
    create_test_file(temp_dir / "unique2.txt", "content2")

    # Create duplicate files
    create_test_file(temp_dir / "dup1.txt", "duplicate")
    create_test_file(temp_dir / "dup2.txt", "duplicate")
    create_test_file(temp_dir / "dup3.txt", "duplicate")

    # Create excluded directory and file
    excluded_dir = temp_dir / "excluded"
    excluded_dir.mkdir()
    create_test_file(excluded_dir / "excluded.txt", "duplicate")

    # Test basic duplicate finding
    duplicates, total_size, duplicate_size, files_processed = processor.find_duplicates(
        str(temp_dir),
        exclude_dirs=[str(excluded_dir)],
        exclude_extensions=[".log"],
        min_size=0,
        verbose=True,
    )

    assert len(duplicates) == 1  # One set of duplicates
    assert files_processed == 5  # 5 files (excluding the one in excluded dir)
    assert duplicate_size > 0
    assert total_size > duplicate_size

    # Verify the duplicate set contains exactly 3 files
    duplicate_set = next(iter(duplicates.values()))
    assert len(duplicate_set) == 3

    # Test with minimum size filter
    duplicates, _, _, _ = processor.find_duplicates(
        str(temp_dir),
        min_size=1000000,  # 1MB
    )
    assert len(duplicates) == 0  # No files are this large
