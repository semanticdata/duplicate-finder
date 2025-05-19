# This script finds and manages duplicate files in a directory.
# It identifies duplicates using MD5 hashing and supports:
# - Parallel processing for improved performance
# - Multiple output formats (txt, json, csv)
# - Size-based filtering
# - Directory and extension exclusions
# - Dry run capability

import argparse
import csv
import hashlib
import json
import multiprocessing
import os
import sys
import time
from collections import defaultdict
from multiprocessing import Pool
from typing import Dict, List, Optional, Tuple

import humanize


def calculate_file_hash(filepath: str, block_size: int = 65536) -> str:
    """Calculate MD5 hash of a file using block-wise reading.

    Args:
        filepath: Path to the file to hash
        block_size: Size of blocks to read, defaults to 64KB

    Returns:
        str: Hexadecimal MD5 hash of the file

    Raises:
        IOError: If file cannot be read
        OSError: If file access fails
    """
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        while True:
            data = f.read(block_size)
            if not data:
                break
            md5.update(data)
    return md5.hexdigest()


def process_file(file_info: Tuple[str, int, int]) -> Optional[Tuple[str, str, int]]:
    """Process a single file by calculating its hash if it meets size criteria.

    Args:
        file_info: Tuple containing (filepath, minimum_size, block_size)

    Returns:
        Optional[Tuple[str, str, int]]: Tuple of (hash, filepath, size) if file meets criteria,
                                      None if file is too small or cannot be processed

    Note:
        Returns None if file size is less than minimum_size or if file cannot be accessed
    """
    filepath, min_size, block_size = file_info
    try:
        file_size = os.path.getsize(filepath)
        if file_size < min_size:
            return None

        file_hash = calculate_file_hash(filepath, block_size)
        return (file_hash, filepath, file_size)
    except (IOError, OSError):
        return None


def find_duplicates(
    directory: str,
    exclude_dirs: List[str] = None,
    exclude_extensions: List[str] = None,
    min_size: int = 0,
    verbose: bool = False,
    ignore_dot_dirs: bool = True,
) -> Tuple[Dict[str, List[str]], int, int, int]:
    """Find duplicate files in the given directory using parallel processing.

    Args:
        directory: Root directory to scan
        exclude_dirs: List of directory paths to exclude from scan
        exclude_extensions: List of file extensions to exclude (e.g., ['.log', '.tmp'])
        min_size: Minimum file size in bytes to consider
        verbose: Whether to print progress information
        ignore_dot_dirs: Whether to skip directories starting with a dot

    Returns:
        Tuple containing:
        - Dict[str, List[str]]: Dictionary mapping file hash to list of duplicate file paths
        - int: Total size of all processed files in bytes
        - int: Total size taken by duplicate files in bytes
        - int: Number of files processed

    Note:
        Paths in exclude_dirs are normalized to absolute paths for comparison
        File extensions in exclude_extensions are converted to lowercase
    """
    hash_map: Dict[str, List[str]] = defaultdict(list)
    total_size = 0
    files_processed = 0

    # Normalize exclude directories to absolute paths
    exclude_dirs = exclude_dirs or []
    exclude_dirs = [os.path.abspath(d) for d in exclude_dirs]

    # Normalize exclude extensions
    exclude_extensions = exclude_extensions or []
    exclude_extensions = [ext.lower() for ext in exclude_extensions]

    # Collect all files to process
    files_to_process = []
    for root, dirs, files in os.walk(directory):
        if any(os.path.abspath(root).startswith(d) for d in exclude_dirs):
            if verbose:
                print(f"Skipping excluded directory: {root}")
            continue

        if ignore_dot_dirs:
            dirs[:] = [d for d in dirs if not d.startswith(".")]

        for filename in files:
            if exclude_extensions and any(
                filename.lower().endswith(ext) for ext in exclude_extensions
            ):
                if verbose:
                    print(
                        f"Skipping excluded file type: {os.path.join(root, filename)}"
                    )
                continue

            filepath = os.path.join(root, filename)
            files_to_process.append((filepath, min_size, 65536))

    # Process files in parallel
    cpu_count = multiprocessing.cpu_count()
    with Pool(processes=cpu_count) as pool:
        if verbose:
            print(f"Processing files using {cpu_count} CPU cores...")

        results = pool.imap_unordered(process_file, files_to_process)

        for result in results:
            if result:
                file_hash, filepath, file_size = result
                hash_map[file_hash].append(filepath)
                total_size += file_size
                files_processed += 1

                if verbose and files_processed % 100 == 0:
                    print(f"Processed {files_processed} files...")

    # Filter out unique files and calculate duplicate size
    duplicate_files = {h: files for h, files in hash_map.items() if len(files) > 1}
    duplicate_size = sum(
        os.path.getsize(files[0]) * (len(files) - 1)
        for files in duplicate_files.values()
    )

    return duplicate_files, total_size, duplicate_size, files_processed


def export_to_file(
    duplicates: Dict[str, List[str]], output_file: str, format: str = "txt"
) -> None:
    """Export duplicate files list to a file in the specified format.

    Args:
        duplicates: Dictionary mapping file hash to list of duplicate file paths
        output_file: Path where the output file will be created
        format: Output format, one of 'txt', 'json', or 'csv'
            - txt: Human-readable text format with duplicate sets
            - json: Structured JSON with duplicate sets and file sizes
            - csv: Tabular format with set number, size, and file path

    Note:
        The JSON format includes file sizes and groups duplicates into sets
        The CSV format assigns a sequential number to each set of duplicates
        The text format includes human-readable file sizes
    """
    if format == "json":
        # Convert to a more JSON-friendly format
        json_data = {
            "duplicate_sets": [
                {"size": os.path.getsize(files[0]), "files": files}
                for files in duplicates.values()
            ]
        }
        with open(output_file, "w") as f:
            json.dump(json_data, f, indent=2)

    elif format == "csv":
        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Set", "Size", "File"])
            for i, (_, files) in enumerate(duplicates.items(), 1):
                size = os.path.getsize(files[0])
                for filepath in files:
                    writer.writerow([i, size, filepath])

    else:  # txt format
        with open(output_file, "w") as f:
            for hash_value, file_list in duplicates.items():
                f.write(
                    f"\nDuplicate set (size: {humanize.naturalsize(os.path.getsize(file_list[0]))})\n"
                )
                for filepath in file_list:
                    f.write(f"  {filepath}\n")

    print(f"Results exported to {output_file} in {format} format")


def main() -> None:
    """Main entry point for the duplicate file finder.

    Processes command line arguments and orchestrates the duplicate finding process.
    Supports various options including:
    - Directory exclusions (-e/--exclude-dir)
    - Extension exclusions (-x/--exclude-ext)
    - Minimum file size (-m/--min-size)
    - Output file and format (-o/--output, --format)
    - Dry run mode (--dry-run)
    - Verbose output (-v/--verbose)
    - Dot directory inclusion (--include-dot-dirs)

    Returns:
        None

    Exit codes:
        0: Success
        1: Invalid directory or size format
    """
    parser = argparse.ArgumentParser(description="Find duplicate files in a directory")
    parser.add_argument("directory", help="Directory to scan for duplicates")
    parser.add_argument(
        "-e",
        "--exclude-dir",
        action="append",
        help="Directories to exclude (can be used multiple times)",
    )
    parser.add_argument(
        "-x",
        "--exclude-ext",
        action="append",
        help="File extensions to exclude (can be used multiple times)",
    )
    parser.add_argument(
        "-m",
        "--min-size",
        type=str,
        default="0B",
        help="Minimum file size to consider (e.g. 10KB, 5MB)",
    )
    parser.add_argument(
        "-o",
        "--output",
        nargs="?",
        const="duplicates.txt",
        help="Export results to a file (defaults to 'duplicates.txt' if flag is used without argument)",
    )
    parser.add_argument(
        "--format",
        choices=["txt", "json", "csv"],
        default="txt",
        help="Output format (txt, json, or csv)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be found without processing files",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Show verbose output"
    )
    parser.add_argument(
        "--include-dot-dirs",
        action="store_true",
        help="Include directories that start with a dot (like .git)",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.directory):
        print(f"Error: '{args.directory}' is not a valid directory", file=sys.stderr)
        sys.exit(1)

    # Convert human-readable size to bytes
    try:
        # Handle special case for 0B
        if args.min_size.upper() == "0B":
            min_size = 0
        else:
            # Handle KB, MB, GB, etc.
            size_str = args.min_size.upper()
            if size_str.endswith("B"):
                # Handle plain bytes (e.g., "1B", "100B")
                if size_str[:-1].isdigit():
                    min_size = int(size_str[:-1])
                else:
                    # Handle KB, MB, GB, TB
                    if len(size_str) > 2 and not size_str[-2].isdigit():
                        size_str = size_str[:-1]  # Remove 'B' for unit processing
                        units = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
                        if size_str[-1] in units and size_str[:-1].isdigit():
                            min_size = int(size_str[:-1]) * units[size_str[-1]]
                        else:
                            raise ValueError(f"Invalid size format: {args.min_size}")
                    else:
                        raise ValueError(f"Invalid size format: {args.min_size}")
            elif size_str.isdigit():
                min_size = int(size_str)
            else:
                raise ValueError(f"Invalid size format: {args.min_size}")
    except Exception:
        print(f"Error: Invalid minimum size format: {args.min_size}", file=sys.stderr)
        print("Please use formats like: 10KB, 5MB, 1GB", file=sys.stderr)
        sys.exit(1)

    print(f"\nScanning directory: {args.directory}")
    if args.exclude_dir:
        print(f"Excluding directories: {', '.join(args.exclude_dir)}")
    if args.exclude_ext:
        print(f"Excluding file extensions: {', '.join(args.exclude_ext)}")
    if min_size > 0:
        print(
            f"Minimum file size: {args.min_size}"
        )  # Use original input format instead of humanize

    if args.dry_run:
        print("\nDRY RUN - showing what would be scanned without processing files")
        return

    print("This might take a while depending on the number and size of files...\n")

    start_time = time.time()
    duplicates, total_size, duplicate_size, files_processed = find_duplicates(
        args.directory,
        exclude_dirs=args.exclude_dir,
        exclude_extensions=args.exclude_ext,
        min_size=min_size,
        verbose=args.verbose,
        ignore_dot_dirs=not args.include_dot_dirs,  # Pass the inverse of include_dot_dirs
    )
    elapsed_time = time.time() - start_time

    if not duplicates:
        print("No duplicate files found.")
        return

    # Print results with improved formatting
    duplicate_count = sum(len(files) for files in duplicates.values()) - len(duplicates)
    print(f"Scan completed in {elapsed_time:.2f} seconds")
    print(f"Files processed: {files_processed}")
    print(f"Found {duplicate_count} duplicate files in {len(duplicates)} sets")
    print(f"Total space used: {humanize.naturalsize(total_size)}")
    print(f"Space taken by duplicates: {humanize.naturalsize(duplicate_size)}")
    print(f"Potential space savings: {humanize.naturalsize(duplicate_size)}\n")

    print("Duplicate files:")
    for hash_value, file_list in duplicates.items():
        print(
            f"\nDuplicate set (size: {humanize.naturalsize(os.path.getsize(file_list[0]))})"
        )
        for filepath in file_list:
            print(f"  {filepath}")

    # Export results if requested
    if args.output:
        export_to_file(duplicates, args.output, args.format)


if __name__ == "__main__":
    main()
