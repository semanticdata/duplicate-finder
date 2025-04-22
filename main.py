# This script is used to find duplicate files in a directory.
# It uses the MD5 hash of the file to determine if it is a duplicate.
# It then prints the duplicate files and the total size of the duplicate files.
# It also prints the potential space savings if the duplicate files are removed.

import os
import hashlib
from collections import defaultdict
from pathlib import Path
import argparse
import sys
from typing import Dict, List, Set, Tuple, Optional
import humanize
import time


def calculate_file_hash(filepath: str, block_size: int = 65536) -> str:
    """Calculate MD5 hash of a file."""
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        while True:
            data = f.read(block_size)
            if not data:
                break
            md5.update(data)
    return md5.hexdigest()


def find_duplicates(
    directory: str,
    exclude_dirs: List[str] = None,
    exclude_extensions: List[str] = None,
    min_size: int = 0,
    verbose: bool = False,
    ignore_dot_dirs: bool = True,
) -> Tuple[Dict[str, List[str]], int, int, int]:
    """Find duplicate files in the given directory."""
    hash_map: Dict[str, List[str]] = defaultdict(list)
    total_size = 0
    duplicate_size = 0
    files_processed = 0
    start_time = time.time()

    # Normalize exclude directories to absolute paths
    exclude_dirs = exclude_dirs or []
    exclude_dirs = [os.path.abspath(d) for d in exclude_dirs]

    # Normalize exclude extensions
    exclude_extensions = exclude_extensions or []
    exclude_extensions = [ext.lower() for ext in exclude_extensions]

    # Walk through the directory
    for root, dirs, files in os.walk(directory):
        # Skip excluded directories
        if any(os.path.abspath(root).startswith(d) for d in exclude_dirs):
            if verbose:
                print(f"Skipping excluded directory: {root}")
            continue

        # Skip directories that start with a dot if ignore_dot_dirs is True
        if ignore_dot_dirs:
            # Modify dirs in-place to prevent os.walk from traversing dot directories
            dirs[:] = [d for d in dirs if not d.startswith(".")]

        for filename in files:
            filepath = os.path.join(root, filename)

            # Skip files with excluded extensions
            if exclude_extensions and any(
                filename.lower().endswith(ext) for ext in exclude_extensions
            ):
                if verbose:
                    print(f"Skipping excluded file type: {filepath}")
                continue

            try:
                file_size = os.path.getsize(filepath)

                # Skip files smaller than min_size
                if file_size < min_size:
                    if verbose:
                        print(
                            f"Skipping file smaller than {humanize.naturalsize(min_size)}: {filepath}"
                        )
                    continue

                files_processed += 1
                if verbose and files_processed % 100 == 0:
                    elapsed = time.time() - start_time
                    print(
                        f"Processed {files_processed} files in {elapsed:.2f} seconds..."
                    )

                file_hash = calculate_file_hash(filepath)
                total_size += file_size
                hash_map[file_hash].append(filepath)
            except (IOError, OSError) as e:
                print(f"Error processing {filepath}: {e}", file=sys.stderr)

    # Filter out unique files and calculate duplicate size
    duplicate_files = {h: files for h, files in hash_map.items() if len(files) > 1}
    for _, files in duplicate_files.items():
        if files:
            duplicate_size += os.path.getsize(files[0]) * (len(files) - 1)

    return duplicate_files, total_size, duplicate_size, files_processed


def export_to_file(duplicates: Dict[str, List[str]], output_file: str) -> None:
    """Export duplicate files list to a text file."""
    with open(output_file, "w") as f:
        for hash_value, file_list in duplicates.items():
            f.write(
                f"\nDuplicate set (size: {humanize.naturalsize(os.path.getsize(file_list[0]))})\n"
            )
            for filepath in file_list:
                f.write(f"  {filepath}\n")
    print(f"Results exported to {output_file}")


def main() -> None:
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
            if (
                size_str.endswith("B")
                and len(size_str) > 1
                and not size_str[-2].isdigit()
            ):
                size_str = size_str[:-1]

            if size_str.isdigit():
                min_size = int(size_str)
            else:
                units = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
                if size_str[-1] in units and size_str[:-1].isdigit():
                    min_size = int(size_str[:-1]) * units[size_str[-1]]
                else:
                    raise ValueError(f"Invalid size format: {args.min_size}")
    except Exception as e:
        print(f"Error: Invalid minimum size format: {args.min_size}", file=sys.stderr)
        print("Please use formats like: 10KB, 5MB, 1GB", file=sys.stderr)
        sys.exit(1)

    print(f"\nScanning directory: {args.directory}")
    if args.exclude_dir:
        print(f"Excluding directories: {', '.join(args.exclude_dir)}")
    if args.exclude_ext:
        print(f"Excluding file extensions: {', '.join(args.exclude_ext)}")
    if min_size > 0:
        print(f"Minimum file size: {humanize.naturalsize(min_size)}")
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
        export_to_file(duplicates, args.output)


if __name__ == "__main__":
    main()
