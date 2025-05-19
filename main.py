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
import logging
import multiprocessing
import os
import sys
import time
from collections import defaultdict
from multiprocessing import Pool
from typing import Dict, List, Optional, Tuple

import humanize
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
)
from rich.table import Table


class SizeParser:
    """Utility class for parsing human-readable file sizes."""

    UNITS = {
        "K": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
    }

    @staticmethod
    def parse_size(size_str: str) -> int:
        """Convert human-readable size string to bytes.

        Args:
            size_str: Size string (e.g., '10KB', '5MB', '1GB')

        Returns:
            int: Size in bytes

        Raises:
            ValueError: If size format is invalid
        """
        size_str = size_str.upper()

        # Handle special case for 0B
        if size_str == "0B":
            return 0

        # Handle plain bytes or numeric value
        if size_str.isdigit():
            return int(size_str)

        if size_str.endswith("B"):
            # Handle plain bytes (e.g., "1B", "100B")
            if size_str[:-1].isdigit():
                return int(size_str[:-1])

            # Handle KB, MB, GB, TB
            if len(size_str) > 2 and not size_str[-2].isdigit():
                size_str = size_str[:-1]  # Remove 'B' for unit processing
                unit = size_str[-1]
                if unit in SizeParser.UNITS and size_str[:-1].isdigit():
                    return int(size_str[:-1]) * SizeParser.UNITS[unit]

        raise ValueError(f"Invalid size format: {size_str}")


class FileProcessor:
    """Handles file processing operations for duplicate detection."""

    def __init__(self, block_size: int = 65536):
        self.block_size = block_size
        self.logger = logging.getLogger(__name__)

    def calculate_file_hash(self, filepath: str) -> str:
        """Calculate MD5 hash of a file using block-wise reading.

        Args:
            filepath: Path to the file to hash

        Returns:
            str: Hexadecimal MD5 hash of the file

        Raises:
            IOError: If file cannot be read
            OSError: If file access fails
        """
        md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            while True:
                data = f.read(self.block_size)
                if not data:
                    break
                md5.update(data)
        return md5.hexdigest()

    def process_file(
        self, file_info: Tuple[str, int]
    ) -> Optional[Tuple[str, str, int]]:
        """Process a single file by calculating its hash if it meets size criteria.

        Args:
            file_info: Tuple containing (filepath, minimum_size)

        Returns:
            Optional[Tuple[str, str, int]]: Tuple of (hash, filepath, size) if file meets criteria,
                                          None if file is too small or cannot be processed
        """
        filepath, min_size = file_info
        try:
            file_size = os.path.getsize(filepath)
            if file_size < min_size:
                self.logger.debug(
                    f"Skipping {filepath} (size: {file_size} < {min_size})"
                )
                return None

            file_hash = self.calculate_file_hash(filepath)
            return (file_hash, filepath, file_size)
        except (IOError, OSError) as e:
            self.logger.error(f"Error processing {filepath}: {str(e)}")
            return None

    def find_duplicates(
        self,
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
            verbose: Whether to enable verbose logging
            ignore_dot_dirs: Whether to skip directories starting with a dot

        Returns:
            Tuple containing:
            - Dict[str, List[str]]: Dictionary mapping file hash to list of duplicate file paths
            - int: Total size of all processed files in bytes
            - int: Total size taken by duplicate files in bytes
            - int: Number of files processed
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
                self.logger.info(f"Skipping excluded directory: {root}")
                continue

            if ignore_dot_dirs:
                dirs[:] = [d for d in dirs if not d.startswith(".")]

            for filename in files:
                if exclude_extensions and any(
                    filename.lower().endswith(ext) for ext in exclude_extensions
                ):
                    self.logger.info(
                        f"Skipping excluded file type: {os.path.join(root, filename)}"
                    )
                    continue

                filepath = os.path.join(root, filename)
                files_to_process.append((filepath, min_size))

        # Process files in parallel
        cpu_count = multiprocessing.cpu_count()
        self.logger.info(f"Processing files using {cpu_count} CPU cores...")

        with Pool(processes=cpu_count) as pool:
            results = pool.imap_unordered(self.process_file, files_to_process)

            for result in results:
                if result:
                    file_hash, filepath, file_size = result
                    hash_map[file_hash].append(filepath)
                    total_size += file_size
                    files_processed += 1

                    if verbose and files_processed % 100 == 0:
                        self.logger.info(f"Processed {files_processed} files...")

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
    """
    logger = logging.getLogger(__name__)

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

    logger.info(f"Results exported to {output_file} in {format} format")


def setup_logging(verbose: bool = False) -> None:
    """Configure logging settings.

    Args:
        verbose: Whether to enable debug logging
    """
    console = Console()

    class RichHandler(logging.Handler):
        def emit(self, record):
            try:
                msg = self.format(record)
                style = "bold red" if record.levelno >= logging.ERROR else "blue"
                console.print(f"[{style}]{msg}[/{style}]")
            except Exception:
                self.handleError(record)

    log_level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Remove existing handlers and add rich handler
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    logger.addHandler(RichHandler())


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
    """
    logger = logging.getLogger(__name__)

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

    logger.info(f"Results exported to {output_file} in {format} format")


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

    # Set up rich console
    console = Console()

    if not os.path.isdir(args.directory):
        console.print(
            f"[bold red]Error:[/bold red] '{args.directory}' is not a valid directory"
        )
        sys.exit(1)

    # Convert human-readable size to bytes
    try:
        min_size = SizeParser.parse_size(args.min_size)
    except ValueError as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        console.print("[yellow]Please use formats like: 10KB, 5MB, 1GB[/yellow]")
        sys.exit(1)

    # Create a panel with scan information
    scan_info = Table.grid(padding=1)
    scan_info.add_row("Directory:", args.directory)
    if args.exclude_dir:
        scan_info.add_row("Excluding directories:", ", ".join(args.exclude_dir))
    if args.exclude_ext:
        scan_info.add_row("Excluding extensions:", ", ".join(args.exclude_ext))
    if min_size > 0:
        scan_info.add_row("Minimum file size:", args.min_size)

    console.print(Panel(scan_info, title="Scan Configuration", border_style="blue"))

    if args.dry_run:
        console.print(
            "\n[yellow]DRY RUN[/yellow] - showing what would be scanned without processing files"
        )
        return

    console.print("\n[blue]Starting scan...[/blue]")

    start_time = time.time()
    file_processor = FileProcessor()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Scanning for duplicates...", total=None)
        duplicates, total_size, duplicate_size, files_processed = (
            file_processor.find_duplicates(
                args.directory,
                exclude_dirs=args.exclude_dir,
                exclude_extensions=args.exclude_ext,
                min_size=min_size,
                verbose=args.verbose,
                ignore_dot_dirs=not args.include_dot_dirs,
            )
        )
        progress.update(task, completed=100)

    elapsed_time = time.time() - start_time

    if not duplicates:
        console.print("\n[green]No duplicate files found.[/green]")
        return

    # Create results table
    results_table = Table(title="Scan Results", border_style="blue")
    results_table.add_column("Metric", style="cyan")
    results_table.add_column("Value", style="green")

    duplicate_count = sum(len(files) for files in duplicates.values()) - len(duplicates)
    results_table.add_row("Scan Duration", f"{elapsed_time:.2f} seconds")
    results_table.add_row("Files Processed", str(files_processed))
    results_table.add_row("Duplicate Sets", str(len(duplicates)))
    results_table.add_row("Total Duplicates", str(duplicate_count))
    results_table.add_row("Total Space Used", humanize.naturalsize(total_size))
    results_table.add_row(
        "Space Used by Duplicates", humanize.naturalsize(duplicate_size)
    )
    results_table.add_row(
        "Potential Space Savings", humanize.naturalsize(duplicate_size)
    )

    console.print("\n", results_table)

    # Display duplicate sets
    console.print("\n[bold blue]Duplicate Files:[/bold blue]")
    for hash_value, file_list in duplicates.items():
        size = humanize.naturalsize(os.path.getsize(file_list[0]))
        console.print(f"\n[yellow]Duplicate set[/yellow] (size: {size})")
        for filepath in file_list:
            console.print(f"  [green]•[/green] {filepath}")

    # Export results if requested
    if args.output:
        export_to_file(duplicates, args.output, args.format)
        console.print(
            f"\n[blue]Results exported to[/blue] [green]{args.output}[/green] [blue]in {args.format} format[/blue]"
        )


if __name__ == "__main__":
    main()
