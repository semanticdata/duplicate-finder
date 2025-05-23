# Duplicate Finder

This script is used to find duplicate files in a directory. It uses the MD5 hash of the file to determine if it is a duplicate, then prints the duplicate files and the total size of the duplicate files, along with potential space savings if the duplicate files are removed.

## Features

- Fast duplicate file detection using MD5 hashing
- Ability to exclude specific directories from the scan
- Ability to exclude specific file extensions
- Minimum file size filtering to ignore small files
- Verbose output option for detailed progress information
- Export results in multiple formats (TXT, JSON, CSV)
- Human-readable file size reporting
- Automatic exclusion of dot directories (like .git) by default
- Dry run mode to preview scan configuration
- Flexible output formatting options

## Usage

```bash
python main.py [directory] [options]
```

### Arguments

- directory : Directory to scan for duplicates

### Options

- `-e, --exclude-dir` : Directories to exclude (can be used multiple times)
- `-x, --exclude-ext` : File extensions to exclude (can be used multiple times)
- `-m, --min-size` : Minimum file size to consider (e.g. 10KB, 5MB, 1GB)
- `-o, --output` : Export results to a file (defaults to 'duplicates.txt' if no filename provided)
- `--format` : Output format (txt, json, or csv)
- `--dry-run` : Show what would be scanned without processing files
- `-v, --verbose` : Show verbose output during scanning
- `--include-dot-dirs` : Include directories that start with a dot (like .git)

## Examples

Basic usage:

```bash
python main.py C:\path\to\directory
 ```

Exclude specific directories:

```bash
python main.py C:\path\to\directory --exclude-dir C:\path\to\directory\node_modules --exclude-dir C:\path\to\directory\.git
 ```

Exclude specific file types:

```bash
python main.py C:\path\to\directory --exclude-ext .tmp --exclude-ext .log
 ```

Only find duplicates larger than 1MB:

```bash
python main.py C:\path\to\directory --min-size 1MB
 ```

Export results to a file:

```bash
python main.py C:\path\to\directory --output duplicates.txt
 ```

Export results to the default file (duplicates.txt):

```bash
python main.py C:\path\to\directory --output
 ```

Include dot directories in the scan:

```bash
python main.py C:\path\to\directory --include-dot-dirs
```

Verbose output:

```bash
python main.py C:\path\to\directory --verbose
 ```

## Output

The script provides the following information:

- Total number of duplicate files found
- Number of duplicate file sets
- Total space used by all scanned files
- Space taken by duplicates
- Potential space savings
- Detailed list of duplicate files grouped by content

### Output Formats

#### Text Format (Default)

```
Duplicate set (size: 1.5MB)
  /path/to/file1.txt
  /path/to/file2.txt
```

#### JSON Format

```json
{
  "duplicates": [
    {
      "size": 1500000,
      "files": [
        "/path/to/file1.txt",
        "/path/to/file2.txt"
      ]
    }
  ]
}
```

#### CSV Format

```csv
Size,FilePath
1500000,/path/to/file1.txt
1500000,/path/to/file2.txt
```

## Testing

To run the tests, use the following command:

```bash
uv run pytest tests/
```
