# Database Documentation Generator

A tool to generate Markdown documentation for PostgreSQL database schemas.

## Features

- Extracts table structure (columns, types, constraints, defaults)
- Extracts column comments from PostgreSQL `pg_description` system catalog
- Extracts index and constraint information
- Extracts table comments
- Creates a template for documentation on the first run
- Preserves manual documentation (everything after `## Notes`) when regenerating

## Installation

### Pre-built binaries

Download the archive for your platform from the [GitHub Releases](https://github.com/birkirb/dbdocs/releases) page, or use the stable latest URLs below.

| Platform | Archive | Latest download |
| --- | --- | --- |
| Linux (amd64) | `dbdocs_linux_amd64.tar.gz` | [download](https://github.com/birkirb/dbdocs/releases/latest/download/dbdocs_linux_amd64.tar.gz) |
| Linux (arm64) | `dbdocs_linux_arm64.tar.gz` | [download](https://github.com/birkirb/dbdocs/releases/latest/download/dbdocs_linux_arm64.tar.gz) |
| macOS (amd64) | `dbdocs_darwin_amd64.tar.gz` | [download](https://github.com/birkirb/dbdocs/releases/latest/download/dbdocs_darwin_amd64.tar.gz) |
| Windows (amd64) | `dbdocs_windows_amd64.zip` | [download](https://github.com/birkirb/dbdocs/releases/latest/download/dbdocs_windows_amd64.zip) |

Example (Linux amd64):

```bash
curl -fsSL -o dbdocs.tar.gz \
  https://github.com/birkirb/dbdocs/releases/latest/download/dbdocs_linux_amd64.tar.gz
tar -xzf dbdocs.tar.gz dbdocs
sudo install dbdocs /usr/local/bin/dbdocs
```

Verify downloads with [checksums.txt](https://github.com/birkirb/dbdocs/releases/latest/download/checksums.txt) from the same release.

### Via go install

To install the latest tagged version:

```bash
go install github.com/birkirb/dbdocs@latest
```

To install a specific version:

```bash
go install github.com/birkirb/dbdocs@v1.0.0
```

**Note:** When installing via `go install`, the version will be automatically detected from git tags. For proper version information, install from a tagged release rather than `@main`.

### From Source

```bash
go mod download
go build -o dbdocs .
```

## Usage

```bash
./dbdocs -d <database_name> [options]
```

### Options

- `-d, --database`: Database name (required)
- `-h, --hostname`: Server hostname (default: localhost)
- `-P, --port`: Database port (default: 5432)
- `-u, --username`: Username to connect as (default: postgres)
- `-p, --password`: User's password (will prompt if not provided)
- `-s, --schema`: Schema name (default: public)
- `-t, --tables`: List of tables to scan (omit for all tables)
- `-o, --output`: Output folder path (default: ./)
- `--version`: Print version and exit

### Example

```bash
./dbdocs -d mydb -u postgres -h localhost -s public -o ./docs/
```

## Output Format

The tool generates a Markdown file for each table with the following structure:

1. **Table Name** (H1 header)
2. **Table Comment** (if present)
3. **Table Schema** (Field, Type, Null, Key, Default, Extra columns)
4. **Column Comments** (if any columns have comments)
5. **Indices** (Primary keys, unique constraints, indexes, foreign keys)
6. **Index Comments** (if any indexes have comments)
7. **Notes** (preserved from previous runs)

## Documentation Preservation

All content after the `## Notes` section is considered manual documentation and will be preserved when the tool is run again. This includes:

- The `## Notes` section itself
- Any `## Columns` section
- Any other custom sections you add

## Releasing

Releases are built and published with [GoReleaser](https://goreleaser.com/) when a version tag is pushed to GitHub.

To cut a new release:

1. Update `VERSION` in the `Makefile` (used for local builds).
2. Commit and push to `main`.
3. Create and push a tag: `make tag` or `git tag v1.0.3 && git push origin v1.0.3`.

The release workflow uploads archives for Linux, macOS, and Windows, plus a `checksums.txt` file.

To test a release build locally without publishing:

```bash
make release-snapshot
```

Built artifacts are written to `dist/`.
