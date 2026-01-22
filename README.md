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

### From Source

```bash
go mod download
go build -o dbdocs .
```

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
