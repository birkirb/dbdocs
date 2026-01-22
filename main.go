package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/olekukonko/tablewriter"
	flag "github.com/spf13/pflag"
	"golang.org/x/term"
	"gopkg.in/cheggaaa/pb.v1"
)

var (
	// VERSION is set by the makefile or from build info
	VERSION = "v0.0.0"
	// BUILDDATE is set by the makefile or from build info
	BUILDDATE = ""
)

func init() {
	// Try to get version from Go build info (works with go install)
	if info, ok := debug.ReadBuildInfo(); ok {
		// Use module version if available and not "(devel)"
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			VERSION = info.Main.Version
		}
		// Try to get build time from build settings
		for _, setting := range info.Settings {
			if setting.Key == "vcs.time" {
				if t, err := time.Parse(time.RFC3339, setting.Value); err == nil {
					BUILDDATE = t.Format(time.RFC3339)
				}
			}
		}
		// If version is still default or devel, try to use vcs.revision as fallback
		if (VERSION == "v0.0.0" || VERSION == "(devel)") && BUILDDATE == "" {
			for _, setting := range info.Settings {
				if setting.Key == "vcs.revision" && len(setting.Value) >= 7 {
					VERSION = "dev-" + setting.Value[:7]
					// Use current time if no build date available
					BUILDDATE = time.Now().UTC().Format(time.RFC3339)
					break
				}
			}
		}
		// If still no build date, use current time
		if BUILDDATE == "" && VERSION != "v0.0.0" {
			BUILDDATE = time.Now().UTC().Format(time.RFC3339)
		}
	}
}

type columnComment struct {
	column  string
	comment string
}

type comments struct {
	// Map structure: database -> table -> column -> comment (for O(1) lookup)
	lookup map[string]map[string]map[string]string
	// Slice structure: database -> table -> []columnComment (to maintain order for getAll)
	ordered map[string]map[string][]columnComment
}

func (c *comments) set(database, table, column, comment string) {
	// Initialize maps if needed
	if _, ok := c.lookup[database]; !ok {
		c.lookup[database] = map[string]map[string]string{}
		c.ordered[database] = map[string][]columnComment{}
	}
	if _, ok := c.lookup[database][table]; !ok {
		c.lookup[database][table] = map[string]string{}
		c.ordered[database][table] = []columnComment{}
	}
	// Store in both structures
	c.lookup[database][table][column] = comment
	c.ordered[database][table] = append(c.ordered[database][table], columnComment{column, comment})
}

func (c *comments) get(database, table, column string) string {
	if dbMap, ok := c.lookup[database]; ok {
		if tableMap, ok := dbMap[table]; ok {
			return tableMap[column]
		}
	}
	return ""
}

func (c *comments) has(database, table string) bool {
	if dbMap, ok := c.lookup[database]; ok {
		_, ok := dbMap[table]
		return ok
	}
	return false
}

func (c *comments) getAll(database, table string) []columnComment {
	if dbMap, ok := c.ordered[database]; ok {
		if tableComments, ok := dbMap[table]; ok {
			return tableComments
		}
	}
	return []columnComment{}
}

func newComments() *comments {
	return &comments{
		lookup:  map[string]map[string]map[string]string{},
		ordered: map[string]map[string][]columnComment{},
	}
}

type databaseDocumentation struct {
	buffer     *bytes.Buffer
	db         *sql.DB
	database   string
	schema     string
	dbComments *comments
	enumTypes  map[string]bool // Cache of enum type names
}

func new(username, password, hostname, port, database, schema string) (*databaseDocumentation, error) {
	if schema == "" {
		schema = "public"
	}
	if port == "" {
		port = "5432"
	}
	dbd := databaseDocumentation{
		database:   database,
		schema:     schema,
		dbComments: newComments(),
		enumTypes:  make(map[string]bool),
	}
	dbd.resetBuffer()
	var err error
	// PostgreSQL connection string format (password excluded from error messages for security)
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		hostname, port, username, password, database)
	dbd.db, err = sql.Open("postgres", connStr)
	if err != nil {
		// Sanitize connection string in error message (don't expose password)
		safeConnStr := fmt.Sprintf("host=%s port=%s user=%s password=*** dbname=%s sslmode=disable",
			hostname, port, username, database)
		return nil, fmt.Errorf("failed to open database connection to %s: %w", safeConnStr, err)
	}
	// Pre-fetch enum types for the schema (after connection is established)
	if err := dbd.loadEnumTypes(); err != nil {
		dbd.db.Close()
		return nil, fmt.Errorf("failed to load enum types: %w", err)
	}
	return &dbd, nil
}

func promptPassword() string {
	fmt.Print("Enter password: ")
	bytePassword, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Print("\n") // wipe prompt
	if err != nil {
		log.Fatal(err)
	}
	return strings.TrimSpace(string(bytePassword))
}

func (dbd *databaseDocumentation) newLine() {
	dbd.Write("\n")
}

func (dbd *databaseDocumentation) Write(s string) {
	dbd.buffer.WriteString(s)
}

func (dbd *databaseDocumentation) handleText(text string, listLines bool) {
	inTable := false
	inHeader := false
	out := tablewriter.NewWriter(dbd.buffer)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !inTable && strings.HasPrefix(line, "+") {
			// entering a table
			inTable = true
			inHeader = true
			out = tablewriter.NewWriter(dbd.buffer)
			out.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
			out.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
			out.SetAutoFormatHeaders(false)
			out.SetCenterSeparator("|")
			out.SetAutoWrapText(false)
			dbd.Write("\n")
			continue
		}
		if !inTable {
			switch listLines {
			case true:
				dbd.Write("* " + strings.TrimSpace(line) + "\n")
			default:
				dbd.Write(strings.TrimSpace(line) + "\n\n")
			}
		} else {
			if inHeader && strings.HasPrefix(line, "+") {
				inHeader = false
				continue
			}
			if inHeader && strings.HasPrefix(line, "|") {
				// process header
				header := strings.Split(line, "|")
				out.SetHeader(header[1 : len(header)-1])
				continue
			}
			if !inHeader && strings.HasPrefix(line, "+") {
				// finish table
				inTable = false
				out.Render()
				dbd.newLine()
				continue
			}
			// process row
			row := strings.Split(line, "|")
			out.Append(row[1 : len(row)-1])
		}
	}
}

type parser struct {
	inNotes      bool
	notesContent string
}

func (dbd *databaseDocumentation) handleExisting(existingDoc string) error {
	data, err := os.ReadFile(existingDoc)
	if err != nil {
		return fmt.Errorf("failed to read existing Markdown file %q: %w", existingDoc, err)
	}
	p := parser{}
	lines := strings.Split(string(data), "\n")
	inNotesSection := false
	
	for _, line := range lines {
		if strings.HasPrefix(line, "## Notes") {
			inNotesSection = true
			p.inNotes = true
			continue
		}
		if inNotesSection {
			// Collect ALL content after ## Notes, including other sections like ## Columns
			p.notesContent += line + "\n"
		}
	}

	// Always preserve existing content if Notes section exists
	if p.inNotes {
		// Trim trailing newlines to avoid accumulating them on each run
		content := strings.TrimRight(p.notesContent, "\n")
		dbd.Write("\n" + content)
	}
	
	return nil
}

func (dbd *databaseDocumentation) getColumnComments() error {
	query := `
	SELECT 
		c.relname as table_name,
		a.attname as column_name,
		pg_catalog.col_description(a.attrelid, a.attnum) as column_comment
	FROM pg_catalog.pg_attribute a
	JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = $1
		AND c.relkind = 'r'
		AND a.attnum > 0
		AND NOT a.attisdropped
		AND pg_catalog.col_description(a.attrelid, a.attnum) IS NOT NULL
		AND pg_catalog.col_description(a.attrelid, a.attnum) != ''
	ORDER BY c.relname, a.attnum
	`
	rows, err := dbd.db.Query(query, dbd.schema)
	if err != nil {
		return fmt.Errorf("failed to fetch column comments from schema %q: %w", dbd.schema, err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, columnName, columnComment sql.NullString
		if err := rows.Scan(&tableName, &columnName, &columnComment); err != nil {
			return fmt.Errorf("failed to scan column comment row (table: %q, column: %q): %w", tableName.String, columnName.String, err)
		}
		if columnComment.Valid && columnComment.String != "" {
			dbd.dbComments.set(dbd.database, tableName.String, columnName.String, columnComment.String)
		}
	}
	return nil
}

func (dbd *databaseDocumentation) renderTable(header []string, data [][]string) {
	table := tablewriter.NewWriter(dbd.buffer)
	table.SetHeader(header)
	table.SetAutoFormatHeaders(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")
	table.SetAutoWrapText(false)
	table.AppendBulk(data)
	table.Render()
}

func (dbd *databaseDocumentation) fetchIndexComments(table string) (map[string]string, error) {
	comments := map[string]string{}

	query := `
	SELECT
		i.relname as index_name,
		COALESCE(obj_description(i.oid, 'pg_class'), '') as index_comment
	FROM pg_class t
	JOIN pg_index ix ON t.oid = ix.indrelid
	JOIN pg_class i ON i.oid = ix.indexrelid
	JOIN pg_namespace n ON n.oid = t.relnamespace
	WHERE t.relkind = 'r'
		AND n.nspname = $1
		AND t.relname = $2
		AND obj_description(i.oid, 'pg_class') IS NOT NULL
		AND obj_description(i.oid, 'pg_class') != ''
	`
	rows, err := dbd.db.Query(query, dbd.schema, table)
	if err != nil {
		return comments, fmt.Errorf("failed to query index comments from table %q in schema %q: %w", table, dbd.schema, err)
	}
	defer rows.Close()

	for rows.Next() {
		var keyName, indexComment sql.NullString
		if err := rows.Scan(&keyName, &indexComment); err != nil {
			return comments, fmt.Errorf("failed to scan index comment row for table %q (index: %q): %w", table, keyName.String, err)
		}
		if indexComment.Valid && indexComment.String != "" {
			comments[keyName.String] = indexComment.String
		}
	}

	return comments, nil
}

func removeDuplicates(s *[]string) {
	found := make(map[string]bool)
	j := 0
	for i, x := range *s {
		if !found[x] {
			found[x] = true
			(*s)[j] = (*s)[i]
			j++
		}
	}
	*s = (*s)[:j]
}

type foreignKey struct {
	name   string
	column string
	ref    string
}

type indexes struct {
	// slices to keep order
	keys        []keyDoc
	foreignKeys []foreignKey
}

type keyDoc struct {
	indexName      string
	constraintType string
	constraintName string
	rowNames       []string
}

func (i *keyDoc) toStringSlice() []string {
	removeDuplicates(&i.rowNames)
	return []string{i.constraintType, i.indexName, strings.Join(i.rowNames, ", "), ""}
}

type schemaRow struct {
	seqNum                     int
	indexName, columnName, ref string
	constraintName, constraintType, referencedTable,
	referencedColumn sql.NullString
	isPrimary, isUnique bool
}

func (dbd *databaseDocumentation) documentIndexes(table string) error {
	// First get all indexes (excluding foreign keys)
	indexQuery := `
	SELECT
		i.relname as index_name,
		a.attname as column_name,
		row_number() OVER (PARTITION BY i.relname ORDER BY array_position(ix.indkey, a.attnum)) as seq_in_index,
		con.conname as constraint_name,
		con.contype as constraint_type,
		ix.indisprimary as is_primary,
		ix.indisunique as is_unique
	FROM pg_index ix
	JOIN pg_class t ON t.oid = ix.indrelid
	JOIN pg_class i ON i.oid = ix.indexrelid
	JOIN pg_namespace n ON n.oid = t.relnamespace
	JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) AND a.attnum > 0 AND NOT a.attisdropped
	LEFT JOIN pg_constraint con ON con.conindid = i.oid AND con.contype != 'f'
	WHERE t.relkind = 'r'
		AND n.nspname = $1
		AND t.relname = $2
	ORDER BY i.relname, seq_in_index
	`

	indexRows, err := dbd.db.Query(indexQuery, dbd.schema, table)
	if err != nil {
		return fmt.Errorf("failed to query indexes from table %q in schema %q: %w", table, dbd.schema, err)
	}
	defer indexRows.Close()

	dbd.Write("## Indices\n\n")

	// Process indexes
	indexRowsList := [][]schemaRow{}
	currentIndex := ""
	currentRows := []schemaRow{}
	for indexRows.Next() {
		sr := schemaRow{}
		var constraintType sql.NullString
		var seqInIndex int
		if err := indexRows.Scan(
			&sr.indexName, &sr.columnName, &seqInIndex, &sr.constraintName,
			&constraintType, &sr.isPrimary, &sr.isUnique,
		); err != nil {
			return fmt.Errorf("failed to scan index row for table %q (index: %q): %w", table, sr.indexName, err)
		}
		sr.seqNum = seqInIndex
		if constraintType.Valid {
			// Map PostgreSQL constraint types to readable names
			switch constraintType.String {
			case "p":
				sr.constraintType = sql.NullString{String: "PRIMARY KEY", Valid: true}
			case "u":
				sr.constraintType = sql.NullString{String: "UNIQUE", Valid: true}
			default:
				sr.constraintType = constraintType
			}
		} else if sr.isPrimary {
			sr.constraintType = sql.NullString{String: "PRIMARY KEY", Valid: true}
		} else if sr.isUnique {
			sr.constraintType = sql.NullString{String: "UNIQUE", Valid: true}
		}

		// if we find a new index: append and clear
		if sr.indexName != currentIndex {
			if len(currentRows) > 0 {
				indexRowsList = append(indexRowsList, currentRows)
			}
			currentIndex = sr.indexName
			currentRows = []schemaRow{}
		}
		currentRows = append(currentRows, sr)
	}
	if len(currentRows) > 0 {
		indexRowsList = append(indexRowsList, currentRows)
	}

	indexesData := indexes{
		keys:        []keyDoc{},
		foreignKeys: []foreignKey{},
	}
	for _, schemaRows := range indexRowsList {
		var doc keyDoc
		var first = true
		for _, sr := range schemaRows {
			// first column in this index
			if sr.seqNum == 1 {
				// append the previous if not the first
				if !first {
					if doc.constraintType != "" || len(doc.rowNames) > 0 {
						indexesData.keys = append(indexesData.keys, doc)
					}
				}
				doc = keyDoc{indexName: sr.indexName}
				first = false
			}
			if sr.constraintName.Valid {
				doc.constraintName = sr.constraintName.String
			}
			doc.rowNames = append(doc.rowNames, sr.columnName)
			// only set the constraint type if the constraint name matches the index name
			if sr.constraintName.Valid && sr.constraintName.String == sr.indexName {
				if sr.constraintType.Valid {
					doc.constraintType = sr.constraintType.String
				}
			} else {
				// default to KEY constraint type
				if sr.constraintType.Valid {
					doc.constraintType = sr.constraintType.String
				} else {
					doc.constraintType = "KEY"
				}
			}
		}
		if doc.constraintType != "" || len(doc.rowNames) > 0 {
			indexesData.keys = append(indexesData.keys, doc)
		}
	}

	// Now get foreign key constraints separately
	fkQuery := `
	SELECT
		con.conname as constraint_name,
		a.attname as column_name,
		ref_t.relname as referenced_table,
		ref_a.attname as referenced_column
	FROM pg_constraint con
	JOIN pg_class t ON t.oid = con.conrelid
	JOIN pg_namespace n ON n.oid = t.relnamespace
	JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(con.conkey) AND a.attnum > 0 AND NOT a.attisdropped
	JOIN pg_class ref_t ON ref_t.oid = con.confrelid
	JOIN pg_attribute ref_a ON ref_a.attrelid = ref_t.oid AND ref_a.attnum = ANY(con.confkey) AND array_position(con.conkey, a.attnum) = array_position(con.confkey, ref_a.attnum)
	WHERE con.contype = 'f'
		AND t.relkind = 'r'
		AND n.nspname = $1
		AND t.relname = $2
	ORDER BY con.conname, array_position(con.conkey, a.attnum)
	`

	fkRows, err := dbd.db.Query(fkQuery, dbd.schema, table)
	if err != nil {
		return fmt.Errorf("failed to query foreign keys from table %q in schema %q: %w", table, dbd.schema, err)
	}
	defer fkRows.Close()

	// Group foreign keys by constraint name, preserving order
	fkMap := make(map[string]*foreignKey)
	fkOrder := []string{} // Track order of constraint names
	for fkRows.Next() {
		var constraintName, columnName string
		var referencedTable, referencedColumn sql.NullString
		if err := fkRows.Scan(&constraintName, &columnName, &referencedTable, &referencedColumn); err != nil {
			return fmt.Errorf("failed to scan foreign key row for table %q (constraint: %q): %w", table, constraintName, err)
		}
		if referencedTable.Valid && referencedColumn.Valid {
			if fk, exists := fkMap[constraintName]; exists {
				// Multi-column foreign key - append column and reference
				fk.column += ", " + columnName
				fk.ref += ", " + referencedTable.String + "." + referencedColumn.String
			} else {
				ref := referencedTable.String + "." + referencedColumn.String
				fkMap[constraintName] = &foreignKey{
					name:   constraintName,
					ref:    ref,
					column: columnName,
				}
				// Track order of first occurrence
				fkOrder = append(fkOrder, constraintName)
			}
		}
	}

	// Convert map to slice in the order they were encountered
	for _, constraintName := range fkOrder {
		if fk, exists := fkMap[constraintName]; exists {
			indexesData.foreignKeys = append(indexesData.foreignKeys, *fk)
		}
	}

	// slightly awkward double loop to have primary keys first in SQL query return order
	data := [][]string{}
	// primary keys first
	for _, doc := range indexesData.keys {
		if doc.indexName == table+"_pkey" || strings.Contains(doc.constraintType, "PRIMARY") {
			data = append(data, doc.toStringSlice())
		}
	}

	// only non-primaries
	for _, doc := range indexesData.keys {
		if doc.indexName != table+"_pkey" && !strings.Contains(doc.constraintType, "PRIMARY") {
			data = append(data, doc.toStringSlice())
		}
	}

	// foreign keys come last
	for _, foreignKeyData := range indexesData.foreignKeys {
		data = append(data, []string{"CONSTRAINT", foreignKeyData.name, "FOREIGN KEY (" + foreignKeyData.column + ")", foreignKeyData.ref})
	}

	dbd.renderTable([]string{"Type", "Name", "Columns", "Ref"}, data)
	dbd.Write("\n\n")
	indexComments, err := dbd.fetchIndexComments(table)
	if err != nil {
		return err
	}
	if len(indexComments) > 0 {
		dbd.Write("## Index Comments\n\n")
		for index, comment := range indexComments {
			dbd.Write(fmt.Sprintf("* `%s`: %s\n", index, comment))
		}
		dbd.Write("\n\n")
	}
	return nil
}

func (dbd *databaseDocumentation) documentTable(table string, tableData *[][]string) error {
	query := `
	SELECT
		a.attname as field,
		pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
		CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END as null,
		CASE 
			WHEN pk.attname IS NOT NULL THEN 'PRI'
			WHEN uq.attname IS NOT NULL THEN 'UNI'
			ELSE ''
		END as key,
		COALESCE(pg_catalog.pg_get_expr(adbin, adrelid), '') as default,
		CASE 
			WHEN a.attidentity = 'a' THEN 'GENERATED ALWAYS AS IDENTITY'
			WHEN a.attidentity = 'd' THEN 'GENERATED BY DEFAULT AS IDENTITY'
			WHEN a.attgenerated = 's' THEN 'GENERATED ALWAYS AS ... STORED'
			WHEN a.attgenerated = 'v' THEN 'GENERATED ALWAYS AS ... VIRTUAL'
			ELSE ''
		END as extra
	FROM pg_catalog.pg_attribute a
	JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
	LEFT JOIN (
		SELECT kcu.column_name as attname
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu 
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema = $1
			AND tc.table_name = $2
	) pk ON pk.attname = a.attname
	LEFT JOIN (
		SELECT kcu.column_name as attname
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu 
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'UNIQUE'
			AND tc.table_schema = $1
			AND tc.table_name = $2
	) uq ON uq.attname = a.attname
	WHERE c.relkind = 'r'
		AND n.nspname = $1
		AND c.relname = $2
		AND a.attnum > 0
		AND NOT a.attisdropped
	ORDER BY a.attnum
	`
	rows, err := dbd.db.Query(query, dbd.schema, table)
	if err != nil {
		return fmt.Errorf("failed to query table structure for %q in schema %q: %w", table, dbd.schema, err)
	}
	defer rows.Close()

	data := [][]string{}
	for rows.Next() {
		var dbField, dbType, dbNull, dbKey, dbExtra string
		var dbDefault sql.NullString
		if err := rows.Scan(&dbField, &dbType, &dbNull, &dbKey, &dbDefault, &dbExtra); err != nil {
			return fmt.Errorf("failed to scan column row for table %q (column: %q): %w", table, dbField, err)
		}

		// Simplify type names for readability
		dbType = dbd.simplifyTypeName(dbType)

		// Clean up default values
		defaultVal := dbd.cleanDefaultValue(dbDefault)

		data = append(data, []string{dbField, dbType, dbNull, dbKey, defaultVal, dbExtra})
	}

	// Store table data for template generation
	*tableData = data

	header := []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	dbd.renderTable(header, data)
	dbd.Write("\n\n")
	if dbd.dbComments.has(dbd.database, table) {
		dbd.Write("## Column Comments\n\n")
		// Display comments in the same order as columns appear in the table
		for _, row := range data {
			columnName := row[0] // Field is the first column
			if comment := dbd.dbComments.get(dbd.database, table, columnName); comment != "" {
				dbd.Write(fmt.Sprintf("* `%s`: %s\n", columnName, comment))
			}
		}
		dbd.Write("\n\n")
	}
	return nil
}

// simplifyTypeName converts PostgreSQL type names to more readable forms
func (dbd *databaseDocumentation) simplifyTypeName(typ string) string {
	// Replace character varying with varchar
	if strings.HasPrefix(typ, "character varying") {
		typ = strings.Replace(typ, "character varying", "varchar", 1)
	}
	// Replace character with char
	if strings.HasPrefix(typ, "character(") {
		typ = strings.Replace(typ, "character(", "char(", 1)
	}
	return typ
}

// cleanDefaultValue cleans up PostgreSQL default value expressions
func (dbd *databaseDocumentation) cleanDefaultValue(dbDefault sql.NullString) string {
	if !dbDefault.Valid || dbDefault.String == "" {
		return ""
	}

	defaultVal := dbDefault.String

	// Remove sequence calls (auto-increment)
	if strings.HasPrefix(defaultVal, "nextval(") {
		return ""
	}

	// Handle PostgreSQL special timestamp values
	if strings.Contains(defaultVal, "-infinity") {
		return "-infinity"
	}
	if strings.Contains(defaultVal, "infinity") && !strings.Contains(defaultVal, "-infinity") {
		return "infinity"
	}

	// Simplify timestamp functions
	if strings.Contains(defaultVal, "now()") {
		if strings.Contains(defaultVal, "AT TIME ZONE") {
			return "CURRENT_TIMESTAMP"
		}
		return "CURRENT_TIMESTAMP"
	}
	if strings.Contains(defaultVal, "CURRENT_TIMESTAMP") {
		return "CURRENT_TIMESTAMP"
	}
	if strings.Contains(defaultVal, "CURRENT_DATE") {
		return "CURRENT_DATE"
	}
	if strings.Contains(defaultVal, "CURRENT_TIME") {
		return "CURRENT_TIME"
	}

	// Remove type casting from string literals (e.g., 'value'::text, 'uuid'::uuid, ''::character varying)
	// Match patterns like '...'::type or '...'::schema.type (including types with spaces like "character varying")
	re := regexp.MustCompile(`^'([^']*)'::.+$`)
	if matches := re.FindStringSubmatch(defaultVal); len(matches) > 1 {
		// If it's an empty string, show it explicitly
		if matches[1] == "" {
			return "''"
		}
		return matches[1]
	}

	// Remove type casting from other expressions (e.g., (expr)::type)
	re2 := regexp.MustCompile(`^\((.+)\)::.+$`)
	if matches := re2.FindStringSubmatch(defaultVal); len(matches) > 1 {
		return matches[1]
	}

	return defaultVal
}

// fetchEnumValues retrieves enum values for a given enum type
func (dbd *databaseDocumentation) fetchEnumValues(enumType string) ([]string, error) {
	// Extract enum type name (handle schema.type format)
	typeParts := strings.Split(enumType, ".")
	var typeName, typeSchema string
	if len(typeParts) == 2 {
		typeSchema = typeParts[0]
		typeName = typeParts[1]
	} else {
		typeSchema = dbd.schema
		typeName = enumType
	}

	query := `
	SELECT e.enumlabel
	FROM pg_enum e
	JOIN pg_type t ON e.enumtypid = t.oid
	JOIN pg_namespace n ON t.typnamespace = n.oid
	WHERE n.nspname = $1 AND t.typname = $2
	ORDER BY e.enumsortorder
	`
	rows, err := dbd.db.Query(query, typeSchema, typeName)
	if err != nil {
		return nil, fmt.Errorf("failed to query enum values for %q: %w", enumType, err)
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			return nil, fmt.Errorf("failed to scan enum value: %w", err)
		}
		values = append(values, label)
	}
	return values, nil
}

// loadEnumTypes loads all enum types from the schema into cache
func (dbd *databaseDocumentation) loadEnumTypes() error {
	query := `
	SELECT t.typname, n.nspname
	FROM pg_type t
	JOIN pg_namespace n ON t.typnamespace = n.oid
	WHERE t.typtype = 'e'
		AND (n.nspname = $1 OR n.nspname = 'pg_catalog')
	`
	rows, err := dbd.db.Query(query, dbd.schema)
	if err != nil {
		return fmt.Errorf("failed to query enum types: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var typeName, typeSchema string
		if err := rows.Scan(&typeName, &typeSchema); err != nil {
			return fmt.Errorf("failed to scan enum type: %w", err)
		}
		// Store both with and without schema prefix
		dbd.enumTypes[typeName] = true
		if typeSchema != dbd.schema {
			dbd.enumTypes[typeSchema+"."+typeName] = true
		}
	}
	return nil
}

// isEnumType checks if a PostgreSQL type is an enum
func (dbd *databaseDocumentation) isEnumType(columnType string) bool {
	// Remove any array notation
	columnType = strings.TrimSuffix(columnType, "[]")
	// Check cache first
	if dbd.enumTypes[columnType] {
		return true
	}
	// Check if it's a base type (not an enum)
	baseTypes := map[string]bool{
		"varchar": true, "char": true, "text": true,
		"int": true, "integer": true, "bigint": true, "smallint": true,
		"numeric": true, "decimal": true, "real": true, "double precision": true,
		"boolean": true, "bool": true,
		"date": true, "time": true, "timestamp": true, "timestamptz": true,
		"uuid": true, "json": true, "jsonb": true, "bytea": true,
	}
	return !baseTypes[strings.ToLower(columnType)]
}

// generateTemplate generates a template for the Notes section
func (dbd *databaseDocumentation) generateTemplate(table string, tableData [][]string) error {
	dbd.Write("\n\n<Please insert table description here>\n\n")
	dbd.Write("## Columns\n\n")

	for _, row := range tableData {
		columnName := row[0]
		columnType := row[1]

		// Skip columns that already have comments
		if comment := dbd.dbComments.get(dbd.database, table, columnName); comment != "" {
			continue
		}

		dbd.Write(fmt.Sprintf("* `%s`: <insert column documentation here>", columnName))

		// Check if this column is an enum type
		if dbd.isEnumType(columnType) {
			enumValues, err := dbd.fetchEnumValues(columnType)
			if err == nil && len(enumValues) > 0 {
				dbd.Write("\n\n")
				// Generate enum table
				enumData := [][]string{}
				for i, val := range enumValues {
					enumData = append(enumData, []string{fmt.Sprintf("%d", i), val, "<add description here>"})
				}
				dbd.renderTable([]string{"id", "name", "description"}, enumData)
			}
		}
		dbd.Write("\n")
	}

	return nil
}

func (dbd *databaseDocumentation) fetchTables(tables []string) (*[]string, error) {
	if len(tables) <= 0 {
		query := `
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = $1
		ORDER BY tablename
		`
		rows, err := dbd.db.Query(query, dbd.schema)
		if err != nil {
			return &tables, fmt.Errorf("failed to list tables from schema %q: %w", dbd.schema, err)
		}

		var tableName string
		for rows.Next() {
			if err := rows.Scan(&tableName); err != nil {
				return &tables, fmt.Errorf("failed to scan table name row: %w", err)
			}
			tables = append(tables, tableName)
		}
		rows.Close()
	}
	return &tables, nil
}

func (dbd *databaseDocumentation) writeFile(output, table string) error {
	// Ensure output path ends with a directory separator
	outputPath := output
	if outputPath != "" && !strings.HasSuffix(outputPath, "/") && !strings.HasSuffix(outputPath, string(os.PathSeparator)) {
		outputPath += "/"
	}
	filename := outputPath + table + ".md"
	
	// Get buffer content and ensure it ends with exactly one newline
	content := dbd.buffer.Bytes()
	contentStr := strings.TrimRight(string(content), "\n") + "\n"
	
	if err := os.WriteFile(filename, []byte(contentStr), 0644); err != nil {
		return fmt.Errorf("failed to write file %q: %w", filename, err)
	}
	return nil
}

func (dbd *databaseDocumentation) resetBuffer() {
	dbd.buffer = bytes.NewBufferString("")
}

func (dbd *databaseDocumentation) tableComment(table string) error {
	query := `
	SELECT obj_description(c.oid, 'pg_class') as table_comment
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relkind = 'r'
		AND n.nspname = $1
		AND c.relname = $2
	`
	rows, err := dbd.db.Query(query, dbd.schema, table)
	if err != nil {
		return fmt.Errorf("failed to query table comment for %q in schema %q: %w", table, dbd.schema, err)
	}
	defer rows.Close()
	var hadComment bool
	for rows.Next() {
		var comment sql.NullString
		if err := rows.Scan(&comment); err != nil {
			return fmt.Errorf("failed to scan table comment row for %q: %w", table, err)
		}
		if !comment.Valid || comment.String == "" {
			continue
		}
		dbd.Write(comment.String)
		dbd.newLine()
		hadComment = true
	}
	if hadComment {
		dbd.newLine()
	}
	return nil
}

func main() {
	var (
		hostname = flag.StringP("hostname", "h", "localhost", "Server Hostname")
		database = flag.StringP("database", "d", "", "Database to scan")
		username = flag.StringP("username", "u", "postgres", "Username to connect as")
		tables   = flag.StringSliceP("tables", "t", []string{}, "List of tables to scan. Omit for all.")
		output   = flag.StringP("output", "o", "./", "Output folder path")
		password = flag.StringP("password", "p", "", "User's password")
		port     = flag.StringP("port", "P", "5432", "Database port")
		schema   = flag.StringP("schema", "s", "public", "Schema name")
		version  = flag.BoolP("version", "v", false, "print the application version and exit")
	)
	flag.Parse()

	if *version {
		fmt.Printf("%s %s\n", VERSION, BUILDDATE)
		return
	}

	if *database == "" {
		flag.PrintDefaults()
		fmt.Println()
		log.Fatal("--database needs to be set")
	}

	if *password == "" {
		*password = promptPassword()
	}

	// Validate and create output directory
	if err := os.MkdirAll(*output, 0755); err != nil {
		log.Fatalf("failed to create output directory %q: %v", *output, err)
	}

	dbd, err := new(*username, *password, *hostname, *port, *database, *schema)
	if err != nil {
		log.Fatalf("failed to initialize database connection: %v", err)
	}
	defer dbd.db.Close()

	// fetch tables in case none provided
	if tables, err = dbd.fetchTables(*tables); err != nil {
		log.Fatalf("failed to fetch table list: %v", err)
	}

	if len(*tables) == 0 {
		log.Fatal("no tables found to document")
	}

	// fetch table comments
	if err := dbd.getColumnComments(); err != nil {
		log.Fatalf("failed to fetch column comments: %v", err)
	}

	bar := pb.StartNew(len(*tables))
	var errors []error
	successCount := 0

	for _, table := range *tables {
		dbd.resetBuffer()

		dbd.Write("# " + table + "\n\n")

		// optionally fetch the table comment
		if err := dbd.tableComment(table); err != nil {
			errors = append(errors, fmt.Errorf("table %q: failed to fetch table comment: %w", table, err))
			bar.Increment()
			continue
		}

		// create the table documentation
		var tableData [][]string
		if err := dbd.documentTable(table, &tableData); err != nil {
			errors = append(errors, fmt.Errorf("table %q: failed to document table structure: %w", table, err))
			bar.Increment()
			continue
		}

		// Fetch index information and render in documentation
		if err := dbd.documentIndexes(table); err != nil {
			errors = append(errors, fmt.Errorf("table %q: failed to document indexes: %w", table, err))
			bar.Increment()
			continue
		}

		dbd.Write("## Notes")

		// merge notes in case we have a previous file
		outputPath := *output
		if outputPath != "" && !strings.HasSuffix(outputPath, "/") && !strings.HasSuffix(outputPath, string(os.PathSeparator)) {
			outputPath += "/"
		}
		existingDoc := outputPath + table + ".md"
		if _, err := os.Stat(existingDoc); err == nil {
			// File exists - preserve existing content
			if err := dbd.handleExisting(existingDoc); err != nil {
				errors = append(errors, fmt.Errorf("table %q: failed to merge existing documentation: %w", table, err))
				bar.Increment()
				continue
			}
		} else {
			// File doesn't exist - generate template on first run
			if err := dbd.generateTemplate(table, tableData); err != nil {
				errors = append(errors, fmt.Errorf("table %q: failed to generate template: %w", table, err))
				bar.Increment()
				continue
			}
		}
		if err := dbd.writeFile(outputPath, table); err != nil {
			errors = append(errors, fmt.Errorf("table %q: failed to write output file: %w", table, err))
			bar.Increment()
			continue
		}
		successCount++
		bar.Increment()
	}

	bar.Finish()

	// Report results
	if len(errors) > 0 {
		fmt.Fprintf(os.Stderr, "\nErrors encountered:\n")
		for _, err := range errors {
			fmt.Fprintf(os.Stderr, "  - %v\n", err)
		}
		fmt.Fprintf(os.Stderr, "\nSuccessfully documented %d of %d tables.\n", successCount, len(*tables))
		if successCount == 0 {
			os.Exit(1)
		}
	} else {
		fmt.Printf("\nSuccessfully documented %d table(s).\n", successCount)
	}
}
